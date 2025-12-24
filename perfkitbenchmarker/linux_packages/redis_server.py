# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing redis installation and cleanup functions."""

import logging
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util


class RedisEvictionPolicy:
  """Enum of options for --redis_eviction_policy."""

  NOEVICTION = 'noeviction'
  ALLKEYS_LRU = 'allkeys-lru'
  VOLATILE_LRU = 'volatile-lru'
  ALLKEYS_RANDOM = 'allkeys-random'
  VOLATILE_RANDOM = 'volatile-random'
  VOLATILE_TTL = 'volatile-ttl'


_VERSION = flags.DEFINE_string(
    'redis_server_version', '6.2.1', 'Version of redis server to use.'
)
CLUSTER_MODE = flags.DEFINE_bool(
    'redis_server_cluster_mode', False, 'Whether to use cluster mode.'
)
IO_THREADS = flags.DEFINE_list(
    'redis_server_io_threads',
    None,
    'Only supported for redis version >= 6, the '
    'number of redis server IO threads to use.',
)
_IO_THREADS_DO_READS = flags.DEFINE_bool(
    'redis_server_io_threads_do_reads',
    False,
    'If true, makes both reads and writes use IO threads instead of just '
    'writes.',
)
_IO_THREAD_AFFINITY = flags.DEFINE_bool(
    'redis_server_io_threads_cpu_affinity',
    False,
    'If true, attempts to pin IO threads to CPUs.',
)
_ENABLE_SNAPSHOTS = flags.DEFINE_bool(
    'redis_server_enable_snapshots',
    False,
    'If true, uses the default redis snapshot policy.',
)
NUM_PROCESSES = flags.DEFINE_integer(
    'redis_total_num_processes',
    1,
    'Total number of redis server processes. Useful when running with a redis '
    'version lower than 6. If set to 0, uses num_cpus.',
    lower_bound=0,
)
_EVICTION_POLICY = flags.DEFINE_enum(
    'redis_eviction_policy',
    RedisEvictionPolicy.NOEVICTION,
    [
        RedisEvictionPolicy.NOEVICTION,
        RedisEvictionPolicy.ALLKEYS_LRU,
        RedisEvictionPolicy.VOLATILE_LRU,
        RedisEvictionPolicy.ALLKEYS_RANDOM,
        RedisEvictionPolicy.VOLATILE_RANDOM,
        RedisEvictionPolicy.VOLATILE_TTL,
    ],
    'Redis eviction policy when maxmemory limit is reached. This requires '
    'running clients with larger amounts of data than Redis can hold.',
)
REDIS_AOF = flags.DEFINE_bool(
    'redis_aof',
    False,
    'If true, use disks on the server for aof backups.',
)
REDIS_AOF_VERIFY = flags.DEFINE_bool(
    'redis_aof_verify',
    False,
    'If true, execute redis-check-aof to verify the AOF backups. This depends'
    ' on enabling --redis_aof. Default is set to False.',
)


# Default port for Redis
DEFAULT_PORT = 6379
REDIS_PID_FILE = 'redis.pid'
FLAGS = flags.FLAGS
REDIS_GIT = 'https://github.com/redis/redis.git'
REDIS_BACKUP = 'scratch'
CURRENT_IO_THREADS = None


def _GetRedisTarName() -> str:
  return f'redis-{_VERSION.value}.tar.gz'


def GetRedisDir() -> str:
  return f'{linux_packages.INSTALL_DIR}/redis'


def _GetNumProcesses(vm) -> int:
  num_processes = NUM_PROCESSES.value
  if num_processes == 0 and vm is not None:
    num_processes = vm.NumCpusForBenchmark()
  assert num_processes >= 0, 'num_processes must be >=0.'

  return num_processes


def CheckPrerequisites():
  """Verifies if the flags are valid."""
  values = IO_THREADS.value

  try:
    int_values = [int(v) for v in values] if values is not None else []
  except ValueError as e:
    raise errors.Setup.InvalidFlagConfigurationError(
        'redis_server_io_threads must be a list of valid integers.'
    ) from e

  if not all(1 <= v <= 128 for v in int_values):
    raise errors.Setup.InvalidFlagConfigurationError(
        'redis_server_io_threads must be a list of integers between 1 and 128.'
    )

  # TODO(user): Add support for cluster mode and session storage.
  if (CLUSTER_MODE.value or REDIS_AOF.value) and len(int_values) > 1:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Sweeping redis_server_io_threads is not supported for cluster mode or'
        ' session storage configurations.'
    )

  return True


def PrepareSystem(vm) -> None:
  """Set system-wide parameters on the VM."""
  CheckPrerequisites()

  if vm.PLATFORM == provider_info.KUBERNETES:
    logging.info('Skipping system preparation for Kubernetes VMs.')
    return

  num_processes = _GetNumProcesses(vm)
  # 10 is an arbituary multiplier that ensures this value is high enough.
  mux_sessions = 10 * num_processes
  vm.RemoteCommand(
      f'echo "\nMaxSessions {mux_sessions}" | sudo tee -a /etc/ssh/sshd_config'
  )
  # Redis tuning parameters, see
  # https://www.techandme.se/performance-tips-for-redis-cache-server/.
  # This command works on 2nd generation of VMs only.
  update_sysvtl = vm.TryRemoteCommand(
      'echo "'
      'vm.overcommit_memory = 1\n'
      'net.core.somaxconn = 65535\n'
      '" | sudo tee -a /etc/sysctl.conf'
  )
  # /usr/sbin/sysctl is not applicable on certain distros.
  commit_sysvtl = vm.TryRemoteCommand(
      'sudo /usr/sbin/sysctl -p || sudo sysctl -p'
  )
  if not (update_sysvtl and commit_sysvtl):
    logging.info('Fail to optimize overcommit_memory and socket connections.')


def _Install(vm) -> None:
  """Installs the redis package on the VM."""
  CheckPrerequisites()
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR}; git clone {REDIS_GIT}')
  vm.RemoteCommand(
      f'cd {GetRedisDir()} && git checkout {_VERSION.value} && make'
  )


def YumInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-devel')
  if REDIS_AOF_VERIFY.value:
    vm.InstallPackages('redis')
  _Install(vm)


def AptInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-dev')
  if REDIS_AOF_VERIFY.value:
    vm.InstallPackages('redis-tools')
  _Install(vm)


def _GetIOThreads(vm) -> int:
  if CURRENT_IO_THREADS is not None:
    return CURRENT_IO_THREADS
  # Redis docs suggests that i/o threads should not exceed number of cores.
  nthreads_per_core = vm.CheckLsCpu().threads_per_core
  num_cpus = vm.NumCpusForBenchmark()
  if nthreads_per_core == 1:
    return max(num_cpus - 1, 1)
  else:
    return num_cpus // 2


def _GetRedisAofFilename(port) -> str:
  return f'backup_{port}'


def _GetRedisConfigDir(vm, localhost, port) -> str:
  """Returns the directory where redis files are stored."""
  stdout, _ = vm.RemoteCommand(
      f'sudo {GetRedisDir()}/src/redis-cli -h {localhost} -p {port} CONFIG'
      ' GET dir'
  )
  return stdout.strip().split('\n')[-1]


def _BuildStartCommand(vm, port: int) -> str:
  """Returns the run command used to start the redis server.

  See https://raw.githubusercontent.com/redis/redis/6.0/redis.conf
  for the default redis configuration.

  Args:
    vm: The redis server VM.
    port: The port to start redis on.

  Returns:
    A command that can be used to start redis in the background.
  """
  redis_dir = GetRedisDir()
  cmd = 'nohup sudo {redis_dir}/src/redis-server {args} &> /dev/null &'
  cmd_args = [
      f'--port {port}',
      '--protected-mode no',
  ]
  if CLUSTER_MODE.value:
    cmd_args += [
        '--cluster-enabled yes',
    ]
  if REDIS_AOF.value:
    cmd_args += [
        '--appendonly yes',
        f'--appendfilename {_GetRedisAofFilename(port)}',
        f'--dir /{REDIS_BACKUP}',
    ]
  # Add check for the MADV_FREE/fork arm64 Linux kernel bug
  if _VERSION.value >= '6.2.1':
    cmd_args.append('--ignore-warnings ARM64-COW-BUG')
    io_threads = _GetIOThreads(vm)
    cmd_args.append(f'--io-threads {io_threads}')
  # Snapshotting
  if not _ENABLE_SNAPSHOTS.value:
    cmd_args.append('--save ""')
  # IO thread reads
  if _IO_THREADS_DO_READS.value:
    do_reads = 'yes' if _IO_THREADS_DO_READS.value else 'no'
    cmd_args.append(f'--io-threads-do-reads {do_reads}')
  # IO thread affinity
  if _IO_THREAD_AFFINITY.value:
    cpu_affinity = f'0-{vm.num_cpus-1}'
    cmd_args.append(f'--server_cpulist {cpu_affinity}')
  if _EVICTION_POLICY.value:
    cmd_args.append(f'--maxmemory-policy {_EVICTION_POLICY.value}')

  # If aof is not enabled, set maxmemory to 70% of server VM's total memory
  # divided by the number of processes. This is to ensure that the redis
  # instances don't consume too much memory and cause the server VM to become
  # unresponsive.
  # If aof is enabled, deduct 10.5GB from the maxmemory to account
  # for the AOF and OS overheads.
  num_processes = _GetNumProcesses(vm)
  if REDIS_AOF.value:
    max_memory_per_instance = int(vm.total_memory_kb - 11024384)
  else:
    max_memory_per_instance = int(vm.total_memory_kb * 0.7 / num_processes)
  cmd_args.append(f'--maxmemory {max_memory_per_instance}kb')
  return cmd.format(redis_dir=redis_dir, args=' '.join(cmd_args))


@vm_util.Retry(poll_interval=5, timeout=60)
def _WaitForRedisUp(vm, port):
  """Wait until redis server is up on a given port."""
  localhost = vm.GetLocalhostAddr()
  vm.RemoteCommand(
      f'sudo {GetRedisDir()}/src/redis-cli -h {localhost} -p {port} ping | grep'
      ' PONG'
  )


def Start(vm) -> None:
  """Start redis server process."""
  ports = GetRedisPorts(vm)
  for port in ports:
    vm.RemoteCommand(_BuildStartCommand(vm, port))
    # The redis-server command starts in the background, so we need to wait for
    # it to be up before issuing commands to it.
    _WaitForRedisUp(vm, port)


def Stop(vm) -> None:
  """Stops redis server processes, flushes all keys, and resets the cluster."""
  redis_dir = GetRedisDir()
  ports = GetRedisPorts(vm)
  localhost = vm.GetLocalhostAddr()

  for port in ports:
    vm.TryRemoteCommand(
        f'sudo {redis_dir}/src/redis-cli -h {localhost} -p {port} flushall'
    )

  for port in ports:
    vm.TryRemoteCommand(
        f'sudo {redis_dir}/src/redis-cli -h {localhost} -p {port} cluster reset'
        ' hard'
    )

  for port in ports:
    # Gracefully send SHUTDOWN command to redis server.
    vm.TryRemoteCommand(
        f'sudo {redis_dir}/src/redis-cli -h {localhost} -p {port} shutdown'
    )

  for port in ports:
    # Check that redis server is not running anymore.
    _, stderr, return_code = vm.RemoteCommandWithReturnCode(
        f'sudo {redis_dir}/src/redis-cli -h {localhost} -p {port} ping',
        ignore_failure=True,
    )
    if return_code == 0 or 'Could not connect to Redis' not in stderr:
      raise errors.Error(f'Redis on port {port} failed to shut down.')


def StartCluster(server_vms) -> None:
  """Start redis cluster; assumes redis shards started with cluster mode."""
  cluster_create_cmd = f'sudo {GetRedisDir()}/src/redis-cli --cluster create '
  for server_vm in server_vms:
    cluster_create_cmd += f' {server_vm.internal_ip}:{DEFAULT_PORT}'
  stdout, _ = server_vms[0].RemoteCommand(f'echo "yes" | {cluster_create_cmd}')
  assert 'All 16384 slots covered' in stdout, 'incorrect redis cluster output'


def GetMetadata(vm) -> Dict[str, Any]:
  num_processes = _GetNumProcesses(vm)
  return {
      'redis_server_version': _VERSION.value,
      'redis_server_cluster_mode': CLUSTER_MODE.value,
      'redis_server_io_threads': (
          _GetIOThreads(vm) if _VERSION.value >= '6.2.1' else 0
      ),
      'redis_server_io_threads_do_reads': _IO_THREADS_DO_READS.value,
      'redis_server_io_threads_cpu_affinity': _IO_THREAD_AFFINITY.value,
      'redis_server_enable_snapshots': _ENABLE_SNAPSHOTS.value,
      'redis_server_num_processes': num_processes,
      'redis_aof': REDIS_AOF.value,
  }


def GetRedisPorts(vm=None) -> List[int]:
  """Returns a list of redis port(s)."""
  num_processes = _GetNumProcesses(vm)
  return [DEFAULT_PORT + i for i in range(num_processes)]


def VerifyRedisAof(vm) -> None:
  """Apply data validation to the AOF backups."""
  localhost = vm.GetLocalhostAddr()
  ports = GetRedisPorts(vm)
  for port in ports:
    redis_config_dir = _GetRedisConfigDir(vm, localhost, port)
    _, stderr, return_code = vm.RemoteCommandWithReturnCode(
        f'sudo redis-check-aof {redis_config_dir}/{_GetRedisAofFilename(port)}'
    )
    if return_code == 0:
      logging.info(
          'Validation succeeded for the AOF backup file %s',
          f'{redis_config_dir}/{_GetRedisAofFilename(port)}',
      )
    else:
      raise errors.Error(
          'Validation failed with the AOF backup file'
          f' {redis_config_dir}/{_GetRedisAofFilename(port)}, stderr: {stderr}'
      )
