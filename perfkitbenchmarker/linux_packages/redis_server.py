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
from perfkitbenchmarker import linux_packages


class RedisEvictionPolicy():
  """Enum of options for --redis_eviction_policy."""
  NOEVICTION = 'noeviction'
  ALLKEYS_LRU = 'allkeys-lru'
  VOLATILE_LRU = 'volatile-lru'
  ALLKEYS_RANDOM = 'allkeys-random'
  VOLATILE_RANDOM = 'volatile-random'
  VOLATILE_TTL = 'volatile-ttl'


_VERSION = flags.DEFINE_string('redis_server_version', '6.2.1',
                               'Version of redis server to use.')
_IO_THREADS = flags.DEFINE_integer(
    'redis_server_io_threads', 4, 'Only supported for redis version >= 6, the '
    'number of redis server IO threads to use.')
_IO_THREADS_DO_READS = flags.DEFINE_bool(
    'redis_server_io_threads_do_reads', False,
    'If true, makes both reads and writes use IO threads instead of just '
    'writes.')
_IO_THREAD_AFFINITY = flags.DEFINE_bool(
    'redis_server_io_threads_cpu_affinity', False,
    'If true, attempts to pin IO threads to CPUs.')
_ENABLE_SNAPSHOTS = flags.DEFINE_bool(
    'redis_server_enable_snapshots', False,
    'If true, uses the default redis snapshot policy.')
_NUM_PROCESSES = flags.DEFINE_integer(
    'redis_total_num_processes', 1,
    'Total number of redis server processes. Useful when running with a redis '
    'version lower than 6.',
    lower_bound=1)
_EVICTION_POLICY = flags.DEFINE_enum(
    'redis_eviction_policy', RedisEvictionPolicy.NOEVICTION,
    [RedisEvictionPolicy.NOEVICTION, RedisEvictionPolicy.ALLKEYS_LRU,
     RedisEvictionPolicy.VOLATILE_LRU, RedisEvictionPolicy.ALLKEYS_RANDOM,
     RedisEvictionPolicy.VOLATILE_RANDOM, RedisEvictionPolicy.VOLATILE_TTL],
    'Redis eviction policy when maxmemory limit is reached. This requires '
    'running clients with larger amounts of data than Redis can hold.')


# Default port for Redis
_DEFAULT_PORT = 6379
REDIS_PID_FILE = 'redis.pid'
FLAGS = flags.FLAGS
REDIS_GIT = 'https://github.com/antirez/redis.git'


def _GetRedisTarName() -> str:
  return f'redis-{_VERSION.value}.tar.gz'


def GetRedisDir() -> str:
  return f'{linux_packages.INSTALL_DIR}/redis'


def _Install(vm) -> None:
  """Installs the redis package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand(f'cd {linux_packages.INSTALL_DIR}; git clone {REDIS_GIT}')
  vm.RemoteCommand(
      f'cd {GetRedisDir()} && git checkout {_VERSION.value} && make')


def YumInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-devel')
  vm.InstallPackages('scl-utils centos-release-scl')
  vm.InstallPackages('devtoolset-7 libuuid-devel')
  vm.InstallPackages('openssl openssl-devel curl-devel '
                     'devtoolset-7-libatomic-devel tcl '
                     'tcl-devel git wget epel-release')
  vm.InstallPackages('tcltls libzstd procps-ng')
  vm.RemoteCommand(
      'echo "source scl_source enable devtoolset-7" | sudo tee -a $HOME/.bashrc'
  )
  _Install(vm)


def AptInstall(vm) -> None:
  """Installs the redis package on the VM."""
  vm.InstallPackages('tcl-dev')
  _Install(vm)


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
  # Add check for the MADV_FREE/fork arm64 Linux kernel bug
  if _VERSION.value >= '6.2.1':
    cmd_args.append('--ignore-warnings ARM64-COW-BUG')
  # Snapshotting
  if not _ENABLE_SNAPSHOTS.value:
    cmd_args.append('--save ""')
  # IO threads
  if _IO_THREADS.value:
    cmd_args.append(f'--io-threads {_IO_THREADS.value}')
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
  return cmd.format(redis_dir=redis_dir, args=' '.join(cmd_args))


def Start(vm) -> None:
  """Start redis server process."""
  # Redis tuning parameters, see
  # https://www.techandme.se/performance-tips-for-redis-cache-server/.
  # This command works on 2nd generation of VMs only.
  update_sysvtl = vm.TryRemoteCommand('echo "'
                                      'vm.overcommit_memory = 1\n'
                                      'net.core.somaxconn = 65535\n'
                                      '" | sudo tee -a /etc/sysctl.conf')
  commit_sysvtl = vm.TryRemoteCommand('sudo /usr/sbin/sysctl -p')
  if not (update_sysvtl and commit_sysvtl):
    logging.info('Fail to optimize overcommit_memory and socket connections.')
  for port in GetRedisPorts():
    vm.RemoteCommand(_BuildStartCommand(vm, port))


def GetMetadata() -> Dict[str, Any]:
  return {
      'redis_server_version': _VERSION.value,
      'redis_server_io_threads': _IO_THREADS.value,
      'redis_server_io_threads_do_reads': _IO_THREADS_DO_READS.value,
      'redis_server_io_threads_cpu_affinity': _IO_THREAD_AFFINITY.value,
      'redis_server_enable_snapshots': _ENABLE_SNAPSHOTS.value,
      'redis_server_num_processes': _NUM_PROCESSES.value,
  }


def GetRedisPorts() -> List[int]:
  """Returns a list of redis port(s)."""
  return [_DEFAULT_PORT + i for i in range(_NUM_PROCESSES.value)]
