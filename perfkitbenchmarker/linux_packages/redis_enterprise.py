# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing redis enterprise installation and cleanup functions.

TODO(user): Flags should be unified with memtier.py.
"""

import dataclasses
import json
import logging
import posixpath
from typing import List, Optional

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
_LICENSE = flags.DEFINE_string(
    'enterprise_redis_license_file', 'enterprise_redis_license',
    'Name of the redis enterprise license file to use.')
_LICENSE_PATH = flags.DEFINE_string(
    'enterprise_redis_license_path', None,
    'If none, defaults to local data directory joined with _LICENSE.')
_TUNE_ON_STARTUP = flags.DEFINE_boolean(
    'enterprise_redis_tune_on_startup', True,
    'Whether to tune core config during startup.')
_PROXY_THREADS = flags.DEFINE_integer(
    'enterprise_redis_proxy_threads', 24,
    'Number of redis proxy threads to use.')
_SHARDS = flags.DEFINE_integer(
    'enterprise_redis_shard_count', 6,
    'Number of redis shard. Each shard is a redis thread.')
_LOAD_RECORDS = flags.DEFINE_integer(
    'enterprise_redis_load_records', 1000000,
    'Number of keys to pre-load into Redis.')
_RUN_RECORDS = flags.DEFINE_integer(
    'enterprise_redis_run_records', 1000000,
    'Number of requests per loadgen client to send to the '
    'Redis server.')
_PIPELINES = flags.DEFINE_integer(
    'enterprise_redis_pipeline', 9,
    'Number of pipelines to use.')
_LOADGEN_CLIENTS = flags.DEFINE_integer(
    'enterprise_redis_loadgen_clients', 24,
    'Number of clients per loadgen vm.')
_MAX_THREADS = flags.DEFINE_integer(
    'enterprise_redis_max_threads', 40,
    'Maximum number of memtier threads to use.')
_MIN_THREADS = flags.DEFINE_integer(
    'enterprise_redis_min_threads', 18,
    'Minimum number of memtier threads to use.')
_THREAD_INCREMENT = flags.DEFINE_integer(
    'enterprise_redis_thread_increment', 1,
    'Number of memtier threads to increment by.')
_LATENCY_THRESHOLD = flags.DEFINE_integer(
    'enterprise_redis_latency_threshold', 1100,
    'The latency threshold in microseconds '
    'until the test stops.')
_PIN_WORKERS = flags.DEFINE_boolean(
    'enterprise_redis_pin_workers', False,
    'Whether to pin the proxy threads after startup.')
_DISABLE_CPU_IDS = flags.DEFINE_list(
    'enterprise_redis_disable_cpu_ids', None,
    'List of cpus to disable by id.')
_DATA_SIZE = flags.DEFINE_integer(
    'enterprise_redis_data_size_bytes', 100,
    'The size of the data to write to redis enterprise.')

_VM = virtual_machine.VirtualMachine

_VERSION = '6.2.4-54'
_PACKAGE_NAME = 'redis_enterprise'
_WORKING_DIR = '~/redislabs'
_RHEL_TAR = f'redislabs-{_VERSION}-rhel7-x86_64.tar'
_XENIAL_TAR = f'redislabs-{_VERSION}-xenial-amd64.tar'
_BIONIC_TAR = f'redislabs-{_VERSION}-bionic-amd64.tar'
_USERNAME = 'user@google.com'
_ONE_KILOBYTE = 1000
PREPROVISIONED_DATA = {
    # These checksums correspond to version 6.2.4-54. To update, run
    # 'sha256sum <redislabs-{VERSION}-rhel7-x86_64.tar>' and replace the values
    # below.
    _RHEL_TAR:
        'fb0b7aa5f115eb0bc2ac4fb958aaa7ad92bb260f2251a221a15b01fbdf4d2d14',
    _XENIAL_TAR:
        'f78a6bb486f3dfb3e5ba9b5be86b1c880d0c76a08eb0dc4bd3aaaf9cc210406d',
    _BIONIC_TAR:
        'dfe568958b243368c1f1c08c9cce9f660fa06e1bce38fa88f90503e344466927',
}


def _GetTarName() -> Optional[str]:
  """Returns the Redis Enterprise package to use depending on the os.

  For information about available packages, see
  https://redislabs.com/redis-enterprise/software/downloads/.
  """
  if FLAGS.os_type in [os_types.RHEL, os_types.AMAZONLINUX2, os_types.CENTOS7]:
    return _RHEL_TAR
  if FLAGS.os_type in [os_types.UBUNTU1604, os_types.DEBIAN, os_types.DEBIAN9]:
    return _XENIAL_TAR
  if FLAGS.os_type == os_types.UBUNTU1804:
    return _BIONIC_TAR


def Install(vm: _VM) -> None:
  """Installs Redis Enterprise package on the VM."""
  vm.InstallPackages('wget')
  vm.RemoteCommand(f'mkdir -p {_WORKING_DIR}')

  # Check for the license in the data directory if a path isn't specified.
  license_path = _LICENSE_PATH.value
  if not license_path:
    license_path = data.ResourcePath(_LICENSE.value)
  vm.PushFile(license_path, posixpath.join(_WORKING_DIR, _LICENSE.value))

  # Check for the tarfile in the data directory first.
  vm.InstallPreprovisionedPackageData(_PACKAGE_NAME, [_GetTarName()],
                                      _WORKING_DIR)
  vm.RemoteCommand('cd {dir} && sudo tar xvf {tar}'.format(
      dir=_WORKING_DIR, tar=_GetTarName()))

  if FLAGS.os_type == os_types.UBUNTU1804:
    # Fix Ubuntu 18.04 DNS conflict
    vm.RemoteCommand(
        'echo "DNSStubListener=no" | sudo tee -a /etc/systemd/resolved.conf')
    vm.RemoteCommand('sudo mv /etc/resolv.conf /etc/resolv.conf.orig')
    vm.RemoteCommand(
        'sudo ln -s /run/systemd/resolve/resolv.conf /etc/resolv.conf')
    vm.RemoteCommand('sudo service systemd-resolved restart')
  install_cmd = './install.sh -y'
  if not _TUNE_ON_STARTUP.value:
    install_cmd = 'CONFIG_systune=no ./install.sh -y -n'
  vm.RemoteCommand('cd {dir} && sudo {install}'.format(
      dir=_WORKING_DIR, install=install_cmd))


def _JoinCluster(server_vm: _VM, vm: _VM) -> None:
  """Joins a Redis Enterprise cluster."""
  logging.info('Joining redis enterprise cluster.')
  vm.RemoteCommand('sudo /opt/redislabs/bin/rladmin cluster join '
                   'nodes {server_vm_ip} '
                   'username {username} '
                   'password {password} '.format(
                       server_vm_ip=server_vm.internal_ip,
                       username=_USERNAME,
                       password=FLAGS.run_uri))


def CreateCluster(vms: List[_VM]) -> None:
  """Creates a Redis Enterprise cluster on the VM."""
  logging.info('Creating redis enterprise cluster.')
  vms[0].RemoteCommand('sudo /opt/redislabs/bin/rladmin cluster create '
                       'license_file {license_file} '
                       'name redis-cluster '
                       'username {username} '
                       'password {password} '.format(
                           license_file=posixpath.join(_WORKING_DIR,
                                                       _LICENSE.value),
                           username=_USERNAME,
                           password=FLAGS.run_uri))
  for vm in vms[1:]:
    _JoinCluster(vms[0], vm)


def OfflineCores(vms: List[_VM]) -> None:
  """Offlines specific cores."""

  def _Offline(vm):
    for cpu_id in _DISABLE_CPU_IDS.value or []:
      vm.RemoteCommand('sudo bash -c '
                       '"echo 0 > /sys/devices/system/cpu/cpu%s/online"' %
                       cpu_id)

  vm_util.RunThreaded(_Offline, vms)


def TuneProxy(vms: List[_VM], proxy_threads: Optional[int] = None) -> None:
  """Tunes the number of Redis proxies on the server vm."""
  proxy_threads = proxy_threads or _PROXY_THREADS.value

  def _Tune(vm):
    vm.RemoteCommand('sudo /opt/redislabs/bin/rladmin tune '
                     'proxy all '
                     f'max_threads {proxy_threads} '
                     f'threads {proxy_threads} ')
    vm.RemoteCommand('sudo /opt/redislabs/bin/dmc_ctl restart')

  vm_util.RunThreaded(_Tune, vms)


def PinWorkers(vms: List[_VM]) -> None:
  """Splits the Redis worker threads across the NUMA nodes evenly.

  This function is no-op if --enterprise_redis_pin_workers is not set.

  Args:
    vms: The VMs with the Redis workers to pin.
  """
  if not _PIN_WORKERS.value:
    return

  def _Pin(vm):
    numa_nodes = vm.CheckLsCpu().numa_node_count
    proxies_per_node = _PROXY_THREADS.value // numa_nodes
    for node in range(numa_nodes):
      node_cpu_list = vm.RemoteCommand(
          'cat /sys/devices/system/node/node%d/cpulist' % node)[0].strip()
      # List the PIDs of the Redis worker processes and pin a sliding window of
      # `proxies_per_node` workers to the NUMA nodes in increasing order.
      vm.RemoteCommand(
          r'sudo /opt/redislabs/bin/dmc-cli -ts root list | '
          r'grep worker | '
          r'head -n -{proxies_already_partitioned} | '
          r'tail -n {proxies_per_node} | '
          r"awk '"
          r'{{printf "%i\n",$3}}'
          r"' | "
          r'xargs -i sudo taskset -pc {node_cpu_list} {{}} '.format(
              proxies_already_partitioned=proxies_per_node * node,
              proxies_per_node=proxies_per_node,
              node_cpu_list=node_cpu_list))

  vm_util.RunThreaded(_Pin, vms)


def _GetRestCommand() -> str:
  return (f'curl -v -k -u {_USERNAME}:{FLAGS.run_uri} '
          'https://localhost:9443/v1/bdbs')


def CreateDatabase(vms: List[_VM], redis_port: int) -> None:
  """Creates a new Redis Enterprise database.

  See https://docs.redis.com/latest/rs/references/rest-api/objects/bdb/.

  Args:
    vms: The Redis Enterprise DB cluster nodes.
    redis_port: The port to serve the database.
  """
  if _SHARDS.value < len(vms):
    raise errors.Config.InvalidValue(
        'There should be at least 1 shard per server VM.')
  content = {
      'name': 'redisdb',
      'memory_size': int(vms[0].total_memory_kb * _ONE_KILOBYTE / 2 * len(vms)),
      'type': 'redis',
      'proxy_policy': 'all-master-shards',
      'port': redis_port,
      'sharding': False,
      'authentication_redis_pass': FLAGS.run_uri,
      'replication': False,
  }
  if _SHARDS.value > 1:
    content.update({
        'sharding': True,
        'shards_count': _SHARDS.value,
        'shards_placement': 'sparse',
        'oss_cluster': True,
        'shard_key_regex':
            [{'regex': '.*\\{(?<tag>.*)\\}.*'}, {'regex': '(?<tag>.*)'}]
    })

  logging.info('Creating Redis Enterprise database.')
  vms[0].RemoteCommand(f'{_GetRestCommand()} '
                       "-H 'Content-type: application/json' "
                       f"-d '{json.dumps(content)}'")


@vm_util.Retry()
def WaitForDatabaseUp(vm: _VM, redis_port: int) -> None:
  """Waits for the Redis Enterprise database to respond to commands."""
  stdout, _ = vm.RemoteCommand(
      'sudo /opt/redislabs/bin/redis-cli '
      '-h localhost '
      '-p {port} '
      '-a {password} '
      'ping'.format(
          password=FLAGS.run_uri,
          port=redis_port))
  if stdout.find('PONG') == -1:
    raise errors.Resource.RetryableCreationError()


@dataclasses.dataclass(frozen=True)
class LoadRequest():
  key_minimum: int
  key_maximum: int
  redis_port: int
  cluster_mode: bool
  server_ip: str


def _BuildLoadCommand(request: LoadRequest) -> str:
  """Returns the command used to load the database."""
  command = (
      'sudo /opt/redislabs/bin/memtier_benchmark '
      f'-s {request.server_ip} '
      f'-a {FLAGS.run_uri} '
      f'-p {str(request.redis_port)} '
      '-t 1 '  # Set -t and -c to 1 to avoid duplicated work in writing the same
      '-c 1 '  # key/value pairs repeatedly.
      '--ratio 1:0 '
      '--pipeline 100 '
      f'-d {str(_DATA_SIZE.value)} '
      '--key-pattern S:S '
      f'--key-minimum {request.key_minimum} '
      f'--key-maximum {request.key_maximum} '
      '-n allkeys ')
  if request.cluster_mode:
    command += '--cluster-mode'
  return command


def _LoadDatabaseSingleVM(load_vm: _VM, request: LoadRequest) -> None:
  """Loads the DB from a single VM."""
  command = _BuildLoadCommand(request)
  logging.info('Loading database with %s', request)
  load_vm.RemoteCommand(command)


def LoadDatabase(redis_vms: List[_VM], load_vms: List[_VM],
                 redis_port: int) -> None:
  """Loads the database before performing tests."""
  vms = load_vms + redis_vms
  load_requests = []
  load_records_per_vm = _LOAD_RECORDS.value // len(vms)
  cluster_mode = len(redis_vms) > 1
  for i, _ in enumerate(vms):
    load_requests.append(
        LoadRequest(
            key_minimum=max(i * load_records_per_vm, 1),
            key_maximum=(i + 1) * load_records_per_vm,
            redis_port=redis_port,
            cluster_mode=cluster_mode,
            server_ip=redis_vms[0].internal_ip))

  vm_util.RunThreaded(
      _LoadDatabaseSingleVM,
      [((vm, request), {}) for vm, request in zip(vms, load_requests)])


def _BuildRunCommand(redis_vms: _VM, threads: int, port: int) -> str:
  """Spawns a memtier_benchmark on the load_vm against the redis_vm:port.

  Args:
    redis_vms: The target of the memtier_benchmark
    threads: The number of threads to run in this memtier_benchmark process.
    port: the port to target on the redis_vm.

  Returns:
    Command to issue to the redis server.
  """
  if threads == 0:
    return None

  result = ('sudo /opt/redislabs/bin/memtier_benchmark '
            f'-s {redis_vms[0].internal_ip} '
            f'-a {FLAGS.run_uri} '
            f'-p {str(port)} '
            f'-t {str(threads)} '
            '--ratio 1:1 '
            f'--pipeline {str(_PIPELINES.value)} '
            f'-c {str(_LOADGEN_CLIENTS.value)} '
            f'-d {str(_DATA_SIZE.value)} '
            '--test-time 15 '
            '--key-minimum 1 '
            f'--key-maximum {str(_LOAD_RECORDS.value)} ')
  if len(redis_vms) > 1:
    result += '--cluster-mode '
  return result


def Run(redis_vms: List[_VM], load_vms: List[_VM],
        redis_port: int) -> List[sample.Sample]:
  """Run memtier against enterprise redis and measure latency and throughput.

  This function runs memtier against the redis server vm with increasing memtier
  threads until one of following conditions is reached:
    - FLAGS.enterprise_redis_max_threads is reached
    - FLAGS.enterprise_redis_latency_threshold is reached

  Args:
    redis_vms: Redis server vms.
    load_vms: Memtier load vms.
    redis_port: Port for the redis server.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  cur_max_latency = 0.0
  latency_threshold = _LATENCY_THRESHOLD.value
  threads = _MIN_THREADS.value
  max_threads = _MAX_THREADS.value
  max_throughput_for_completion_latency_under_1ms = 0.0
  redis_vm = redis_vms[0]

  while (cur_max_latency < latency_threshold and threads <= max_threads):
    load_command = _BuildRunCommand(redis_vms, threads, redis_port)
    # 1min for throughput to stabilize and 10sec of data.
    measurement_command = (
        'sleep 15 && curl -v -k -u {user}:{password} '
        'https://localhost:9443/v1/bdbs/stats?interval=1sec > ~/output'.format(
            user=_USERNAME,
            password=FLAGS.run_uri,
        ))
    args = [((load_vm, load_command), {}) for load_vm in load_vms]
    args += [((redis_vm, measurement_command), {})]
    vm_util.RunThreaded(lambda vm, command: vm.RemoteCommand(command), args)
    stdout, _ = redis_vm.RemoteCommand('cat ~/output')
    output = json.loads(stdout)[0]
    intervals = output.get('intervals')

    for interval in intervals:
      throughput = interval.get('total_req')
      latency = interval.get('avg_latency')
      cur_max_latency = max(cur_max_latency, latency)
      sample_metadata = interval
      sample_metadata['redis_tune_on_startup'] = (
          _TUNE_ON_STARTUP.value)
      sample_metadata['redis_pipeline'] = (
          _PIPELINES.value)
      sample_metadata['threads'] = threads
      sample_metadata['shard_count'] = _SHARDS.value
      sample_metadata['total_shard_count'] = _SHARDS.value * len(redis_vms)
      sample_metadata['redis_proxy_threads'] = (
          _PROXY_THREADS.value)
      sample_metadata['redis_loadgen_clients'] = (
          _LOADGEN_CLIENTS.value)
      sample_metadata['pin_workers'] = _PIN_WORKERS.value
      sample_metadata['disable_cpus'] = _DISABLE_CPU_IDS.value
      sample_metadata['redis_enterprise_version'] = _VERSION
      sample_metadata['memtier_data_size'] = _DATA_SIZE.value
      sample_metadata['memtier_key_maximum'] = _LOAD_RECORDS.value
      results.append(sample.Sample('throughput', throughput, 'ops/s',
                                   sample_metadata))
      if latency < 1000:
        max_throughput_for_completion_latency_under_1ms = max(
            max_throughput_for_completion_latency_under_1ms, throughput)

      logging.info('Threads : %d  (%f, %f) < %f', threads, throughput, latency,
                   latency_threshold)

    threads += _THREAD_INCREMENT.value

  if cur_max_latency >= 1000:
    results.append(sample.Sample(
        'max_throughput_for_completion_latency_under_1ms',
        max_throughput_for_completion_latency_under_1ms, 'ops/s',
        sample_metadata))

  return results
