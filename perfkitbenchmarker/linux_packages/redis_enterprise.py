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
import time
from typing import Any, Dict, List, Optional, Tuple

from absl import flags
import numpy as np
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
    'enterprise_redis_proxy_threads', None,
    'Number of redis proxy threads to use.')
_SHARDS = flags.DEFINE_integer(
    'enterprise_redis_shard_count', None,
    'Number of redis shards per database. Each shard is a redis thread.')
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
_ThroughputSampleTuple = Tuple[float, List[sample.Sample]]
_ThroughputSampleMatrix = List[List[_ThroughputSampleTuple]]

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

_THREAD_OPTIMIZATION_RATIO = 0.75


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


def TuneProxy(vm: _VM, proxy_threads: Optional[int] = None) -> None:
  """Tunes the number of Redis proxies on the cluster."""
  proxy_threads = proxy_threads or _PROXY_THREADS.value
  vm.RemoteCommand('sudo /opt/redislabs/bin/rladmin tune '
                   'proxy all '
                   f'max_threads {proxy_threads} '
                   f'threads {proxy_threads} ')
  vm.RemoteCommand('sudo /opt/redislabs/bin/dmc_ctl restart')


def PinWorkers(vms: List[_VM], proxy_threads: Optional[int] = None) -> None:
  """Splits the Redis worker threads across the NUMA nodes evenly.

  This function is no-op if --enterprise_redis_pin_workers is not set.

  Args:
    vms: The VMs with the Redis workers to pin.
    proxy_threads: The number of proxy threads per VM.
  """
  if not _PIN_WORKERS.value:
    return

  proxy_threads = proxy_threads or _PROXY_THREADS.value

  def _Pin(vm):
    numa_nodes = vm.CheckLsCpu().numa_node_count
    proxies_per_node = proxy_threads // numa_nodes
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


def CreateDatabase(vms: List[_VM], redis_port: int,
                   shards: Optional[int] = None) -> None:
  """Creates a new Redis Enterprise database.

  See https://docs.redis.com/latest/rs/references/rest-api/objects/bdb/.

  Args:
    vms: The server VMs.
    redis_port: The port to serve the database.
    shards: Number of shards for the database. In a clustered setup, shards will
      be distributed evenly across nodes.
  """
  db_shards = shards or _SHARDS.value
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
  if db_shards > 1:
    content.update({
        'sharding': True,
        'shards_count': db_shards,
        'shards_placement': 'sparse',
        'oss_cluster': True,
        'shard_key_regex':
            [{'regex': '.*\\{(?<tag>.*)\\}.*'}, {'regex': '(?<tag>.*)'}]
    })  # pyformat: disable

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


def _GetDatabase(vm: _VM) -> Optional[str]:
  """Gets the database object running in the cluster.

  Args:
    vm: The VM where the cluster is set up.

  Returns:
    The database object per
    https://docs.redis.com/latest/rs/references/rest-api/objects/bdb/.

  Raises:
    errors.Benchmarks.RunError: If there is more than one database found.
  """
  stdout, _ = vm.RemoteCommand(f'{_GetRestCommand()}')
  results = json.loads(stdout)
  if len(results) != 1:
    logging.info('Expected one database, found %s.', len(results))
    return None
  return results[0]


def DeleteDatabase(vm: _VM) -> None:
  logging.info('Deleting Redis Enterprise database.')
  uid = _GetDatabase(vm)['uid']
  vm.RemoteCommand(f'{_GetRestCommand()}/{uid} -X DELETE')
  while _GetDatabase(vm) is not None:
    time.sleep(5)


def _BuildRunCommand(redis_vms: List[_VM], threads: int, port: int) -> str:
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
            '--test-time 20 '
            '--key-minimum 1 '
            f'--key-maximum {str(_LOAD_RECORDS.value)} ')
  if len(redis_vms) > 1:
    result += '--cluster-mode '
  return result


@dataclasses.dataclass(frozen=True)
class Result():
  """Individual throughput and latency result."""
  throughput: int
  latency_usec: int
  metadata: Dict[str, Any]


@dataclasses.dataclass
class AggregateResult():
  """Aggregated throughput and latency results for whole cluster of DBs.

  Attributes:
    results: Maps database UID to a list of intervals with data from the run.
    throughputs: Time series of average throughputs.
    latencies: Time series of average latencies.
  """
  results: Dict[int, List[Result]] = dataclasses.field(default_factory=dict)

  def _GetAverageList(self, attr: str) -> List[int]:
    """Returns a list that represents the average of multiple lists.

    Args:
      attr: The attribute corresponding to an integer attribute of Result,
        either "throughput" or "latency_usec".

    Returns:
      A list of the average attribute per interval time across DBs. For example:
      db1 = [2, 1, 3, 5, 6]
      db2 = [4, 3, 3, 7, 2]
      ret = [3, 2, 3, 6, 4] (Average)
    """
    lists = []
    for l in self.results.values():
      lists.append([getattr(result, attr) for result in l])
    result = np.average(np.array(lists), axis=0)
    logging.info('Raw %s: %s', attr, np.array(lists))
    logging.info('Average %s: %s', attr, result)
    return result

  @property
  def throughputs(self) -> List[int]:
    return self._GetAverageList('throughput')

  @property
  def latencies(self) -> List[int]:
    return self._GetAverageList('latency_usec')

  def ToSamples(self, metadata: Dict[str, Any]) -> List[sample.Sample]:
    """Returns throughput samples attached with the given metadata."""
    samples = []
    for result_list in self.results.values():
      for r in result_list:
        r.metadata.update(metadata)
        samples.append(
            sample.Sample('throughput', r.throughput, 'ops/s', r.metadata))
    return samples


def ParseResult(output: str) -> AggregateResult:
  """Parses the result from the database statistics API."""
  output_json = json.loads(output)
  results = {}
  time_series_length = float('inf')
  # Parse database output
  for database in output_json:
    uid = database.get('uid')
    results[uid] = []
    for interval in database.get('intervals'):
      result = Result(interval.get('total_req'),
                      interval.get('avg_latency'),
                      interval)
      results[uid].append(result)
    # Intervals can sometimes have different lengths (length 8-10).
    time_series_length = min(time_series_length, len(results[uid]))
  for result in results.values():
    del result[time_series_length:]
  return AggregateResult(results=results)


def Run(redis_vms: List[_VM],
        load_vms: List[_VM],
        redis_port: int,
        shards: Optional[int] = None,
        proxy_threads: Optional[int] = None,
        memtier_threads: Optional[int] = None) -> _ThroughputSampleTuple:
  """Run memtier against enterprise redis and measure latency and throughput.

  This function runs memtier against the redis server vm with increasing memtier
  threads until one of following conditions is reached:
    - FLAGS.enterprise_redis_max_threads is reached
    - FLAGS.enterprise_redis_latency_threshold is reached

  Args:
    redis_vms: Redis server vms.
    load_vms: Memtier load vms.
    redis_port: Port for the redis server.
    shards: The per-DB shard count for this run.
    proxy_threads: The per-VM proxy thread count for this run.
    memtier_threads: If provided, overrides --enterprise_redis_min_threads.

  Returns:
    A tuple of (max_throughput_under_1ms, list of sample.Sample objects).
  """
  results = []
  cur_max_latency = 0.0
  latency_threshold = _LATENCY_THRESHOLD.value
  shards = shards or _SHARDS.value
  threads = memtier_threads or _MIN_THREADS.value
  max_threads = _MAX_THREADS.value
  max_throughput_for_completion_latency_under_1ms = 0.0
  redis_vm = redis_vms[0]

  if threads > max_threads:
    raise errors.Benchmarks.RunError(
        'min threads %s higher than max threads %s, '
        'raise --enterprise_redis_max_threads')

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

    result = ParseResult(stdout)
    metadata = GetMetadata(shards, threads, proxy_threads)
    results.extend(result.ToSamples(metadata))

    for throughput, latency in zip(result.throughputs, result.latencies):
      cur_max_latency = max(cur_max_latency, latency)
      if latency < 1000:
        max_throughput_for_completion_latency_under_1ms = max(
            max_throughput_for_completion_latency_under_1ms, throughput)

      logging.info('Threads : %d  (%f ops/sec, %f ms latency) < %f ms latency',
                   threads, throughput, latency, latency_threshold)

    threads += _THREAD_INCREMENT.value

  if cur_max_latency >= 1000:
    results.append(sample.Sample(
        'max_throughput_for_completion_latency_under_1ms',
        max_throughput_for_completion_latency_under_1ms, 'ops/s', metadata))

  logging.info('Max throughput under 1ms: %s ops/sec.',
               max_throughput_for_completion_latency_under_1ms)
  return max_throughput_for_completion_latency_under_1ms, results


def GetMetadata(shards: int, threads: int,
                proxy_threads: int) -> Dict[str, Any]:
  """Returns metadata associated with the run.

  Args:
    shards: The shard count per database.
    threads: The thread count used by the memtier client.
    proxy_threads: The proxy thread count used on the redis cluster.

  Returns:
    A dictionary of metadata that can be attached to the run sample.
  """
  return {
      'redis_tune_on_startup': _TUNE_ON_STARTUP.value,
      'redis_pipeline': _PIPELINES.value,
      'threads': threads,
      'db_shard_count': shards,
      # TODO(user): Update total_shard_count and db_count once
      # multi-DB support is added.
      'total_shard_count': shards,
      'db_count': 1,
      'redis_proxy_threads': proxy_threads or _PROXY_THREADS.value,
      'redis_loadgen_clients': _LOADGEN_CLIENTS.value,
      'pin_workers': _PIN_WORKERS.value,
      'disable_cpus': _DISABLE_CPU_IDS.value,
      'redis_enterprise_version': _VERSION,
      'memtier_data_size': _DATA_SIZE.value,
      'memtier_key_maximum': _LOAD_RECORDS.value,
      'replication': False
  }


class ThroughputOptimizer():
  """Class that searches for the shard/proxy_thread count for best throughput.

  Attributes:
    server_vms: List of server VMs.
    client_vms: List of client VMs.
    redis_port: The port the Redis Enterprise database is served on.
    results: Matrix that records the search space of shards and proxy threads.
    min_threads: Keeps track of the optimal thread count used in previous run.
  """

  def __init__(self, server_vms: List[_VM], client_vms: List[_VM],
               redis_port: int):
    self.server_vms: List[_VM] = server_vms
    self.client_vms: List[_VM] = client_vms
    self.redis_port: int = redis_port
    self.min_threads: int = _MIN_THREADS.value

    # Determines the search space for the optimization algorithm. We multiply
    # the size by 2 which should be a large enough search space.
    matrix_size = max(server_vms[0].num_cpus,
                      FLAGS.enterprise_redis_proxy_threads or 0,
                      FLAGS.enterprise_redis_shard_count or 0) * 2
    self.results: _ThroughputSampleMatrix = (
        [[() for i in range(matrix_size)] for i in range(matrix_size)])

  def _CreateAndLoadDatabase(self, shards: int) -> None:
    CreateDatabase(self.server_vms, self.redis_port, shards)
    WaitForDatabaseUp(self.server_vms[0], self.redis_port)
    LoadDatabase(self.server_vms, self.client_vms, self.redis_port)

  def _FullRun(self, shard_count: int,
               proxy_thread_count: int) -> _ThroughputSampleTuple:
    """Recreates DB if needed, then runs the test."""
    logging.info('Starting new run with %s shards, %s proxy threads',
                 shard_count, proxy_thread_count)
    server_vm = self.server_vms[0]

    # Recreate the DB if needed
    db = _GetDatabase(server_vm)
    if not db:
      self._CreateAndLoadDatabase(shard_count)
    elif shard_count != db['shards_count']:
      DeleteDatabase(server_vm)
      self._CreateAndLoadDatabase(shard_count)

    # Tune
    TuneProxy(server_vm, proxy_thread_count)
    PinWorkers(self.server_vms, proxy_thread_count)

    # Optimize the number of threads and run the test
    results = Run(self.server_vms,
                  self.client_vms,
                  self.redis_port,
                  shard_count,
                  proxy_thread_count,
                  self.min_threads)  # pyformat: disable
    self.min_threads = max(
        self.min_threads,
        int(results[1][-1].metadata['threads'] * _THREAD_OPTIMIZATION_RATIO))
    return results

  def _GetResult(self, shards: int,
                 proxy_threads: int) -> _ThroughputSampleTuple:
    if not self.results[shards - 1][proxy_threads - 1]:
      self.results[shards - 1][proxy_threads - 1] = self._FullRun(
          shards * len(self.server_vms), proxy_threads)
    return self.results[shards - 1][proxy_threads - 1]

  def _GetNeighborsToCheck(self, shards: int,
                           proxy_threads: int) -> List[Tuple[int, int]]:
    """Returns the shards/proxy_threads neighbor to check."""
    vary_proxy_threads = [(shards, proxy_threads - 1),
                          (shards, proxy_threads + 1)]
    vary_shards = [(shards - 1, proxy_threads), (shards + 1, proxy_threads)]
    return vary_proxy_threads + vary_shards

  def _GetOptimalNeighbor(self, shards: int,
                          proxy_threads: int) -> Tuple[int, int]:
    """Returns the shards/proxy_threads neighbor with the best throughput."""
    optimal_shards = shards
    optimal_proxy_threads = proxy_threads
    optimal_throughput, _ = self._GetResult(shards, proxy_threads)

    for shards_count, proxy_threads_count in self._GetNeighborsToCheck(
        shards, proxy_threads):
      if (shards_count < 1 or shards_count > len(self.results) or
          proxy_threads_count < 1 or proxy_threads_count > len(self.results)):
        continue

      throughput, _ = self._GetResult(shards_count, proxy_threads_count)

      if throughput > optimal_throughput:
        optimal_throughput = throughput
        optimal_shards = shards_count
        optimal_proxy_threads = proxy_threads_count

    return optimal_shards, optimal_proxy_threads

  def DoGraphSearch(self) -> _ThroughputSampleTuple:
    """Performs a graph search with the optimal shards AND proxy thread count.

    Performs a graph search through shards and proxy threads. If the current
    combination is a local maximum, finish and return the result.

    The DB needs to be recreated since shards can only be resized in multiples
    once created.

    Returns:
      Tuple of (optimal_throughput, samples).
    """
    # Uses a heuristic for the number of shards and proxy threads per VM
    # Usually the optimal number is somewhere close to this.
    num_cpus = self.server_vms[0].num_cpus
    shard_count = max(num_cpus // 5, 1)  # Per VM on 1 database
    proxy_thread_count = num_cpus - shard_count

    while True:
      logging.info('Checking shards: %s, proxy_threads: %s', shard_count,
                   proxy_thread_count)
      optimal_shards, optimal_proxies = self._GetOptimalNeighbor(
          shard_count, proxy_thread_count)
      if (shard_count, proxy_thread_count) == (optimal_shards, optimal_proxies):
        break
      shard_count = optimal_shards
      proxy_thread_count = optimal_proxies

    return self._GetResult(shard_count, proxy_thread_count)

  def DoLinearSearch(self) -> _ThroughputSampleTuple:
    """Performs a linear search using either shards or proxy threads."""
    logging.info('Performing linear search through proxy threads OR shards.')
    max_throughput_tuple = (0, None)
    num_cpus = self.server_vms[0].num_cpus
    for i in range(1, num_cpus):
      if _SHARDS.value:
        result = self._FullRun(_SHARDS.value, i)
      else:
        result = self._FullRun(i, _PROXY_THREADS.value)
      if result[0] > max_throughput_tuple[0]:
        max_throughput_tuple = result
    return max_throughput_tuple

  def GetOptimalThroughput(self) -> _ThroughputSampleTuple:
    """Gets the optimal throughput for the Redis Enterprise cluster.

    Returns:
      Tuple of (optimal_throughput, samples).
    """
    # If only optimizing proxy threads, do a linear search.
    if (_SHARDS.value and not _PROXY_THREADS.value) or (
        _PROXY_THREADS.value and not _SHARDS.value):
      return self.DoLinearSearch()
    return self.DoGraphSearch()


