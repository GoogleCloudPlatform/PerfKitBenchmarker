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
import itertools
import json
import logging
import posixpath
from typing import Any, Dict, List, Optional, Tuple

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
import requests

FLAGS = flags.FLAGS
_LICENSE = flags.DEFINE_string(
    'enterprise_redis_license_file',
    'enterprise_redis_license',
    'Name of the redis enterprise license file to use.',
)
_LICENSE_PATH = flags.DEFINE_string(
    'enterprise_redis_license_path',
    None,
    'If none, defaults to local data directory joined with _LICENSE.',
)
_TUNE_ON_STARTUP = flags.DEFINE_boolean(
    'enterprise_redis_tune_on_startup',
    True,
    'Whether to tune core config during startup.',
)
_PROXY_THREADS = flags.DEFINE_integer(
    'enterprise_redis_proxy_threads',
    None,
    'Number of redis proxy threads to use.',
)
_SHARDS = flags.DEFINE_integer(
    'enterprise_redis_shard_count',
    None,
    'Number of redis shards per database. Each shard is a redis thread.',
)
_LOAD_RECORDS = flags.DEFINE_integer(
    'enterprise_redis_load_records',
    1000000,
    'Number of keys to pre-load into Redis. Use'
    ' --enterprise_redis_data_size_bytes to calculate how much space will be'
    ' used. Due to overhead, a ballpark estimate is 1KB record takes 1.5KB of'
    ' utilization. See'
    ' https://docs.redis.com/latest/rs/concepts/memory-performance/for more'
    ' info.',
)
_RUN_RECORDS = flags.DEFINE_integer(
    'enterprise_redis_run_records',
    1000000,
    'Number of requests per loadgen client to send to the Redis server.',
)
_PIPELINES = flags.DEFINE_integer(
    'enterprise_redis_pipeline', 9, 'Number of pipelines to use.'
)
_LOADGEN_CLIENTS = flags.DEFINE_integer(
    'enterprise_redis_loadgen_clients', 24, 'Number of clients per loadgen vm.'
)
_MAX_THREADS = flags.DEFINE_integer(
    'enterprise_redis_max_threads',
    40,
    'Maximum number of memtier threads to use.',
)
_MIN_THREADS = flags.DEFINE_integer(
    'enterprise_redis_min_threads',
    18,
    'Minimum number of memtier threads to use.',
)
_THREAD_INCREMENT = flags.DEFINE_integer(
    'enterprise_redis_thread_increment',
    1,
    'Number of memtier threads to increment by.',
)
_LATENCY_THRESHOLD = flags.DEFINE_integer(
    'enterprise_redis_latency_threshold',
    1100,
    'The latency threshold in microseconds until the test stops.',
)
_PIN_WORKERS = flags.DEFINE_boolean(
    'enterprise_redis_pin_workers',
    False,
    'Whether to pin the proxy threads after startup.',
)
_DISABLE_CPU_IDS = flags.DEFINE_list(
    'enterprise_redis_disable_cpu_ids', None, 'List of cpus to disable by id.'
)
_DATA_SIZE = flags.DEFINE_integer(
    'enterprise_redis_data_size_bytes',
    100,
    'The size of the data to write to redis enterprise.',
)
_NUM_DATABASES = flags.DEFINE_integer(
    'enterprise_redis_db_count',
    1,
    'The number of databases to create on the cluster.',
)
_REPLICATION = flags.DEFINE_bool(
    'enterprise_redis_db_replication',
    False,
    'If true, replicates each database to another node. Doubles the amount of '
    'memory used by the database records.',
)
_MEMORY_SIZE_PERCENTAGE = flags.DEFINE_float(
    'enterprise_redis_memory_size_percentage',
    0.80,
    'The percentage amount of memory to use out of all the available memory '
    'reported by rladmin for provisioning databases. 1 means use all available '
    'memory, which in practice tends to be error-prone.',
)

_VM = virtual_machine.VirtualMachine
_ThroughputSampleTuple = Tuple[float, List[sample.Sample]]
_ThroughputSampleMatrix = List[List[_ThroughputSampleTuple]]
_Json = Dict[str, Any]
_UidToJsonDict = Dict[int, _Json]

_VERSION = '6.2.4-54'
_PACKAGE_NAME = 'redis_enterprise'
_WORKING_DIR = '~/redislabs'
_RHEL_TAR = f'redislabs-{_VERSION}-rhel7-x86_64.tar'
_XENIAL_TAR = f'redislabs-{_VERSION}-xenial-amd64.tar'
_BIONIC_TAR = f'redislabs-{_VERSION}-bionic-amd64.tar'
_USERNAME = 'user@google.com'
_ONE_KILOBYTE = 1000
_ONE_MEGABYTE = _ONE_KILOBYTE * 1000
_ONE_GIGABYTE = _ONE_MEGABYTE * 1000
PREPROVISIONED_DATA = {
    # These checksums correspond to version 6.2.4-54. To update, run
    # 'sha256sum <redislabs-{VERSION}-rhel7-x86_64.tar>' and replace the values
    # below.
    _RHEL_TAR: (
        'fb0b7aa5f115eb0bc2ac4fb958aaa7ad92bb260f2251a221a15b01fbdf4d2d14'
    ),
    _XENIAL_TAR: (
        'f78a6bb486f3dfb3e5ba9b5be86b1c880d0c76a08eb0dc4bd3aaaf9cc210406d'
    ),
    _BIONIC_TAR: (
        'dfe568958b243368c1f1c08c9cce9f660fa06e1bce38fa88f90503e344466927'
    ),
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
  vm.InstallPreprovisionedPackageData(
      _PACKAGE_NAME, [_GetTarName()], _WORKING_DIR
  )
  vm.RemoteCommand(
      'cd {dir} && sudo tar xvf {tar}'.format(
          dir=_WORKING_DIR, tar=_GetTarName()
      )
  )

  if FLAGS.os_type == os_types.UBUNTU1804:
    # Fix Ubuntu 18.04 DNS conflict
    vm.RemoteCommand(
        'echo "DNSStubListener=no" | sudo tee -a /etc/systemd/resolved.conf'
    )
    vm.RemoteCommand('sudo mv /etc/resolv.conf /etc/resolv.conf.orig')
    vm.RemoteCommand(
        'sudo ln -s /run/systemd/resolve/resolv.conf /etc/resolv.conf'
    )
    vm.RemoteCommand('sudo service systemd-resolved restart')
  install_cmd = './install.sh -y'
  if not _TUNE_ON_STARTUP.value:
    install_cmd = 'CONFIG_systune=no ./install.sh -y -n'
  vm.RemoteCommand(
      'cd {dir} && sudo {install}'.format(dir=_WORKING_DIR, install=install_cmd)
  )


def _JoinCluster(server_vm: _VM, vm: _VM) -> None:
  """Joins a Redis Enterprise cluster."""
  logging.info('Joining redis enterprise cluster.')
  vm.RemoteCommand(
      'sudo /opt/redislabs/bin/rladmin cluster join '
      'nodes {server_vm_ip} '
      'username {username} '
      'password {password} '.format(
          server_vm_ip=server_vm.internal_ip,
          username=_USERNAME,
          password=FLAGS.run_uri,
      )
  )


def CreateCluster(vms: List[_VM]) -> None:
  """Creates a Redis Enterprise cluster on the VM."""
  logging.info('Creating redis enterprise cluster.')
  vms[0].RemoteCommand(
      'sudo /opt/redislabs/bin/rladmin cluster create '
      'license_file {license_file} '
      'name redis-cluster '
      'username {username} '
      'password {password} '.format(
          license_file=posixpath.join(_WORKING_DIR, _LICENSE.value),
          username=_USERNAME,
          password=FLAGS.run_uri,
      )
  )
  for vm in vms[1:]:
    _JoinCluster(vms[0], vm)


def OfflineCores(vms: List[_VM]) -> None:
  """Offlines specific cores."""

  def _Offline(vm):
    for cpu_id in _DISABLE_CPU_IDS.value or []:
      vm.RemoteCommand(
          'sudo bash -c "echo 0 > /sys/devices/system/cpu/cpu%s/online"'
          % cpu_id
      )

  background_tasks.RunThreaded(_Offline, vms)


def TuneProxy(vm: _VM, proxy_threads: Optional[int] = None) -> None:
  """Tunes the number of Redis proxies on the cluster."""
  proxy_threads = proxy_threads or _PROXY_THREADS.value
  vm.RemoteCommand(
      'sudo /opt/redislabs/bin/rladmin tune '
      'proxy all '
      f'max_threads {proxy_threads} '
      f'threads {proxy_threads} '
  )
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
          'cat /sys/devices/system/node/node%d/cpulist' % node
      )[0].strip()
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
              node_cpu_list=node_cpu_list,
          )
      )

  background_tasks.RunThreaded(_Pin, vms)


def GetDatabaseMemorySize(vm: _VM) -> int:
  """Gets the available memory (bytes) that can be used to provision databases."""
  output, _ = vm.RemoteCommand('sudo /opt/redislabs/bin/rladmin status')
  # See tests/data/redis_enterprise_cluster_output.txt
  node_output = output.splitlines()[2]
  provisional_ram = node_output.split()[7]
  size_gb = float(provisional_ram.split('/')[0].strip('GB')) * (
      _MEMORY_SIZE_PERCENTAGE.value
  )
  return int(size_gb * _ONE_GIGABYTE)


@dataclasses.dataclass(frozen=True)
class LoadRequest:
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
      '-n allkeys '
  )
  if request.cluster_mode:
    command += '--cluster-mode'
  return command


def _LoadDatabaseSingleVM(load_vm: _VM, request: LoadRequest) -> None:
  """Loads the DB from a single VM."""
  command = _BuildLoadCommand(request)
  logging.info('Loading database with %s', request)
  load_vm.RemoteCommand(command)


def LoadDatabases(
    redis_vms: List[_VM],
    load_vms: List[_VM],
    endpoints: List[Tuple[str, int]],
    shards: Optional[int] = None,
) -> None:
  """Loads the databases before performing tests."""
  vms = load_vms + redis_vms
  load_requests = []
  load_records_per_vm = _LOAD_RECORDS.value // len(vms)
  shards = shards or _SHARDS.value
  cluster_mode = shards > 1
  for endpoint, port in endpoints:
    for i, _ in enumerate(vms):
      load_requests.append((
          vms[i],
          LoadRequest(
              key_minimum=max(i * load_records_per_vm, 1),
              key_maximum=(i + 1) * load_records_per_vm,
              redis_port=port,
              cluster_mode=cluster_mode,
              server_ip=endpoint,
          ),
      ))

  background_tasks.RunThreaded(
      _LoadDatabaseSingleVM, [(arg, {}) for arg in load_requests]
  )


class HttpClient:
  """HTTP Client for interacting with Redis REST API."""

  def __init__(self, server_vms: List[_VM]):
    self.vms = server_vms
    self.api_base_url = f'https://{server_vms[0].ip_address}:9443'
    self.session = requests.Session()
    self.session.auth = (_USERNAME, FLAGS.run_uri)
    self.provisional_memory = 0

  def _LogCurlifiedCommand(self, response: requests.Response) -> None:
    """Logs the version of the request that can be run from curl."""
    request = response.request
    command = f'curl -v -k -u {_USERNAME}:{FLAGS.run_uri} -X {request.method} '
    if request.body:
      body = request.body.decode('UTF-8')
      command += f'-d "{body}" -H "Content-type: application/json" '
    command += f'{request.url}'
    logging.info('Making API call equivalent to this curl command: %s', command)

  def GetDatabases(self) -> Optional[_UidToJsonDict]:
    """Gets the database object(s) running in the cluster.

    Returns:
      A dictionary of database objects by uid, per
      https://docs.redis.com/latest/rs/references/rest-api/objects/bdb/.

    Raises:
      errors.Benchmarks.RunError: If the number of databases found does not
        match --enterprise_redis_db_count.
    """
    logging.info('Getting Redis Enterprise databases.')
    r = self.session.get(f'{self.api_base_url}/v1/bdbs', verify=False)
    self._LogCurlifiedCommand(r)
    results = r.json()
    return {result['uid']: result for result in results}

  def GetEndpoints(self) -> List[Tuple[str, int]]:
    """Returns a list of (ip, port) tuples."""
    endpoints = []
    for db in self.GetDatabases().values():
      host = db['endpoints'][0]['addr'][0]
      port = db['endpoints'][0]['port']
      endpoints.append((host, port))
    logging.info('Database endpoints: %s', endpoints)
    return endpoints

  def GetDatabase(self, uid: int) -> Optional[_Json]:
    """Returns the database JSON object corresponding to uid."""
    logging.info('Getting Redis Enterprise database (uid: %s).', uid)
    all_databases = self.GetDatabases()
    return all_databases.get(uid, None)

  @vm_util.Retry(
      poll_interval=5,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def WaitForDatabaseUp(self, uid: int) -> None:
    """Waits for the Redis Enterprise database to become active."""
    db = self.GetDatabase(uid)
    if not db:
      raise errors.Benchmarks.RunError(
          'Database %s does not exist, expected to be waiting for startup.'
      )
    if db['status'] != 'active':
      raise errors.Resource.RetryableCreationError()

  def CreateDatabase(self, shards: Optional[int] = None) -> _Json:
    """Creates a new Redis Enterprise database.

    See https://docs.redis.com/latest/rs/references/rest-api/objects/bdb/.

    Args:
      shards: Number of shards for the database. In a clustered setup, shards
        will be distributed evenly across nodes.

    Returns:
      Returns the JSON object corresponding to the database that was created.
    """
    db_shards = shards or _SHARDS.value
    if not self.provisional_memory:
      self.provisional_memory = GetDatabaseMemorySize(self.vms[0])
    per_db_memory_size = int(
        self.provisional_memory * len(self.vms) / _NUM_DATABASES.value
    )
    content = {
        'name': 'redisdb',
        'type': 'redis',
        'memory_size': per_db_memory_size,
        'proxy_policy': 'all-master-shards',
        'sharding': False,
        'authentication_redis_pass': FLAGS.run_uri,
        'replication': _REPLICATION.value,
    }
    if db_shards > 1:
      content.update({
          'sharding': True,
          'shards_count': db_shards,
          'shards_placement': 'sparse',
          'oss_cluster': True,
          'shard_key_regex': [
              {'regex': '.*\\{(?<tag>.*)\\}.*'},
              {'regex': '(?<tag>.*)'}],
      })  # pyformat: disable

    logging.info('Creating Redis Enterprise database with %s.', content)
    r = self.session.post(
        f'{self.api_base_url}/v1/bdbs', json=content, verify=False
    )
    self._LogCurlifiedCommand(r)
    if r.status_code != 200:
      raise errors.Benchmarks.RunError(
          f'Unable to create database: status code: {r.status_code}, '
          f'reason {r.reason}.'
      )
    self.WaitForDatabaseUp(r.json()['uid'])
    logging.info('Finished creating Redis Enterprise database %s.', r.json())
    return r.json()

  def CreateDatabases(self, shards: Optional[int] = None) -> None:
    """Creates all databases with the specified number of shards."""
    for _ in range(_NUM_DATABASES.value):
      self.CreateDatabase(shards)

  @vm_util.Retry(
      poll_interval=5,
      retryable_exceptions=(errors.Resource.RetryableDeletionError,),
  )
  def DeleteDatabase(self, uid: int) -> None:
    """Deletes the database from the cluster."""
    logging.info('Deleting Redis Enterprise database (uid: %s).', uid)
    r = self.session.delete(f'{self.api_base_url}/v1/bdbs/{uid}', verify=False)
    self._LogCurlifiedCommand(r)
    if self.GetDatabase(uid) is not None:
      logging.info('Waiting for DB to finish deleting.')
      raise errors.Resource.RetryableDeletionError()

  def DeleteDatabases(self) -> None:
    """Deletes all databases."""
    for uid in self.GetDatabases().keys():
      self.DeleteDatabase(uid)


def _BuildRunCommand(
    host: str, threads: int, port: int, shards: Optional[int] = None
) -> str:
  """Spawns a memtier_benchmark on the load_vm against the redis_vm:port.

  Args:
    host: The target IP of the memtier_benchmark
    threads: The number of threads to run in this memtier_benchmark process.
    port: The port to target on the redis_vm.
    shards: The number of shards per database.

  Returns:
    Command to issue to the redis server.
  """
  if threads == 0:
    return None

  result = (
      'sudo /opt/redislabs/bin/memtier_benchmark '
      f'-s {host} '
      f'-a {FLAGS.run_uri} '
      f'-p {str(port)} '
      f'-t {str(threads)} '
      '--ratio 1:1 '
      f'--pipeline {str(_PIPELINES.value)} '
      f'-c {str(_LOADGEN_CLIENTS.value)} '
      f'-d {str(_DATA_SIZE.value)} '
      '--test-time 30 '
      '--key-minimum 1 '
      f'--key-maximum {str(_LOAD_RECORDS.value)} '
  )
  shards = shards or _SHARDS.value
  if shards > 1:
    result += '--cluster-mode '
  return result


@dataclasses.dataclass(frozen=True)
class Result:
  """Individual throughput and latency result."""

  throughput: int
  latency_usec: int
  metadata: Dict[str, Any]

  def ToSample(self, metadata: Dict[str, Any]) -> sample.Sample:
    """Returns throughput sample attached with the given metadata."""
    self.metadata.update(metadata)
    return sample.Sample('throughput', self.throughput, 'ops/s', self.metadata)


def ParseResults(output: str) -> List[Result]:
  """Parses the result from the cluster statistics API."""
  output_json = json.loads(output)
  results = []
  for interval in output_json.get('intervals'):
    results.append(
        Result(interval.get('total_req'), interval.get('avg_latency'), interval)
    )
  return results


def Run(
    redis_vms: List[_VM],
    load_vms: List[_VM],
    shards: Optional[int] = None,
    proxy_threads: Optional[int] = None,
    memtier_threads: Optional[int] = None,
) -> _ThroughputSampleTuple:
  """Run memtier against enterprise redis and measure latency and throughput.

  This function runs memtier against the redis server vm with increasing memtier
  threads until one of following conditions is reached:
    - FLAGS.enterprise_redis_max_threads is reached
    - FLAGS.enterprise_redis_latency_threshold is reached

  Args:
    redis_vms: Redis server vms.
    load_vms: Memtier load vms.
    shards: The per-DB shard count for this run.
    proxy_threads: The per-VM proxy thread count for this run.
    memtier_threads: If provided, overrides --enterprise_redis_min_threads.

  Returns:
    A tuple of (max_throughput_under_1ms, list of sample.Sample objects).
  """
  # TODO(liubrandon): Break up this function as it's getting rather long.
  results = []
  cur_max_latency = 0.0
  latency_threshold = _LATENCY_THRESHOLD.value
  shards = shards or _SHARDS.value
  threads = memtier_threads or _MIN_THREADS.value
  max_threads = _MAX_THREADS.value
  max_throughput_for_completion_latency_under_1ms = 0.0
  client = HttpClient(redis_vms)
  endpoints = client.GetEndpoints()
  redis_vm = redis_vms[0]

  # Validate before running
  if len(set(endpoints)) < _NUM_DATABASES.value:
    raise errors.Benchmarks.RunError(
        f'Wrong number of unique endpoints {endpoints}, '
        f'expected {_NUM_DATABASES.value}.'
    )
  if threads > max_threads:
    raise errors.Benchmarks.RunError(
        'min threads %s higher than max threads %s, '
        'raise --enterprise_redis_max_threads'
    )

  while cur_max_latency < latency_threshold and threads <= max_threads:
    # Set up run commands
    run_cmds = [
        _BuildRunCommand(endpoint, threads, port, shards)
        for endpoint, port in endpoints
    ]
    args = [(arg, {}) for arg in itertools.product(load_vms, run_cmds)]
    # 30 sec for throughput to stabilize and 10 sec of data.
    measurement_command = (
        'sleep 25 && curl -v -k -u {user}:{password} '
        'https://localhost:9443/v1/cluster/stats?interval=1sec > ~/output'
        .format(
            user=_USERNAME,
            password=FLAGS.run_uri,
        )
    )
    args += [((redis_vm, measurement_command), {})]
    # Run
    background_tasks.RunThreaded(
        lambda vm, command: vm.RemoteCommand(command), args
    )
    stdout, _ = redis_vm.RemoteCommand('cat ~/output')

    # Parse results and iterate
    metadata = GetMetadata(shards, threads, proxy_threads)
    run_results = ParseResults(stdout)
    for result in run_results:
      results.append(result.ToSample(metadata))
      latency = result.latency_usec
      cur_max_latency = max(cur_max_latency, latency)
      if latency < 1000:
        max_throughput_for_completion_latency_under_1ms = max(
            max_throughput_for_completion_latency_under_1ms, result.throughput
        )

      logging.info(
          'Threads : %d  (%f ops/sec, %f ms latency) < %f ms latency',
          threads,
          result.throughput,
          latency,
          latency_threshold,
      )

    threads += _THREAD_INCREMENT.value

  if cur_max_latency >= 1000:
    results.append(
        sample.Sample(
            'max_throughput_for_completion_latency_under_1ms',
            max_throughput_for_completion_latency_under_1ms,
            'ops/s',
            metadata,
        )
    )

  logging.info(
      'Max throughput under 1ms: %s ops/sec.',
      max_throughput_for_completion_latency_under_1ms,
  )
  return max_throughput_for_completion_latency_under_1ms, results


def GetMetadata(
    shards: int, threads: int, proxy_threads: int
) -> Dict[str, Any]:
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
      'total_shard_count': shards * _NUM_DATABASES.value,
      'db_count': _NUM_DATABASES.value,
      'redis_proxy_threads': proxy_threads or _PROXY_THREADS.value,
      'redis_loadgen_clients': _LOADGEN_CLIENTS.value,
      'pin_workers': _PIN_WORKERS.value,
      'disable_cpus': _DISABLE_CPU_IDS.value,
      'redis_enterprise_version': _VERSION,
      'memtier_data_size': _DATA_SIZE.value,
      'memtier_key_maximum': _LOAD_RECORDS.value,
      'replication': _REPLICATION.value,
  }


class ThroughputOptimizer:
  """Class that searches for the shard/proxy_thread count for best throughput.

  Attributes:
    client: Client that interacts with the Redis API.
    server_vms: List of server VMs.
    client_vms: List of client VMs.
    results: Matrix that records the search space of shards and proxy threads.
    min_threads: Keeps track of the optimal thread count used in previous run.
  """

  def __init__(self, server_vms: List[_VM], client_vms: List[_VM]):
    self.server_vms: List[_VM] = server_vms
    self.client_vms: List[_VM] = client_vms
    self.min_threads: int = _MIN_THREADS.value
    self.client = HttpClient(server_vms)

    # Determines the search space for the optimization algorithm. We multiply
    # the size by 2 which should be a large enough search space.
    matrix_size = (
        max(
            server_vms[0].num_cpus,
            FLAGS.enterprise_redis_proxy_threads or 0,
            FLAGS.enterprise_redis_shard_count or 0,
        )
        * 2
    )
    self.results: _ThroughputSampleMatrix = [
        [() for i in range(matrix_size)] for i in range(matrix_size)
    ]

  def _CreateAndLoadDatabases(self, shards: int) -> None:
    """Creates and loads all the databases needed for the run."""
    self.client.CreateDatabases(shards)
    LoadDatabases(
        self.server_vms, self.client_vms, self.client.GetEndpoints(), shards
    )

  def _FullRun(
      self, shard_count: int, proxy_thread_count: int
  ) -> _ThroughputSampleTuple:
    """Recreates databases if needed, then runs the test."""
    logging.info(
        'Starting new run with %s shards, %s proxy threads',
        shard_count,
        proxy_thread_count,
    )
    server_vm = self.server_vms[0]

    # Recreate the DB if needed
    dbs = self.client.GetDatabases()
    if not dbs:
      self._CreateAndLoadDatabases(shard_count)
    elif shard_count != list(dbs.values())[0]['shards_count']:
      self.client.DeleteDatabases()
      self._CreateAndLoadDatabases(shard_count)
    TuneProxy(server_vm, proxy_thread_count)
    PinWorkers(self.server_vms, proxy_thread_count)

    # Optimize the number of threads and run the test
    results = Run(self.server_vms,
                  self.client_vms,
                  shard_count,
                  proxy_thread_count,
                  self.min_threads)  # pyformat: disable
    self.min_threads = max(
        self.min_threads,
        int(results[1][-1].metadata['threads'] * _THREAD_OPTIMIZATION_RATIO),
    )
    return results

  def _GetResult(
      self, shards: int, proxy_threads: int
  ) -> _ThroughputSampleTuple:
    if not self.results[shards - 1][proxy_threads - 1]:
      self.results[shards - 1][proxy_threads - 1] = self._FullRun(
          shards, proxy_threads
      )
    return self.results[shards - 1][proxy_threads - 1]

  def _GetNeighborsToCheck(
      self, shards: int, proxy_threads: int
  ) -> List[Tuple[int, int]]:
    """Returns the shards/proxy_threads neighbor to check."""
    vary_proxy_threads = [
        (shards, proxy_threads - 1),
        (shards, proxy_threads + 1),
    ]
    vary_shards = [(shards - 1, proxy_threads), (shards + 1, proxy_threads)]
    return vary_proxy_threads + vary_shards

  def _GetOptimalNeighbor(
      self, shards: int, proxy_threads: int
  ) -> Tuple[int, int]:
    """Returns the shards/proxy_threads neighbor with the best throughput."""
    optimal_shards = shards
    optimal_proxy_threads = proxy_threads
    optimal_throughput, _ = self._GetResult(shards, proxy_threads)

    for shards_count, proxy_threads_count in self._GetNeighborsToCheck(
        shards, proxy_threads
    ):
      if (
          shards_count < 1
          or shards_count > len(self.results)
          or proxy_threads_count < 1
          or proxy_threads_count > len(self.results)
      ):
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
      logging.info(
          'Checking shards: %s, proxy_threads: %s',
          shard_count,
          proxy_thread_count,
      )
      optimal_shards, optimal_proxies = self._GetOptimalNeighbor(
          shard_count, proxy_thread_count
      )
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
        _PROXY_THREADS.value and not _SHARDS.value
    ):
      return self.DoLinearSearch()
    return self.DoGraphSearch()
