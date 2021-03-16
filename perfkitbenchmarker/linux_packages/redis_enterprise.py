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


"""Module containing redis enterprise installation and cleanup functions."""

import json
import logging
import posixpath
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
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

_PACKAGE_NAME = 'redis_enterprise'
_LICENSE = 'enterprise_redis_license'
_WORKING_DIR = '~/redislabs'
_RHEL_TAR = 'redislabs-5.4.2-24-rhel7-x86_64.tar'
_XENIAL_TAR = 'redislabs-5.4.2-24-xenial-amd64.tar'
_BIONIC_TAR = 'redislabs-5.4.2-24-bionic-amd64.tar'
_USERNAME = 'user@google.com'
PREPROVISIONED_DATA = {
    _RHEL_TAR:
        '8db83074b3e4e6de9c249ce34b6bb899ed158a6a4801f36c530e79bdb97a4c20',
    _XENIAL_TAR:
        'ef2da8b5eaa02b53488570392df258c0d5d3890a9085c2495aeb5c96f336e639',
    _BIONIC_TAR:
        'ef0c58d6d11683aac07d3f2cae6b9544cb53064c9f7a7419d63b6d14cd858d53',
    _LICENSE:
        'ae7eeae0aebffebdfd3dec7a8429513034b0d6dc5bf745abd00feb2d51f79dc5',
}


def _GetTarName():
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


def Install(vm):
  """Installs Redis Enterprise package on the VM."""
  vm.InstallPackages('wget')
  vm.InstallPreprovisionedPackageData(_PACKAGE_NAME,
                                      [_GetTarName(), _LICENSE],
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


def CreateCluster(vm):
  """Create an Redis Enterprise cluster on the vm."""
  vm.RemoteCommand(
      'sudo /opt/redislabs/bin/rladmin cluster create '
      'license_file {license_file} '
      'name redis-cluster '
      'username {username} '
      'password {password} '.format(
          license_file=posixpath.join(_WORKING_DIR, _LICENSE),
          username=_USERNAME,
          password=FLAGS.run_uri))


def OfflineCores(vm):
  """Offline specific cores."""
  for cpu_id in _DISABLE_CPU_IDS.value or []:
    vm.RemoteCommand('sudo bash -c '
                     '"echo 0 > /sys/devices/system/cpu/cpu%s/online"' % cpu_id)


def TuneProxy(vm):
  """Tune the number of Redis proxies on the server vm."""
  vm.RemoteCommand(
      'sudo /opt/redislabs/bin/rladmin tune '
      'proxy all '
      'max_threads {proxy_threads} '
      'threads {proxy_threads} '.format(
          proxy_threads=str(_PROXY_THREADS.value)))
  vm.RemoteCommand('sudo /opt/redislabs/bin/dmc_ctl restart')


def PinWorkers(vm):
  """Splits the Redis worker threads across the NUMA nodes evenly.

  This function is no-op if --enterprise_redis_pin_workers is not set.

  Args:
    vm: The VM with the Redis workers to pin.
  """
  if not _PIN_WORKERS.value:
    return

  numa_nodes = vm.CheckLsCpu().numa_node_count
  proxies_per_node = _PROXY_THREADS.value // numa_nodes
  for node in range(numa_nodes):
    node_cpu_list = vm.RemoteCommand(
        'cat /sys/devices/system/node/node%d/cpulist' % node)[0].strip()
    # List the PIDs of the Redis worker processes and pin a sliding window of
    # `proxies_per_node` workers to the NUMA nodes in increasing order.
    vm.RemoteCommand(r'sudo /opt/redislabs/bin/dmc-cli -ts root list | '
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


def SetUpCluster(vm, redis_port):
  """Set up the details of the cluster."""
  content = {
      'name': 'redisdb',
      'memory_size': 10000000000,
      'type': 'redis',
      'proxy_policy': 'all-master-shards',
      'port': redis_port,
      'sharding': False,
      'authentication_redis_pass': FLAGS.run_uri,
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

  vm.RemoteCommand(
      "curl -v -k -u {username}:{password} https://localhost:9443/v1/bdbs "
      "-H 'Content-type: application/json' -d '{content}'".format(
          username=_USERNAME,
          password=FLAGS.run_uri,
          content=json.dumps(content)))


@vm_util.Retry()
def WaitForClusterUp(vm, redis_port):
  """Waits for the Redis Enterprise cluster to respond to commands."""
  stdout, _ = vm.RemoteCommand(
      '/opt/redislabs/bin/redis-cli '
      '-h localhost '
      '-p {port} '
      '-a {password} '
      'ping'.format(
          password=FLAGS.run_uri,
          port=redis_port))
  if stdout.find('PONG') == -1:
    raise errors.Resource.RetryableCreationError()


def LoadCluster(vm, redis_port):
  """Load the cluster before performing tests."""
  vm.RemoteCommand(
      '/opt/redislabs/bin/memtier_benchmark '
      '-s localhost '
      '-a {password} '
      '-p {port} '
      '-t 1 '  # Set -t and -c to 1 to avoid duplicated work in writing the same
      '-c 1 '  # key/value pairs repeatedly.
      '--ratio 1:0 '
      '--pipeline 100 '
      '-d 100 '
      '--key-pattern S:S '
      '--key-minimum 1 '
      '--key-maximum {load_records} '
      '-n allkeys '
      '--cluster-mode '.format(
          password=FLAGS.run_uri,
          port=str(redis_port),
          load_records=str(_LOAD_RECORDS.value)))


def BuildRunCommand(redis_vm, threads, port):
  """Spawn a memtir_benchmark on the load_vm against the redis_vm:port.

  Args:
    redis_vm: The target of the memtier_benchmark
    threads: The number of threads to run in this memtier_benchmark process.
    port: the port to target on the redis_vm.

  Returns:
    Command to issue to the redis server.
  """
  if threads == 0:
    return None

  return ('/opt/redislabs/bin/memtier_benchmark '
          '-s {ip_address} '
          '-a {password} '
          '-p {port} '
          '-t {threads} '
          '--ratio 1:1 '
          '--pipeline {pipeline} '
          '-c {clients} '
          '-d 100 '
          '--key-minimum 1 '
          '--key-maximum {key_maximum} '
          '-n {run_records} '
          '--cluster-mode '.format(
              ip_address=redis_vm.internal_ip,
              password=FLAGS.run_uri,
              port=str(port),
              threads=str(threads),
              pipeline=str(_PIPELINES.value),
              clients=str(_LOADGEN_CLIENTS.value),
              key_maximum=str(_LOAD_RECORDS.value),
              run_records=str(_RUN_RECORDS.value)))


def Run(redis_vm, load_vms, redis_port):
  """Run memtier against enterprise redis and measure latency and throughput.

  This function runs memtier against the redis server vm with increasing memtier
  threads until one of following conditions is reached:
    - FLAGS.enterprise_redis_max_threads is reached
    - FLAGS.enterprise_redis_latency_threshold is reached

  Args:
    redis_vm: Redis server vm.
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

  while (cur_max_latency < latency_threshold and threads <= max_threads):
    load_command = BuildRunCommand(redis_vm, threads, redis_port)
    # 1min for throughput to stabilize and 10sec of data.
    measurement_command = (
        'sleep 60 && curl -v -k -u {user}:{password} '
        'https://localhost:9443/v1/bdbs/stats?interval=1sec > ~/output'.format(
            user=_USERNAME,
            password=FLAGS.run_uri,))
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
      sample_metadata['redis_proxy_threads'] = (
          _PROXY_THREADS.value)
      sample_metadata['redis_loadgen_clients'] = (
          _LOADGEN_CLIENTS.value)
      sample_metadata['pin_workers'] = _PIN_WORKERS.value
      sample_metadata['disable_cpus'] = _DISABLE_CPU_IDS.value
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
