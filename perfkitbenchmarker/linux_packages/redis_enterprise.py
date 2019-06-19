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
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_boolean('enterprise_redis_tune_on_startup', True,
                     'Whether to tune core config during startup.')
flags.DEFINE_integer('enterprise_redis_proxy_threads', 24,
                     'Number of redis proxy threads to use.')
flags.DEFINE_integer('enterprise_redis_shard_count', 6,
                     'Number of redis shard. Each shard is a redis thread.')
flags.DEFINE_integer('enterprise_redis_load_records', 1000000,
                     'Number of keys to pre-load into Redis.')
flags.DEFINE_integer('enterprise_redis_run_records', 1000000,
                     'Number of requests per loadgen client to send to the '
                     'Redis server.')
flags.DEFINE_integer('enterprise_redis_pipeline', 9,
                     'Number of pipelines to use.')
flags.DEFINE_integer('enterprise_redis_loadgen_clients', 24,
                     'Number of clients per loadgen vm.')
flags.DEFINE_integer('enterprise_redis_max_threads', 40,
                     'Maximum number of memtier threads to use.')
flags.DEFINE_integer('enterprise_redis_min_threads', 18,
                     'Minimum number of memtier threads to use.')
flags.DEFINE_integer('enterprise_redis_thread_increment', 1,
                     'Number of memtier threads to increment by.')
flags.DEFINE_integer('enterprise_redis_latency_threshold', 1100,
                     'The latency threshold in microseconds '
                     'until the test stops.')
flags.DEFINE_boolean('enterprise_redis_pin_workers', False,
                     'Whether to pin the proxy threads after startup.')

_PACKAGE_NAME = 'redis_enterprise'
_LICENSE = 'enterprise_redis_license'
_WORKING_DIR = '~/redislabs'
_RHEL_TAR = 'redislabs-5.4.2-24-rhel7-x86_64.tar'
_XENIAL_TAR = 'redislabs-5.4.2-24-xenial-amd64.tar'
_BIONIC_TAR = 'redislabs-5.4.2-24-bionic-amd64.tar'
_USERNAME = 'user@google.com'
PREPROVISIONED_DATA = {
    _RHEL_TAR: '2c7b6847e8383d81d9e17e63ffed1ce1',
    _XENIAL_TAR: 'b5338c5c3a3e432ef1c47a3b3bc8174b',
    _BIONIC_TAR: 'e827d4811270d4a5a63497b0333b5ca7',
    _LICENSE: '8dde2a8af3fe8cc8a1a01d43f89eb622',
}


def _GetTarName():
  if FLAGS.os_type in [os_types.RHEL, os_types.AMAZONLINUX2, os_types.CENTOS7]:
    return _RHEL_TAR
  if FLAGS.os_type in [os_types.UBUNTU1604, os_types.DEBIAN, os_types.DEBIAN9]:
    return _XENIAL_TAR
  if FLAGS.os_type == os_types.UBUNTU1804:
    return _BIONIC_TAR


def _Install(vm):
  """Installs redis enterprise on the VM."""
  vm.InstallPackages('wget')
  vm.InstallPreprovisionedPackageData(_PACKAGE_NAME,
                                      [_GetTarName(), _LICENSE],
                                      _WORKING_DIR)
  vm.RemoteCommand('cd {dir} && sudo tar xvf {tar}'.format(
      dir=_WORKING_DIR, tar=_GetTarName()))

  if FLAGS.os_type == os_types.UBUNTU1804:
    vm.RemoteCommand(
        'echo "DNSStubListener=no" | sudo tee -a /etc/systemd/resolved.conf')
    vm.RemoteCommand('sudo mv /etc/resolv.conf /etc/resolv.conf.orig')
    vm.RemoteCommand(
        'sudo ln -s /run/systemd/resolve/resolv.conf /etc/resolv.conf')
    vm.RemoteCommand('sudo service systemd-resolved restart')
  tune_param = ('' if FLAGS.enterprise_redis_tune_on_startup
                else 'CONFIG_systune=no')
  vm.RemoteCommand('cd {dir} && sudo {tune} ./install.sh -y'.format(
      dir=_WORKING_DIR, tune=tune_param))


def YumInstall(vm):
  """Installs the redis package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the redis package on the VM."""
  _Install(vm)


def CreateCluster(vm):
  """Create an enterprise redis cluster on the vm."""
  command = [
      'sudo /opt/redislabs/bin/rladmin cluster create',
      'license_file', posixpath.join(_WORKING_DIR, _LICENSE),
      'name', 'redis-cluster',
      'username', _USERNAME,
      'password', FLAGS.run_uri,
  ]
  vm.RemoteCommand(' '.join(command))


def TuneProxy(vm):
  """Tune the number of redis proxies on the server vm."""
  command = [
      'sudo /opt/redislabs/bin/rladmin tune',
      'proxy all',
      'max_threads', str(FLAGS.enterprise_redis_proxy_threads),
      'threads', str(FLAGS.enterprise_redis_proxy_threads),
  ]
  vm.RemoteCommand(' '.join(command))
  vm.RemoteCommand('sudo /opt/redislabs/bin/dmc_ctl restart')


def PinWorkers(vm):
  """Splits the Redis worker threads across the NUMA nodes evenly.

  This function is no-op if --enterprise_redis_pin_workers is not set.

  Args:
    vm: The VM with the Redis workers to pin.
  """
  if not FLAGS.enterprise_redis_pin_workers:
    return

  # Grab the number of NUMA nodes by counting the number of node0, node1, node2,
  # etc. directories in sysfs.
  numa_nodes = int(
      vm.RemoteCommand('ls /sys/devices/system/node | '
                       'grep -E "node[0-9]+" | '
                       'wc -l')[0].strip())
  proxies_per_node = FLAGS.enterprise_redis_proxy_threads / numa_nodes
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
      'sharding': False
  }
  if FLAGS.enterprise_redis_shard_count > 1:
    content.update({
        'sharding': True,
        'shards_count': FLAGS.enterprise_redis_shard_count,
        'shards_placement': 'sparse',
        'oss_cluster': True,
        'shard_key_regex':
            [{'regex': '.*\\{(?<tag>.*)\\}.*'}, {'regex': '(?<tag>.*)'}]
    })

  cmd = (
      """curl -v -k -u {username}:{password} """
      """https://localhost:9443/v1/bdbs """
      """-H "Content-type: application/json" -d '{content}'""".format(
          username=_USERNAME,
          password=FLAGS.run_uri,
          content=json.dumps(content)))
  vm.RemoteCommand(cmd)


@vm_util.Retry()
def WaitForClusterUp(vm, redis_port):
  stdout, _ = vm.RemoteCommand(
      '/opt/redislabs/bin/redis-cli -h localhost -p {port} ping'.format(
          port=redis_port))
  if stdout.find('PONG') == -1:
    raise errors.Resource.RetryableCreationError()


def LoadCluster(vm, redis_port):
  """Load the cluster before performing tests."""
  load_command = [
      '/opt/redislabs/bin/memtier_benchmark',
      '-s localhost',
      '-p', str(redis_port),
      '-t 6',
      '--ratio 1:0',
      '--pipeline 100',
      '-c 128',
      '-d 100',
      '--key-pattern S:S',
      '--key-minimum 1',
      '--key-maximum', str(FLAGS.enterprise_redis_load_records),
      '-n allkeys',
      '--cluster-mode',
  ]
  vm.RemoteCommand(' '.join(load_command))


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

  run_command = [
      '/opt/redislabs/bin/memtier_benchmark',
      '-s', redis_vm.internal_ip,
      '-p', str(port),
      '-t', str(threads),
      '--ratio 1:1',
      '--pipeline', str(FLAGS.enterprise_redis_pipeline),
      '-c', str(FLAGS.enterprise_redis_loadgen_clients),
      '-d 100',
      '--key-minimum 1',
      '--key-maximum', str(FLAGS.enterprise_redis_load_records),
      '-n', str(FLAGS.enterprise_redis_run_records),
      '--cluster-mode',
  ]
  return ' '.join(run_command)


def RunCommand(vm, command):
  vm.RemoteCommand(command)


def Run(redis_vm, load_vms, redis_port):
  """Run memtier against enterprise redis and measure latency and throughput.

  Args:
    redis_vm: Redis server vm.
    load_vms: Memtier load vms.
    redis_port: Port for the redis server.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  cur_max_latency = 0.0
  latency_threshold = FLAGS.enterprise_redis_latency_threshold
  threads = FLAGS.enterprise_redis_min_threads
  max_throughput_for_completion_latency_under_1ms = 0.0

  while (cur_max_latency < latency_threshold
         and threads <= FLAGS.enterprise_redis_max_threads):
    load_command = BuildRunCommand(redis_vm, threads, redis_port)
    # 1min for throughput to stabilize and 10sec of data.
    measurement_command = (
        'sleep 60 && curl -v -k -u {user}:{password} '
        'https://localhost:9443/v1/bdbs/stats?interval=1sec > ~/output'.format(
            user=_USERNAME,
            password=FLAGS.run_uri,))
    args = [((load_vm, load_command), {}) for load_vm in load_vms]
    args += [((redis_vm, measurement_command), {})]
    vm_util.RunThreaded(RunCommand, args)
    stdout, _ = redis_vm.RemoteCommand('cat ~/output')
    output = json.loads(stdout)[0]
    intervals = output.get('intervals')

    for interval in intervals:
      throughput = interval.get('total_req')
      latency = interval.get('avg_latency')
      cur_max_latency = max(cur_max_latency, latency)
      sample_metadata = interval
      sample_metadata['redis_tune_on_startup'] = (
          FLAGS.enterprise_redis_tune_on_startup)
      sample_metadata['redis_pipeline'] = (
          FLAGS.enterprise_redis_pipeline)
      sample_metadata['threads'] = threads
      sample_metadata['shard_count'] = FLAGS.enterprise_redis_shard_count
      sample_metadata['redis_proxy_threads'] = (
          FLAGS.enterprise_redis_proxy_threads)
      sample_metadata['redis_loadgen_clients'] = (
          FLAGS.enterprise_redis_loadgen_clients)
      sample_metadata['pin_workers'] = FLAGS.enterprise_redis_pin_workers
      results.append(sample.Sample('throughput', throughput, 'ops/s',
                                   sample_metadata))
      if latency < 1000:
        max_throughput_for_completion_latency_under_1ms = max(
            max_throughput_for_completion_latency_under_1ms, throughput)

      logging.info('Threads : %d  (%f, %f) < %f', threads, throughput, latency,
                   latency_threshold)

    threads += FLAGS.enterprise_redis_thread_increment

  if cur_max_latency >= 1000:
    results.append(sample.Sample(
        'max_throughput_for_completion_latency_under_1ms',
        max_throughput_for_completion_latency_under_1ms, 'ops/s',
        sample_metadata))

  return results
