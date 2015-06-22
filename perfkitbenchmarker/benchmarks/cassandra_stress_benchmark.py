# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs cassandra.

Cassandra homepage: http://cassandra.apache.org
cassandra-stress tool page:
http://www.datastax.com/documentation/cassandra/2.0/cassandra/tools/toolsCStress_t.html
"""

import functools
import logging
import math
import os
import posixpath
import re
import time


from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import cassandra


NUM_KEYS_PER_CORE = 2000000

flags.DEFINE_integer('num_keys', 0,
                     'Number of keys used in cassandra-stress tool. '
                     'If unset, this benchmark will use %s * num_cpus '
                     'on data nodes as the value.' % NUM_KEYS_PER_CORE)

flags.DEFINE_integer('num_cassandra_stress_threads', 50,
                     'Number of threads used in cassandra-stress tool '
                     'on each loader node.')

FLAGS = flags.FLAGS

DEFAULT_CLUSTER_SIZE = 4

# Disks and machines are set in config file.
BENCHMARK_INFO = {'name': 'cassandra_stress',
                  'description': 'Benchmark Cassandra using cassandra-stress',
                  'scratch_disk': False,
                  'num_machines': DEFAULT_CLUSTER_SIZE}

LOADER_NODE = 'loader'
DATA_NODE = 'cas'
PROPAGATION_WAIT_TIME = 30
SLEEP_BETWEEN_CHECK_IN_SECONDS = 5

# Stress test options.
CONSISTENCY_LEVEL = 'quorum'
REPLICATION_FACTOR = 3
RETRIES = 1000

CASSANDRA_STRESS = posixpath.join(cassandra.CASSANDRA_DIR, 'tools', 'bin',
                                  'cassandra-stress')


def GetInfo():
  return BENCHMARK_INFO


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  cassandra.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Install Cassandra and Java on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_dict
  logging.info('VM dictionary %s', vm_dict)

  if vm_dict['default']:
    logging.info('No config file is provided, use default settings: '
                 '1 loader node, 3 data nodes')
    vm_dict[LOADER_NODE] = [vm_dict['default'][-1]]
    vm_dict[DATA_NODE] = vm_dict['default'][:3]
    mount_point = os.path.join(vm_util.GetTempDir(), 'cassandra_data')
    disk_spec = disk.BaseDiskSpec(
        FLAGS.scratch_disk_size,
        FLAGS.scratch_disk_type,
        mount_point)
    for vm in vm_dict[DATA_NODE]:
      vm.CreateScratchDisk(disk_spec)

  logging.info('Authorizing loader[0] permission to access all other vms.')
  vm_dict[LOADER_NODE][0].AuthenticateVm()

  logging.info('Preparing data files and Java on all vms.')
  vm_util.RunThreaded(lambda vm: vm.Install('cassandra'), benchmark_spec.vms)
  seed_vm = vm_dict[DATA_NODE][0]
  configure = functools.partial(cassandra.Configure, seed_vms=[seed_vm])
  vm_util.RunThreaded(configure, vm_dict[DATA_NODE])
  cassandra.StartCluster(seed_vm, vm_dict[DATA_NODE][1:])


def _ResultFilePath(vm):
  return posixpath.join(vm_util.VM_TMP_DIR,
                        vm.hostname + '.stress_results.txt')


def RunTestOnLoader(vm, data_node_ips):
  """Run Cassandra-stress test on loader node.

  Args:
    vm: The target vm.
    data_node_ips: List of IP addresses for all data nodes.
  """
  vm.RobustRemoteCommand(
      '%s '
      '--file "%s" --nodes %s '
      '--replication-factor %s --consistency-level %s '
      '--num-keys %s -K %s -t %s' % (
          CASSANDRA_STRESS,
          _ResultFilePath(vm),
          ','.join(data_node_ips),
          REPLICATION_FACTOR, CONSISTENCY_LEVEL, FLAGS.num_keys,
          RETRIES, FLAGS.num_cassandra_stress_threads))


def RunCassandraStress(benchmark_spec):
  """Start Cassandra test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  logging.info('Creating Keyspace.')
  data_node_ips = [data_vm.internal_ip
                   for data_vm in benchmark_spec.vm_dict[DATA_NODE]]
  loader_vms = benchmark_spec.vm_dict[LOADER_NODE]
  loader_vms[0].RemoteCommand(
      '%s '
      '--nodes %s --replication-factor %s '
      '--consistency-level %s --num-keys 1 > /dev/null' % (
          CASSANDRA_STRESS,
          ','.join(data_node_ips),
          REPLICATION_FACTOR, CONSISTENCY_LEVEL))
  logging.info('Waiting %s for keyspace to propagate.', PROPAGATION_WAIT_TIME)
  time.sleep(PROPAGATION_WAIT_TIME)

  if not FLAGS.num_keys:
    FLAGS.num_keys = NUM_KEYS_PER_CORE * benchmark_spec.vm_dict[
        DATA_NODE][0].num_cpus
    logging.info('Num keys not set, using %s in cassandra-stress test.',
                 FLAGS.num_keys)
  logging.info('Executing the benchmark.')
  args = [((loader_vm, data_node_ips), {})
          for loader_vm in benchmark_spec.vm_dict[LOADER_NODE]]
  vm_util.RunThreaded(RunTestOnLoader, args)


def WaitForLoaderToFinish(vm):
  """Watch loader node and wait for it to finish test.

  Args:
    vm: The target vm.
  """
  result_path = _ResultFilePath(vm)
  while True:
    resp, _ = vm.RemoteCommand('tail -n 1 ' + result_path)
    if re.findall(r'END', resp):
      break
    if re.findall(r'FAILURE', resp):
      vm.PullFile(vm_util.GetTempDir(), result_path)
      raise errors.Benchmarks.RunError(
          'cassandra-stress tool failed, check %s for details.'
          % posixpath.join(vm_util.GetTempDir(),
                           os.path.basename(result_path)))
    time.sleep(SLEEP_BETWEEN_CHECK_IN_SECONDS)


def CollectResultFile(vm, interval_op_rate_list, interval_key_rate_list,
                      latency_median_list, latency_95th_list,
                      latency_99_9th_list,
                      total_operation_time_list):
  """Collect result file on vm.

  Args:
    vm: The target vm.
    interval_op_rate_list: The list stores interval_op_rate.
    interval_key_rate_list: The list stores interval_key_rate.
    latency_median_list: The list stores latency median.
    latency_95th_list: The list stores latency 95th percentile.
    latency_99_9th_list: The list stores latency 99.9th percentile.
    total_operation_time_list: The list stores total operation time.
  """
  result_path = _ResultFilePath(vm)
  vm.PullFile(vm_util.GetTempDir(), result_path)
  resp, _ = vm.RemoteCommand('tail ' + result_path)
  match = re.findall(r'[\w\t ]: +([\d\.:]+)', resp)
  if len(match) < 6:
    raise ValueError('Result not found in "%s"' % resp)
  interval_op_rate_list.append(int(match[0]))
  interval_key_rate_list.append(int(match[1]))
  latency_median_list.append(float(match[2]))
  latency_95th_list.append(float(match[3]))
  latency_99_9th_list.append(float(match[4]))
  raw_time_data = match[5].split(':')
  total_operation_time_list.append(
      int(raw_time_data[0]) * 3600 + int(raw_time_data[1]) * 60 + int(
          raw_time_data[2]))


def RunCassandraStressTest(benchmark_spec):
  """Start all loader nodes as Cassandra clients and run stress test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  try:
    RunCassandraStress(benchmark_spec)
  finally:
    logging.info('Tests running. Watching progress.')
    vm_util.RunThreaded(WaitForLoaderToFinish,
                        benchmark_spec.vm_dict[LOADER_NODE])


def CollectResults(benchmark_spec):
  """Collect and parse test results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('Gathering results.')
  vm_dict = benchmark_spec.vm_dict
  interval_op_rate_list = []
  interval_key_rate_list = []
  latency_median_list = []
  latency_95th_list = []
  latency_99_9th_list = []
  total_operation_time_list = []
  args = [((vm, interval_op_rate_list, interval_key_rate_list,
            latency_median_list, latency_95th_list,
            latency_99_9th_list,
            total_operation_time_list), {}) for vm in vm_dict[LOADER_NODE]]
  vm_util.RunThreaded(CollectResultFile, args)
  results = []
  metadata = {'num_keys': FLAGS.num_keys,
              'num_data_nodes': len(vm_dict[DATA_NODE]),
              'num_loader_nodes': len(vm_dict[LOADER_NODE]),
              'num_cassandra_stress_threads':
              FLAGS.num_cassandra_stress_threads}
  results = [
      sample.Sample('Interval_op_rate', math.fsum(interval_op_rate_list),
                    'operations per second', metadata),
      sample.Sample('Interval_key_rate', math.fsum(interval_key_rate_list),
                    'operations per second', metadata),
      sample.Sample('Latency median',
                    math.fsum(latency_median_list) / len(vm_dict[LOADER_NODE]),
                    'ms', metadata),
      sample.Sample('Latency 95th percentile',
                    math.fsum(latency_95th_list) / len(vm_dict[LOADER_NODE]),
                    'ms', metadata),
      sample.Sample('Latency 99.9th percentile',
                    math.fsum(latency_99_9th_list) / len(vm_dict[LOADER_NODE]),
                    'ms', metadata),
      sample.Sample('Total operation time',
                    math.fsum(total_operation_time_list) / len(
                        vm_dict[LOADER_NODE]), 'seconds', metadata)]
  logging.info('Cassandra results:\n%s', results)
  return results


def Run(benchmark_spec):
  """Run Cassandra on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  RunCassandraStressTest(benchmark_spec)
  return CollectResults(benchmark_spec)


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_dict

  vm_util.RunThreaded(cassandra.Stop, vm_dict[DATA_NODE])
  vm_util.RunThreaded(cassandra.CleanNode, vm_dict[DATA_NODE])
