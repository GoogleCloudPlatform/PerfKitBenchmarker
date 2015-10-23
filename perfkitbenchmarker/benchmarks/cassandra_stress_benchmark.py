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
http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html
"""

import collections
import functools
import logging
import math
import os
import posixpath
import re
import time


from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
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

BENCHMARK_NAME = 'cassandra_stress'
BENCHMARK_CONFIG = """
cassandra_stress:
  description: Benchmark Cassandra using cassandra-stress
  vm_groups:
    cassandra_nodes:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: 3
    stress_client:
      vm_spec: *default_single_core
"""

CASSANDRA_GROUP = 'cassandra_nodes'
CLIENT_GROUP = 'stress_client'

PROPAGATION_WAIT_TIME = 30
SLEEP_BETWEEN_CHECK_IN_SECONDS = 5


# Stress test options.
CONSISTENCY_LEVEL = 'QUORUM'
REPLICATION_FACTOR = 3
RETRIES = 1000

CASSANDRA_STRESS = posixpath.join(cassandra.CASSANDRA_DIR, 'tools', 'bin',
                                  'cassandra-stress')

# Results documentation:
# http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStressOutput_c.html
RESULTS_METRICS = (
    'op rate',  # Number of operations per second performed during the run.
    'partition rate',  # Number of partition operations per second performed
                       # during the run.
    'row rate',  # Number of row operations per second performed during the run.
    'latency mean',  # Average latency in milliseconds for each operation during
                     # that run.
    'latency median',  # Median latency in milliseconds for each operation
                       # during that run.
    'latency 95th percentile',  # 95% of the time the latency was less than
                                # the number displayed in the column.
    'latency 99th percentile',  # 99% of the time the latency was less than
                                # the number displayed in the column.
    'latency 99.9th percentile',  # 99.9% of the time the latency was less than
                                  # the number displayed in the column.
    'latency max',  # Maximum latency in milliseconds.
    'Total partitions',  # Number of partitions.
    'Total errors',  # Number of errors.
    'Total operation time')  # Total operation time.

# Metrics are aggregated between client vms.
AGGREGATED_METRICS = {'op rate', 'partition rate', 'row rate',
                      'Total partitions', 'Total errors'}
# Maximum value will be choisen between client vms.
MAXIMUM_METRICS = {'latency max'}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


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
  vm_dict = benchmark_spec.vm_groups
  cassandra_vms = vm_dict[CASSANDRA_GROUP]
  logging.info('VM dictionary %s', vm_dict)

  logging.info('Authorizing loader[0] permission to access all other vms.')
  vm_dict[CLIENT_GROUP][0].AuthenticateVm()

  logging.info('Preparing data files and Java on all vms.')
  vm_util.RunThreaded(lambda vm: vm.Install('cassandra'), benchmark_spec.vms)
  seed_vm = cassandra_vms[0]
  configure = functools.partial(cassandra.Configure, seed_vms=[seed_vm])
  vm_util.RunThreaded(configure, cassandra_vms)
  cassandra.StartCluster(seed_vm, cassandra_vms[1:])


def _ResultFilePath(vm):
  return posixpath.join(vm_util.VM_TMP_DIR,
                        vm.hostname + '.stress_results.txt')


def RunTestOnLoader(vm, loader_index, keys_per_vm, data_node_ips):
  """Run Cassandra-stress test on loader node.

  Args:
    vm: The target vm.
    loader_index: The index of target vm in loader vms.
    keys_per_vm: The number of keys per loader vm need to insert.
    data_node_ips: List of IP addresses for all data nodes.
  """
  vm.RobustRemoteCommand(
      '{0} write cl={1} n={2} '
      '-node {3} -schema replication\(factor={4}\) -pop seq={5}..{6} '
      '-log file={7} -rate threads={8} -errors retries={9}'.format(
          CASSANDRA_STRESS, CONSISTENCY_LEVEL, keys_per_vm,
          ','.join(data_node_ips), REPLICATION_FACTOR,
          loader_index * keys_per_vm + 1, (loader_index + 1) * keys_per_vm,
          _ResultFilePath(vm), RETRIES, FLAGS.num_cassandra_stress_threads))


def RunCassandraStress(benchmark_spec):
  """Start Cassandra test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  logging.info('Creating Keyspace.')
  loader_vms = benchmark_spec.vm_groups[CLIENT_GROUP]
  cassandra_vms = benchmark_spec.vm_groups[CASSANDRA_GROUP]
  data_node_ips = [vm.internal_ip for vm in cassandra_vms]

  loader_vms[0].RemoteCommand(
      '{0} write n=1 cl={1} '
      '-node {2} -schema replication\(factor={3}\) > /dev/null'.format(
          CASSANDRA_STRESS, CONSISTENCY_LEVEL,
          ','.join(data_node_ips), REPLICATION_FACTOR))
  logging.info('Waiting %s for keyspace to propagate.', PROPAGATION_WAIT_TIME)
  time.sleep(PROPAGATION_WAIT_TIME)

  if not FLAGS.num_keys:
    FLAGS.num_keys = NUM_KEYS_PER_CORE * cassandra_vms[0].num_cpus
    logging.info('Num keys not set, using %s in cassandra-stress test.',
                 FLAGS.num_keys)
  logging.info('Executing the benchmark.')
  num_loaders = len(loader_vms)
  keys_per_vm = FLAGS.num_keys / num_loaders
  if FLAGS.num_keys % num_loaders:
    logging.warn('Total number of keys rounded to %s (%s keys per loader vm).',
                 keys_per_vm * num_loaders, keys_per_vm)
  args = [((loader_vms[i], i, keys_per_vm, data_node_ips), {})
          for i in xrange(0, num_loaders)]
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


def CollectResultFile(vm, results):
  """Collect result file on vm.

  Args:
    vm: The target vm.
    results: A dictionary of lists. Each list contains results of a field
       defined in RESULTS_METRICS collected from each loader machines.
  """
  result_path = _ResultFilePath(vm)
  vm.PullFile(vm_util.GetTempDir(), result_path)
  resp, _ = vm.RemoteCommand('tail -n 20 ' + result_path)
  for metric in RESULTS_METRICS:
    value = regex_util.ExtractGroup(r'%s[\t ]+: ([\d\.:]+)' % metric, resp)
    if metric == RESULTS_METRICS[-1]:  # Total operation time
      value = value.split(':')
      results[metric].append(
          int(value[0]) * 3600 + int(value[1]) * 60 + int(value[2]))
    else:
      results[metric].append(float(value))


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
                        benchmark_spec.vm_groups[CLIENT_GROUP])


def CollectResults(benchmark_spec):
  """Collect and parse test results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('Gathering results.')
  vm_dict = benchmark_spec.vm_groups
  loader_vms = vm_dict[CLIENT_GROUP]
  raw_results = collections.defaultdict(list)
  args = [((vm, raw_results), {}) for vm in loader_vms]
  vm_util.RunThreaded(CollectResultFile, args)

  metadata = {'num_keys': FLAGS.num_keys,
              'num_data_nodes': len(vm_dict[CASSANDRA_GROUP]),
              'num_loader_nodes': len(loader_vms),
              'num_cassandra_stress_threads':
              FLAGS.num_cassandra_stress_threads}
  results = []
  for metric in RESULTS_METRICS:
    if metric in MAXIMUM_METRICS:
      value = max(raw_results[metric])
    else:
      value = math.fsum(raw_results[metric])
      if metric not in AGGREGATED_METRICS:
        value = value / len(loader_vms)
    if metric.startswith('latency'):
      unit = 'ms'
    elif metric.endswith('rate'):
      unit = 'operations per second'
    elif metric == 'Total operation time':
      unit = 'seconds'
    results.append(sample.Sample(metric, value, unit, metadata))
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
  vm_dict = benchmark_spec.vm_groups
  cassandra_vms = vm_dict[CASSANDRA_GROUP]

  vm_util.RunThreaded(cassandra.Stop, cassandra_vms)
  vm_util.RunThreaded(cassandra.CleanNode, cassandra_vms)
