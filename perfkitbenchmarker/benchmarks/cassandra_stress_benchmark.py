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
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import cassandra


NUM_KEYS_PER_CORE = 2000000

# cassandra-stress command
WRITE_COMMAND = 'write'
COUNTER_WRITE_COMMAND = 'counter_write'
USER_COMMAND = 'user'
READ_COMMAND = 'read'
COUNTER_READ_COMMAND = 'counter_read'
MIXED_COMMAND = 'mixed'
PRELOAD_REQUIRED = (READ_COMMAND, COUNTER_READ_COMMAND, MIXED_COMMAND)

# cassandra-stress command [options]
flags.DEFINE_enum('cassandra_stress_command', WRITE_COMMAND,
                  [WRITE_COMMAND,
                   COUNTER_WRITE_COMMAND,
                   USER_COMMAND,
                   READ_COMMAND,
                   COUNTER_READ_COMMAND,
                   MIXED_COMMAND],
                  'cassandra-stress command to use.')

flags.DEFINE_integer('cassandra_stress_preload_num_keys', 0,
                     'Number of keys to preload into cassandra database. '
                     'By default, read/counter_read/mixed mode require '
                     'preload cassandra database. The number of the keys '
                     'pre-loaded will be the same as --num_keys.')

# Options for cassandra-stress
flags.DEFINE_integer('num_keys', 0,
                     'Number of keys used in cassandra-stress tool across '
                     'all loader vms. If unset, this benchmark will use '
                     '%s * num_cpus on data nodes as the value.'
                     % NUM_KEYS_PER_CORE)

flags.DEFINE_integer('num_cassandra_stress_threads', 50,
                     'Number of threads used in cassandra-stress tool '
                     'on each loader node.')

flags.DEFINE_integer('cassandra_stress_replication_factor', 3,
                     'Number of replicas.')

flags.DEFINE_enum('cassandra_stress_consistency_level', 'QUORUM',
                  ['ONE', 'QUORUM', 'LOCAL_ONE', 'LOCAL_QUORUM',
                   'EACH_QUORUM', 'ALL', 'ANY'],
                  'Set the consistency level to use during cassandra-stress.')

flags.DEFINE_integer('cassandra_stress_retries', 1000,
                     'Number of retries when error encountered during stress.')
# Use ./cassandra-stress -help pop to get more details.
# [dist=DIST(?)]: Seeds are selected from this distribution
#  EXP(min..max):
#      An exponential distribution over the range [min..max]
#  EXTREME(min..max,shape):
#      An extreme value (Weibull) distribution over the range [min..max]
#  QEXTREME(min..max,shape,quantas):
#      An extreme value, split into quantas, within which the chance of
#      selection is uniform
#  GAUSSIAN(min..max,stdvrng):
#      A gaussian/normal distribution, where mean=(min+max)/2, and stdev
#      is (mean-min)/stdvrng
#  GAUSSIAN(min..max,mean,stdev):
#      A gaussian/normal distribution, with explicitly defined mean and stdev
#  UNIFORM(min..max):
#      A uniform distribution over the range [min, max]
#  FIXED(val):
#      A fixed distribution, always returning the same value
#  Preceding the name with ~ will invert the distribution,
#  e.g. ~exp(1..10) will yield 10 most.
flags.DEFINE_enum('cassandra_stress_pop_distribution', None,
                  ['EXP', 'EXTREME', 'QEXTREME', 'GAUSSIAN', 'UNIFORM', 'FIXED',
                   '~EXP', '~EXTREME', '~QEXTREME', '~GAUSSIAN', '~UNIFORM',
                   '~FIXED'],
                  'The population distribution cassandra-stress uses. '
                  'By default, use no distribution.')

flags.DEFINE_integer('cassandra_stress_pop_size', None,
                     'The range of the distribution across all clients. '
                     'By default, the size of the pop is the same as '
                     '--num_keys.')

flags.DEFINE_list('cassandra_stress_pop_parameters', None,
                  'Additional parameters to use with. '
                  'This benchmark will calculate min, max for each '
                  'distribution. Some distributions need more parameters. '
                  'Commma-seperated list.')

# Options to use with cassandra-stress mixed mode, below flags only matter if
# --cassandra_stress_command=mixed.
flags.DEFINE_string('cassandra_stress_mixed_ratio', 'write=1,read=1',
                    'Read/write ratio of cassandra-stress. Only valid if '
                    '--cassandra_stress_command=mix. By default, '
                    '50% read and 50% write.')

# Options to use with cassandra-stress user mode, below flags only matter if
# --cassandra_stress_command=user.
# http://www.datastax.com/dev/blog/improved-cassandra-2-1-stress-tool-benchmark-any-schema
flags.DEFINE_string('cassandra_stress_profile', '',
                    'Path to cassandra-stress profile file. '
                    'Only valid if --cassandra_stress_command=user.')
flags.DEFINE_string('cassandra_stress_ops', 'insert=1',
                    'Specify what operations (inserts and/or queries) to '
                    'run and the number of each. '
                    'Only valid if --cassandra_stress_command=user.')

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

SLEEP_BETWEEN_CHECK_IN_SECONDS = 5
TEMP_PROFILE_PATH = posixpath.join(vm_util.VM_TMP_DIR, 'profile.yaml')

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
  if FLAGS.cassandra_stress_command == USER_COMMAND:
    data.ResourcePath(FLAGS.cassandra_stress_profile)


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
  benchmark_spec.metadata = {}
  if not FLAGS.num_keys:
    benchmark_spec.metadata['num_keys'] = (
        NUM_KEYS_PER_CORE * cassandra_vms[0].num_cpus)
    logging.info(
        'Num keys not set, using %s in cassandra-stress test.',
        benchmark_spec.metadata['num_keys'])
  else:
    benchmark_spec.metadata['num_keys'] = FLAGS.num_keys
  if (FLAGS.cassandra_stress_command in PRELOAD_REQUIRED and
      not FLAGS.cassandra_stress_preload_num_keys):
    benchmark_spec.metadata['num_preload_keys'] = benchmark_spec.metadata[
        'num_keys']
  else:
    benchmark_spec.metadata[
        'num_preload_keys'] = FLAGS.cassandra_stress_preload_num_keys
  # Preload database
  if benchmark_spec.metadata['num_preload_keys']:
    if (FLAGS.cassandra_stress_command == 'read' or
        FLAGS.cassandra_stress_command == 'mixed'):
      cassandra_stress_command = 'write'
    elif FLAGS.cassandra_stress_command == 'counter_read':
      cassandra_stress_command = 'counter_write'
    else:
      cassandra_stress_command = FLAGS.cassandra_stress_command
    RunCassandraStressTest(
        benchmark_spec,
        benchmark_spec.metadata['num_preload_keys'],
        cassandra_stress_command)

  if FLAGS.cassandra_stress_command == USER_COMMAND:
    for vm in vm_dict[CLIENT_GROUP]:
      vm.PushFile(FLAGS.cassandra_stress_profile,
                  TEMP_PROFILE_PATH)


def _ResultFilePath(vm):
  return posixpath.join(vm_util.VM_TMP_DIR,
                        vm.hostname + '.stress_results.txt')


def RunTestOnLoader(vm,
                    loader_index,
                    ops_per_vm,
                    data_node_ips,
                    cassandra_stress_command,
                    cassandra_stress_profile_ops,
                    cassandra_stress_pop_dist,
                    cassandra_stress_pop_params):
  """Run Cassandra-stress test on loader node.

  Args:
    vm: The target vm.
    loader_index: The index of target vm in loader vms.
    ops_per_vm: The number of ops per loader vm need to insert.
    data_node_ips: List of IP addresses for all data nodes.
    cassandra_stress_command: The cassandra-stress command to use.
    cassandra_stress_profile_ops: The ops to use with user mode.
    cassandra_stress_pop_dist: The population distribution.
    cassandra_stress_pop_params: A list of additional population parameters.
  """
  if cassandra_stress_command == USER_COMMAND:
    cassandra_stress_command += ' profile={profile} ops\({ops}\)'.format(
        profile=TEMP_PROFILE_PATH,
        ops=cassandra_stress_profile_ops)
    schema_option = ''
  else:
    if cassandra_stress_command == MIXED_COMMAND:
      cassandra_stress_command += ' ratio\({ratio}\)'.format(
          ratio=FLAGS.cassandra_stress_mixed_ratio)
    # TODO: Support more complex replication strategy.
    schema_option = '-schema replication\(factor={replication_factor}\)'.format(
        replication_factor=FLAGS.cassandra_stress_replication_factor)
  pop_range = '%s..%s' % (loader_index * ops_per_vm + 1,
                          (loader_index + 1) * ops_per_vm)
  cassandra_stress_pop_params.insert(0, pop_range)
  if cassandra_stress_pop_dist:
    pop_dist = 'dist=%s\(%s\)' % (
        cassandra_stress_pop_dist,
        ','.join(cassandra_stress_pop_params))
  else:
    pop_dist = 'seq=%s' % pop_range
  vm.RobustRemoteCommand(
      '{cassandra} {command} cl={consistency_level} n={num_keys} '
      '-node {nodes} {schema} -pop seq={pop_dist} '
      '-log file={result_file} -rate threads={threads} '
      '-errors retries={retries}'.format(
          cassandra=CASSANDRA_STRESS,
          command=cassandra_stress_command,
          consistency_level=FLAGS.cassandra_stress_consistency_level,
          num_keys=ops_per_vm,
          nodes=','.join(data_node_ips),
          schema=schema_option,
          pop_dist=pop_dist,
          result_file=_ResultFilePath(vm),
          retries=FLAGS.cassandra_stress_retries,
          threads=FLAGS.num_cassandra_stress_threads))


def RunCassandraStress(benchmark_spec,
                       num_ops,
                       cassandra_stress_command,
                       cassandra_stress_profile_ops,
                       cassandra_stress_pop_dist,
                       cassandra_stress_pop_params):
  """Start Cassandra test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
    num_ops: The number of operations cassandra-stress clients should issue.
    cassandra_stress_command: The cassandra-stress command to use.
    cassandra_stress_profile_ops: The ops to use with user mode.
    cassandra_stress_pop_dist: The population distribution.
    cassandra_stress_pop_params: A list of additional population parameters.
  """
  loader_vms = benchmark_spec.vm_groups[CLIENT_GROUP]
  num_loaders = len(loader_vms)
  cassandra_vms = benchmark_spec.vm_groups[CASSANDRA_GROUP]
  data_node_ips = [vm.internal_ip for vm in cassandra_vms]
  ops_per_vm = num_ops / num_loaders
  if num_ops % num_loaders:
    logging.warn(
        'Total number of opse rounded to %s (%s ops per loader vm).',
        ops_per_vm * num_loaders, ops_per_vm)
  logging.info('Executing the benchmark.')
  args = [((loader_vms[i], i, ops_per_vm, data_node_ips,
            cassandra_stress_command,
            cassandra_stress_profile_ops,
            cassandra_stress_pop_dist,
            cassandra_stress_pop_params), {})
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


def RunCassandraStressTest(benchmark_spec,
                           num_ops,
                           cassandra_stress_command,
                           cassandra_stress_profile_ops=None,
                           cassandra_stress_pop_dist=None,
                           cassandra_stress_pop_params=None):
  """Start all loader nodes as Cassandra clients and run stress test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
    num_keys: The number of operations cassandra-stress clients should issue.
    cassandra_stress_command: The cassandra-stress command to use.
    cassandra_stress_profile_ops: The ops to use with user mode.
    cassandra_stress_pop_dist: The population distribution.
    cassandra_stress_pop_params: A list of additional population parameters.
  """
  try:
    RunCassandraStress(benchmark_spec, num_ops,
                       cassandra_stress_command,
                       cassandra_stress_profile_ops,
                       cassandra_stress_pop_dist,
                       cassandra_stress_pop_params)
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

  benchmark_spec.metadata.update({
      'num_data_nodes': len(vm_dict[CASSANDRA_GROUP]),
      'num_loader_nodes': len(loader_vms),
      'num_cassandra_stress_threads': FLAGS.num_cassandra_stress_threads,
      'command': FLAGS.cassandra_stress_command,
      'consistency_level': FLAGS.cassandra_stress_consistency_level,
      'retries': FLAGS.cassandra_stress_retries})

  if FLAGS.cassandra_stress_command == USER_COMMAND:
    benchmark_spec.metadata.update({
        'profile': FLAGS.cassandra_stress_profile,
        'ops': FLAGS.cassandra_stress_ops})
  else:
    if FLAGS.cassandra_stress_command == MIXED_COMMAND:
      benchmark_spec.metadata[
          'mixed_ratio'] = FLAGS.cassandra_stress_mixed_ratio

    benchmark_spec.metadata[
        'replication_factor'] = FLAGS.cassandra_stress_replication_factor

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
    results.append(sample.Sample(metric, value, unit, benchmark_spec.metadata))
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
  if FLAGS.cassandra_stress_pop_distribution:
    benchmark_spec.metadata[
        'pop_dist'] = FLAGS.cassandra_stress_pop_distribution
    if FLAGS.cassandra_stress_pop_size:
      benchmark_spec.metadata[
          'pop_size'] = FLAGS.cassandra_stress_pop_size
    else:
      benchmark_spec.metadata[
          'pop_size'] = benchmark_spec.metadata['num_keys']
    if FLAGS.cassandra_stress_pop_parameters:
      benchmark_spec.metadata[
          'pop_parameters'] = FLAGS.cassandra_stress_pop_parameters
  RunCassandraStressTest(
      benchmark_spec,
      benchmark_spec.metadata['pop_size'],
      FLAGS.cassandra_stress_command, '',
      benchmark_spec.metadata['pop_dist'],
      benchmark_spec.metadata['pop_parameters'])
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
