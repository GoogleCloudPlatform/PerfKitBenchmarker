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

"""Runs cassandra.

Cassandra homepage: http://cassandra.apache.org
cassandra-stress tool page:
http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html
"""

import collections
import copy
import functools
import logging
import math
import posixpath
import time
from typing import Union

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cassandra


NUM_KEYS_PER_CORE = 2000000
PROPAGATION_WAIT_TIME = 30

# cassandra-stress command
WRITE_COMMAND = 'write'
COUNTER_WRITE_COMMAND = 'counter_write'
USER_COMMAND = 'user'

READ_COMMAND = 'read'
COUNTER_READ_COMMAND = 'counter_read'
MIXED_COMMAND = 'mixed'
PRELOAD_REQUIRED = (READ_COMMAND, COUNTER_READ_COMMAND, MIXED_COMMAND)

# cassandra-stress command [options]
flags.DEFINE_enum(
    'cassandra_stress_command',
    WRITE_COMMAND,
    [
        WRITE_COMMAND,
        COUNTER_WRITE_COMMAND,
        USER_COMMAND,
        READ_COMMAND,
        COUNTER_READ_COMMAND,
        MIXED_COMMAND,
    ],
    'cassandra-stress command to use.',
)

flags.DEFINE_integer(
    'cassandra_stress_preload_num_keys',
    None,
    'Number of keys to preload into cassandra database. '
    'Read/counter_read/mixed modes require preloading '
    'cassandra database. If not set, the number of the keys '
    'preloaded will be the same as --num_keys for '
    'read/counter_read/mixed mode, the same as the number of '
    'loaders for write/counter_write/user mode.',
)

# Options for cassandra-stress
flags.DEFINE_integer(
    'num_keys',
    0,
    'Number of keys used in cassandra-stress tool across '
    'all loader vms. If unset, this benchmark will use '
    '%s * NumCpusForBenchmark() on data nodes as the value.'
    % NUM_KEYS_PER_CORE,
)

flags.DEFINE_integer(
    'num_cassandra_stress_threads',
    150,
    'Number of threads used in cassandra-stress tool on each loader node.',
)

flags.DEFINE_integer(
    'cassandra_stress_replication_factor', 3, 'Number of replicas.'
)

flags.DEFINE_enum(
    'cassandra_stress_consistency_level',
    'QUORUM',
    ['ONE', 'QUORUM', 'LOCAL_ONE', 'LOCAL_QUORUM', 'EACH_QUORUM', 'ALL', 'ANY'],
    'Set the consistency level to use during cassandra-stress.',
)

flags.DEFINE_integer(
    'cassandra_stress_retries',
    1000,
    'Number of retries when error encountered during stress.',
)

CASSANDRA_SERVER_ZONES = flags.DEFINE_list(
    'cassandra_server_zones',
    [],
    'zones to launch the cassandra servers in. ',
)

CASSANDRA_CLIENT_ZONES = flags.DEFINE_list(
    'cassandra_client_zones',
    [],
    'zones to launch the clients for the benchmark in. ',
)
# Use "./cassandra-stress help -pop" to get more details.
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
#  Preceding the name with ~ will invert the distribution,
#  e.g. ~EXP(1..10) will yield 10 most, instead of least, often.
flags.DEFINE_enum(
    'cassandra_stress_population_distribution',
    None,
    [
        'EXP',
        'EXTREME',
        'QEXTREME',
        'GAUSSIAN',
        'UNIFORM',
        '~EXP',
        '~EXTREME',
        '~QEXTREME',
        '~GAUSSIAN',
        '~UNIFORM',
    ],
    'The population distribution cassandra-stress uses. '
    'By default, each loader vm is given a range of keys '
    '[min, max], and loaders will read/insert keys sequentially '
    'from min to max.',
)

flags.DEFINE_integer(
    'cassandra_stress_population_size',
    None,
    'The size of the population across all clients. '
    'By default, the size of the population equals to '
    'max(num_keys,cassandra_stress_preload_num_keys).',
)

flags.DEFINE_list(
    'cassandra_stress_population_parameters',
    [],
    'Additional parameters to use with distribution. '
    'This benchmark will calculate min, max for each '
    'distribution. Some distributions need more parameters. '
    'See: "./cassandra-stress help -pop" for more details. '
    'Comma-separated list.',
)

# Options to use with cassandra-stress mixed mode, below flags only matter if
# --cassandra_stress_command=mixed.
flags.DEFINE_string(
    'cassandra_stress_mixed_ratio',
    'write=1,read=1',
    'Read/write ratio of cassandra-stress. Only valid if '
    '--cassandra_stress_command=mixed. By default, '
    '50% read and 50% write.',
)

# Options to use with cassandra-stress user mode, below flags only matter if
# --cassandra_stress_command=user.
# http://www.datastax.com/dev/blog/improved-cassandra-2-1-stress-tool-benchmark-any-schema
flags.DEFINE_string(
    'cassandra_stress_profile',
    '',
    'Path to cassandra-stress profile file. '
    'Only valid if --cassandra_stress_command=user.',
)
flags.DEFINE_string(
    'cassandra_stress_operations',
    'insert=1',
    'Specify what operations (inserts and/or queries) to '
    'run and the ratio of each operation. '
    'Only valid if --cassandra_stress_command=user.',
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cassandra_stress'
BENCHMARK_CONFIG = """
cassandra_stress:
  description: Benchmark Cassandra using cassandra-stress
  vm_groups:
    workers:
      vm_spec:
        GCP:
          machine_type: c3-standard-4
          zone: us-central1-a
        Azure:
          machine_type: Standard_D4_v5
          zone: eastus2
        AWS:
          machine_type: m6i.xlarge
          zone: us-east-1
      disk_spec: *default_500_gb
    client:
      vm_spec:
        GCP:
          machine_type: c3-standard-4
          zone: us-central1-a
        Azure:
          machine_type: Standard_D4_v5
          zone: eastus2
        AWS:
          machine_type: m6i.xlarge
          zone: us-east-1
"""

CASSANDRA_GROUP = 'workers'
CLIENT_GROUP = 'client'

SLEEP_BETWEEN_CHECK_IN_SECONDS = 5
TEMP_PROFILE_PATH = posixpath.join(vm_util.VM_TMP_DIR, 'profile.yaml')

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
    'total partitions',  # Number of partitions.
    'total errors',  # Number of errors.
    'total operation time',
)  # Total operation time.

# Metrics are aggregated between client vms.
AGGREGATED_METRICS = frozenset({
    'op rate',
    'partition rate',
    'row rate',
    'Total partitions',
    'Total errors',
})
# Maximum value will be choisen between client vms.
MAXIMUM_METRICS = {'latency max'}


def GetConfig(user_config):
  """Customize the config for the benchmark based on flags."""
  cloud = FLAGS.cloud
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.client_vm_machine_type:
    vm_spec = config['vm_groups'][CLIENT_GROUP]['vm_spec']
    for cloud in vm_spec:
      config['vm_groups'][CLIENT_GROUP]['vm_spec'][cloud][
          'machine_type'
      ] = FLAGS.client_vm_machine_type
  if FLAGS.db_machine_type:
    vm_spec = config['vm_groups'][CASSANDRA_GROUP]['vm_spec']
    for cloud in vm_spec:
      config['vm_groups'][CASSANDRA_GROUP]['vm_spec'][cloud][
          'machine_type'
      ] = FLAGS.db_machine_type
  ConfigureVmGroups(
      config, CASSANDRA_SERVER_ZONES.value, CASSANDRA_GROUP, cloud
  )
  ConfigureVmGroups(config, CASSANDRA_CLIENT_ZONES.value, CLIENT_GROUP, cloud)
  return config


def ConfigureVmGroups(config, flag, group_name, cloud):
  for index, zone in enumerate(flag):
    if index == 0:
      config['vm_groups'][group_name]['vm_spec'][cloud]['zone'] = zone
      continue
    node = copy.deepcopy(config['vm_groups'][group_name])
    node['vm_spec'][cloud]['zone'] = zone
    config['vm_groups'][f'{group_name}_{index}'] = node


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if not CASSANDRA_SERVER_ZONES.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Specify zones for cassandra servers in cassandra_server_zones flag'
    )
  if not CASSANDRA_CLIENT_ZONES.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Specify zones for cassandra clients in cassandra_client_zones flag'
    )
  cassandra.CheckPrerequisites()
  if FLAGS.cassandra_stress_command == USER_COMMAND:
    data.ResourcePath(FLAGS.cassandra_stress_profile)


def CheckMetadata(metadata):
  """Verify that metadata is valid.

  Args:
    metadata: dict. Contains metadata for this benchmark.
  """
  if metadata['command'] in PRELOAD_REQUIRED:
    if metadata['population_size'] > metadata['num_preload_keys']:
      raise errors.Benchmarks.PrepareException(
          'For %s modes, number of preloaded keys must be larger than or '
          'equal to population size.',
          PRELOAD_REQUIRED,
      )


def GenerateMetadataFromFlags(benchmark_spec, cassandra_vms, client_vms):
  """Generate metadata from command-line flags.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
    cassandra_vms: cassandra server vms.
    client_vms: cassandra client vms.

  Returns:
    dict. Contains metadata for this benchmark.
  """
  vm_dict = benchmark_spec.vm_groups
  metadata = {}
  if not FLAGS.num_keys:
    metadata['num_keys'] = (
        NUM_KEYS_PER_CORE * vm_dict[CASSANDRA_GROUP][0].NumCpusForBenchmark()
    )
  else:
    metadata['num_keys'] = FLAGS.num_keys

  if FLAGS['cassandra_stress_preload_num_keys'].present:
    metadata['num_preload_keys'] = FLAGS.cassandra_stress_preload_num_keys
  elif FLAGS.cassandra_stress_command in PRELOAD_REQUIRED:
    metadata['num_preload_keys'] = metadata['num_keys']
  else:
    metadata['num_preload_keys'] = len(vm_dict[CLIENT_GROUP])

  metadata.update({
      'concurrent_reads': FLAGS.cassandra_concurrent_reads,
      'concurrent_writes': FLAGS.cassandra_concurrent_writes,
      'num_data_nodes': len(cassandra_vms),
      'num_loader_nodes': len(client_vms),
      'num_cassandra_stress_threads': FLAGS.num_cassandra_stress_threads,
      'command': FLAGS.cassandra_stress_command,
      'consistency_level': FLAGS.cassandra_stress_consistency_level,
      'retries': FLAGS.cassandra_stress_retries,
      'population_size': FLAGS.cassandra_stress_population_size or max(
          metadata['num_keys'], metadata['num_preload_keys']
      ),
      'population_dist': FLAGS.cassandra_stress_population_distribution,
      'population_parameters': ','.join(
          FLAGS.cassandra_stress_population_parameters
      ),
  })

  if FLAGS.cassandra_stress_command == USER_COMMAND:
    metadata.update({
        'profile': FLAGS.cassandra_stress_profile,
        'operations': FLAGS.cassandra_stress_operations,
    })
  else:
    if FLAGS.cassandra_stress_command == MIXED_COMMAND:
      metadata['mixed_ratio'] = FLAGS.cassandra_stress_mixed_ratio
    metadata['replication_factor'] = FLAGS.cassandra_stress_replication_factor
  logging.info('Metadata: %s', metadata)
  return metadata


def PreloadCassandraServer(cassandra_vms, client_vms, metadata):
  """Preload cassandra cluster if necessary.

  Args:
    cassandra_vms: cassandra server vms.
    client_vms: client vms that run cassandra-stress.
    metadata: dict. Contains metadata for this benchmark.
  """
  if (
      FLAGS.cassandra_stress_command == 'read'
      or FLAGS.cassandra_stress_command == 'mixed'
  ):
    cassandra_stress_command = 'write'
  elif FLAGS.cassandra_stress_command == 'counter_read':
    cassandra_stress_command = 'counter_write'
  else:
    cassandra_stress_command = FLAGS.cassandra_stress_command
  logging.info(
      'Preloading cassandra database with %s %s operations.',
      metadata['num_preload_keys'],
      cassandra_stress_command,
  )
  RunCassandraStressTest(
      cassandra_vms,
      client_vms,
      metadata['num_preload_keys'],
      cassandra_stress_command,
  )
  logging.info('Waiting %s for keyspace to propagate.', PROPAGATION_WAIT_TIME)
  time.sleep(PROPAGATION_WAIT_TIME)


def ParseVmGroups(vm_dict):
  cassandra_vms = []
  client_vms = []
  for key, value in vm_dict.items():
    if key.startswith(CASSANDRA_GROUP):
      cassandra_vms.append(value[0])
    elif key.startswith(CLIENT_GROUP):
      client_vms.append(value[0])
  return cassandra_vms, client_vms


def Prepare(benchmark_spec):
  """Install Cassandra and Java on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_groups
  cassandra_vms, client_vms = ParseVmGroups(vm_dict)
  logging.info('VM dictionary %s', vm_dict)

  logging.info('Authorizing loader[0] permission to access all other vms.')
  client_vms[0].AuthenticateVm()

  logging.info('Preparing data files and Java on all vms.')
  background_tasks.RunThreaded(
      lambda vm: vm.Install('cassandra'), cassandra_vms
  )

  background_tasks.RunThreaded(
      lambda vm: vm.Install('cassandra_stress'), client_vms
  )
  seed_vm = cassandra_vms[0]
  configure = functools.partial(cassandra.Configure, seed_vms=[seed_vm])
  background_tasks.RunThreaded(configure, cassandra_vms)
  cassandra.StartCluster(seed_vm, cassandra_vms[1:])
  if FLAGS.cassandra_stress_command == USER_COMMAND:
    for vm in client_vms:
      vm.PushFile(FLAGS.cassandra_stress_profile, TEMP_PROFILE_PATH)
  metadata = GenerateMetadataFromFlags(
      benchmark_spec, cassandra_vms, client_vms
  )
  if metadata['num_preload_keys']:
    CheckMetadata(metadata)
  cassandra.CreateKeyspace(
      seed_vm, replication_factor=FLAGS.cassandra_replication_factor
  )
  PreloadCassandraServer(cassandra_vms, client_vms, metadata)


def _ResultFilePath(vm):
  return posixpath.join(vm_util.VM_TMP_DIR, vm.hostname + '.stress_results.txt')


def RunTestOnLoader(
    vm,
    loader_index,
    operations_per_vm,
    data_node_ips,
    command,
    user_operations,
    population_per_vm,
    population_dist,
    population_params,
):
  """Run Cassandra-stress test on loader node.

  Args:
    vm: The target vm.
    loader_index: integer. The index of target vm in loader vms.
    operations_per_vm: integer. The number of operations each loader vm
      requests.
    data_node_ips: list. List of IP addresses for all data nodes.
    command: string. The cassandra-stress command to use.
    user_operations: string. The operations to use with user mode.
    population_per_vm: integer. Population per loader vm.
    population_dist: string. The population distribution.
    population_params: string. Representing additional population parameters.
  """
  if command == USER_COMMAND:
    command += r' profile={profile} ops\({ops}\)'.format(
        profile=TEMP_PROFILE_PATH, ops=user_operations
    )

    schema_option = ''
  else:
    if command == MIXED_COMMAND:
      command += r' ratio\({ratio}\)'.format(
          ratio=FLAGS.cassandra_stress_mixed_ratio
      )
    # TODO: Support more complex replication strategy.
    schema_option = (
        r'-schema replication\(factor={replication_factor}\)'.format(
            replication_factor=FLAGS.cassandra_stress_replication_factor
        )
    )
  population_range = '%s..%s' % (
      loader_index * population_per_vm + 1,
      (loader_index + 1) * population_per_vm,
  )
  if population_params:
    population_params = '%s,%s' % (population_range, population_params)
  else:
    population_params = population_range
  if population_dist:
    population_dist = r'-pop dist=%s\(%s\)' % (
        population_dist,
        population_params,
    )
  else:
    population_dist = '-pop seq=%s' % population_params
  vm.RobustRemoteCommand(
      'sudo {cassandra_stress_command} {command} cl={consistency_level}'
      ' n={num_keys} -node {nodes} {schema} {population_dist} -log'
      ' file={result_file} -rate threads={threads} -errors retries={retries}'
      .format(
          cassandra_stress_command=cassandra.GetCassandraStressPath(vm),
          command=command,
          consistency_level=FLAGS.cassandra_stress_consistency_level,
          num_keys=operations_per_vm,
          nodes=','.join(data_node_ips),
          schema=schema_option,
          population_dist=population_dist,
          result_file=_ResultFilePath(vm),
          retries=FLAGS.cassandra_stress_retries,
          threads=FLAGS.num_cassandra_stress_threads,
      )
  )


def RunCassandraStressTest(
    cassandra_vms,
    loader_vms,
    num_operations,
    command,
    profile_operations='insert=1',
    population_size=None,
    population_dist=None,
    population_params=None,
):
  """Start all loader nodes as Cassandra clients and run stress test.

  Args:
    cassandra_vms: list. A list of vm objects. Cassandra servers.
    load_vms: list. A list of vm objects. Cassandra clients.
    num_keys: integer. The number of operations cassandra-stress clients should
      issue.
    command: string. The cassandra-stress command to use.
    profile_operations: string. The operations to use with user mode.
    population_size: integer. The population size.
    population_dist: string. The population distribution.
    population_params: string. Representing additional population parameters.
  """
  num_loaders = len(loader_vms)
  data_node_ips = [vm.internal_ip for vm in cassandra_vms]
  population_size = population_size or num_operations
  operations_per_vm = int(math.ceil(float(num_operations) / num_loaders))
  population_per_vm = int(population_size / num_loaders)
  if num_operations % num_loaders:
    logging.warn(
        'Total number of operations rounded to %s '
        '(%s operations per loader vm).',
        operations_per_vm * num_loaders,
        operations_per_vm,
    )
  logging.info('Executing the benchmark.')
  args = [
      (
          (
              loader_vms[i],
              i,
              operations_per_vm,
              data_node_ips,
              command,
              profile_operations,
              population_per_vm,
              population_dist,
              population_params,
          ),
          {},
      )
      for i in range(0, num_loaders)
  ]
  background_tasks.RunThreaded(RunTestOnLoader, args)


def ParseResp(resp) -> dict[str, Union[float, int]]:
  """Parses response from Cassandra stress test.

  Args:
    resp: metric data to parse

  Returns:
    dict of all the metrics and their values
  """
  all_rows = resp.split('\n')
  metric_values = {}
  for row in all_rows:
    if row.strip() == 'Results:' or not row.strip():
      continue
    metric_details = _ParseResultRow(row)
    if metric_details:
      metric_name, metric_value = metric_details
      metric_values[metric_name] = metric_value
  return metric_values


def _ParseResultRow(row: str) -> tuple[str, float] | None:
  """Parses a single row of the result file."""
  try:
    metric_name = regex_util.ExtractGroup('(.*) :', row, 1).lower().strip()
  except regex_util.NoMatchError:
    logging.error('Metric name not found in row: %s', row)
    return None
  if metric_name not in RESULTS_METRICS:
    return None
  try:
    metric_data = regex_util.ExtractGroup(
        r'(.*) :\s*(\d{1,3}(,\d{3})*(\.\d+)*)', row, 2
    ).strip()
  except regex_util.NoMatchError:
    logging.error('Invalid value for %s: %s', metric_name, row)
    return None
  if metric_name == 'total operation time':
    operation_time_values = regex_util.ExtractGroup(
        r'(.*) :\s*(\d{2}:\d{2}:\d{2})', row, 2
    ).split(':')
    metric_value = (
        int(operation_time_values[0].strip()) * 3600
        + int(operation_time_values[1].strip()) * 60
        + int(operation_time_values[2].strip())
    )
  else:
    metric_value = float(metric_data.strip().replace(',', ''))
  return (metric_name, metric_value)


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
  metrics = ParseResp(resp)
  logging.info('Metrics: %s', metrics)
  for metric in RESULTS_METRICS:
    if metric not in metrics:
      raise ValueError(f'Metric {metric} not found in result file.')
    value = metrics[metric]
    results[metric].append(value)


def CollectResults(loader_vms, metadata):
  """Collect and parse test results.

  Args:
    loader_vms: client vms.
    metadata: dict. Contains metadata for this benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  logging.info('Gathering results.')
  raw_results = collections.defaultdict(list)
  args = [((vm, raw_results), {}) for vm in loader_vms]
  background_tasks.RunThreaded(CollectResultFile, args)
  results = []
  for metric in RESULTS_METRICS:
    if metric in MAXIMUM_METRICS:
      value = max(raw_results[metric])
    else:
      value = math.fsum(raw_results[metric])
      if metric not in AGGREGATED_METRICS:
        value = value / len(loader_vms)
    unit = ''
    if metric.startswith('latency'):
      unit = 'ms'
    elif metric.endswith('rate'):
      unit = 'operations per second'
    elif metric == 'Total operation time':
      unit = 'seconds'
    results.append(sample.Sample(metric, value, unit, metadata))
  return results


def Run(benchmark_spec):
  """Run Cassandra on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  cassandra_vms, client_vms = ParseVmGroups(benchmark_spec.vm_groups)
  metadata = GenerateMetadataFromFlags(
      benchmark_spec, cassandra_vms, client_vms
  )
  metadata['cassandra_version'] = cassandra.GetCassandraVersion(
      benchmark_spec.vm_groups[CASSANDRA_GROUP][0]
  )
  RunCassandraStressTest(
      cassandra_vms,
      client_vms,
      metadata['num_keys'],
      metadata['command'],
      metadata.get('operations'),
      metadata['population_size'],
      metadata['population_dist'],
      metadata['population_parameters'],
  )
  return CollectResults(client_vms, metadata)


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_groups
  cassandra_vms = vm_dict[CASSANDRA_GROUP]

  background_tasks.RunThreaded(cassandra.Stop, cassandra_vms)
  background_tasks.RunThreaded(cassandra.CleanNode, cassandra_vms)
