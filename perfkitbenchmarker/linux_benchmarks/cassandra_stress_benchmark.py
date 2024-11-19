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

"""Runs cassandra-stress on a cluster of Cassandra servers.

Cassandra is a distributed, open source, NoSQL database management system
Cassandra homepage: http://cassandra.apache.org
cassandra-stress tool page:
http://docs.datastax.com/en/cassandra/2.1/cassandra/tools/toolsCStress_t.html

We are using cassandra-stress to test the performance of the cassandra cluster
for different workloads at varied load. The benchmark is run in two phases:
1. Preload phase: The cassandra cluster is preloaded with a set of keys.
2. Run phase: The cassandra cluster is run with a specified workload. Workload
can be of type read only, write only or mixed read and write. The benchmark can
be configured to run the test multiple times by using --optimize_performance
flag. We update the load for each test by updating the number of threads to get
max op rate.
"""

import collections
import copy
import functools
import logging
import math
import posixpath
import re
import time
from typing import Union

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cassandra


NUM_KEYS_PER_CORE = 2000000
# Adding wait between prefill and the test workload to give some time
# for the data to propagate and for the cluster to stabilize.
PROPAGATION_WAIT_TIME = 720
WAIT_BETWEEN_COMPACTION_TASKS_CHECK = 720

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
CASSANDRA_STRESS_NUM_KEYS = flags.DEFINE_integer(
    'cassandra_stress_num_keys',
    0,
    'Number of keys used in cassandra-stress tool across all loader vms. If'
    ' unset, this benchmark will use %s * NumCpusForBenchmark() on data nodes'
    ' as the value. Ignored if --cassandra_stress_run_duration is set.'
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

CASSANDRA_STRESS_PRELOAD_THREADS = flags.DEFINE_integer(
    'cassandra_stress_preload_thead_count',
    300,
    'Number of threads to use for preloading.',
)
CASSANDRA_STRESS_RUN_DURATION = flags.DEFINE_string(
    'cassandra_stress_run_duration',
    None,
    'Duration of the cassandra-stress. Use m, s and h as units. Overrides'
    ' --num_keys.',
)
IS_ROW_CACHE_ENABLED = flags.DEFINE_bool(
    'is_row_cache_enabled',
    False,
    'Enable row cache for the cassandra server.',
)

ROW_CACHE_SIZE = flags.DEFINE_integer(
    'row_cache_size',
    1000,
    'Size of the row cache for cassandra in MiB if --is_row_cache_enabled is'
    ' true.',
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
CASSANDRA_CPU_UTILIZATION_LIMIT = flags.DEFINE_integer(
    'cassandra_cpu_utilization_limit',
    70,
    'Maximum cpu utilization percentage for the benchmark. ',
)
CASSANDRA_STRESS_MAX_RUNS = flags.DEFINE_integer(
    'cassandra_stress_max_runs',
    6,
    'Max Number of times to run cassandra-stress to optimize performance.',
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
# Maximum value will be chosen between client vms.
MAXIMUM_METRICS = {'latency max'}
THREAD_INCREMENT_COUNT = 50
MAX_MEDIAN_LATENCY_MS = 20
MAX_ACCEPTED_COMPACTION_TIME = 30
STARTING_THREAD_COUNT = 25
SAR_CPU_UTILIZATION_INTERVAL = 10


class CassandraCompactionNotCompletedError(Exception):
  """Exception for cassandra compaction not complete."""


def GetConfig(user_config):
  """Customize the config for the benchmark based on flags."""
  cloud = FLAGS.cloud
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.client_vm_machine_type:
    vm_spec = config['vm_groups'][CLIENT_GROUP]['vm_spec']
    for cloud_value in vm_spec:
      config['vm_groups'][CLIENT_GROUP]['vm_spec'][cloud_value][
          'machine_type'
      ] = FLAGS.client_vm_machine_type
  if FLAGS.db_machine_type:
    vm_spec = config['vm_groups'][CASSANDRA_GROUP]['vm_spec']
    for cloud_value in vm_spec:
      config['vm_groups'][CASSANDRA_GROUP]['vm_spec'][cloud_value][
          'machine_type'
      ] = FLAGS.db_machine_type
  if FLAGS.db_disk_type:
    disk_spec = config['vm_groups'][CASSANDRA_GROUP]['disk_spec']
    for cloud_value in disk_spec:
      config['vm_groups'][CASSANDRA_GROUP]['disk_spec'][cloud_value][
          'disk_type'
      ] = FLAGS.db_disk_type
  if FLAGS.db_disk_size:
    disk_spec = config['vm_groups'][CASSANDRA_GROUP]['disk_spec']
    for cloud_value in disk_spec:
      config['vm_groups'][CASSANDRA_GROUP]['disk_spec'][cloud_value][
          'disk_size'
      ] = FLAGS.db_disk_size
  if FLAGS.db_disk_iops:
    disk_spec = config['vm_groups'][CASSANDRA_GROUP]['disk_spec']
    for cloud_value in disk_spec:
      config['vm_groups'][CASSANDRA_GROUP]['disk_spec'][cloud_value][
          'provisioned_iops'
      ] = FLAGS.db_disk_iops
  if FLAGS.db_disk_throughput:
    disk_spec = config['vm_groups'][CASSANDRA_GROUP]['disk_spec']
    for cloud_value in disk_spec:
      config['vm_groups'][CASSANDRA_GROUP]['disk_spec'][cloud_value][
          'provisioned_throughput'
      ] = FLAGS.db_disk_throughput
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
  if not CASSANDRA_STRESS_NUM_KEYS.value:
    metadata['num_keys'] = (
        NUM_KEYS_PER_CORE * vm_dict[CASSANDRA_GROUP][0].NumCpusForBenchmark()
    )
  else:
    metadata['num_keys'] = CASSANDRA_STRESS_NUM_KEYS.value

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
      'num_cassandra_stress_preload_threads': (
          CASSANDRA_STRESS_PRELOAD_THREADS.value
      ),
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
      'is_row_cache_enabled': FLAGS.is_row_cache_enabled,
      'row_cache_size': FLAGS.row_cache_size,
      'duration': CASSANDRA_STRESS_RUN_DURATION.value,
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
      CASSANDRA_STRESS_PRELOAD_THREADS.value,
      is_preload=True,
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
  # needed to run sar
  background_tasks.RunThreaded(
      lambda vm: vm.InstallPackages('sysstat'), cassandra_vms + client_vms
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
  WaitForCompactionTasks(cassandra_vms)


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
    thread_count,
    is_preload,
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
    thread_count: integer. The number of threads to use for cassandra-stress.
    is_preload: boolean. Whether this is a preload run.
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
  duration = ''
  num_keys_parameter = 'n={num_keys}'.format(num_keys=operations_per_vm)
  # Duration specificies how long to run the test. If it is set, we don't need
  # to specify the number of keys.
  if not is_preload and CASSANDRA_STRESS_RUN_DURATION.value:
    duration = r'duration={duration}'.format(
        duration=CASSANDRA_STRESS_RUN_DURATION.value
    )
    num_keys_parameter = ''
  vm.RobustRemoteCommand(
      'sudo {cassandra_stress_command} {command} {duration}'
      ' cl={consistency_level} {num_keys_parameter} -node {nodes} {schema}'
      ' {population_dist} -log file={result_file} -rate threads={threads}'
      ' -errors retries={retries}'.format(
          cassandra_stress_command=cassandra.GetCassandraStressPath(vm),
          command=command,
          consistency_level=FLAGS.cassandra_stress_consistency_level,
          num_keys_parameter=num_keys_parameter,
          nodes=','.join(data_node_ips),
          schema=schema_option,
          population_dist=population_dist,
          result_file=_ResultFilePath(vm),
          retries=FLAGS.cassandra_stress_retries,
          threads=int(thread_count),
          duration=duration,
      )
  )


def RunCassandraStressTest(
    cassandra_vms,
    loader_vms,
    num_operations,
    command,
    thread_count,
    profile_operations='insert=1',
    population_size=None,
    population_dist=None,
    population_params=None,
    is_preload=False,
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
    thread_count: integer. The number of threads to use for cassandra-stress.
    is_preload: boolean. Whether this is a preload run.

  Returns:
    A list of cpu load for each cassandra node.
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
  tasks = []
  for i in range(0, len(loader_vms)):
    tasks.append((
        RunTestOnLoader,
        [
            loader_vms[i],
            i,
            operations_per_vm,
            data_node_ips,
            command,
            profile_operations,
            population_per_vm,
            population_dist,
            population_params,
            thread_count,
            is_preload,
        ],
        {},
    ))
  if not is_preload:
    for vm in cassandra_vms:
      tasks.append((CPUUtilizationReporting, [vm], {}))
  background_tasks.RunParallelThreads(tasks, max_concurrency=10)
  return background_tasks.RunThreaded(GetLoadAverage, cassandra_vms)


def CPUUtilizationReporting(vm):
  # command : sar -u <interval> <count>
  # we are collection the data every SAR_CPU_UTILIZATION_INTERVAL seconds for
  # the duration of the test
  vm.RobustRemoteCommand(
      f'sar -u {SAR_CPU_UTILIZATION_INTERVAL}'
      f' {CalculateNumberOfSarRequestsFromDuration(CASSANDRA_STRESS_RUN_DURATION.value, SAR_CPU_UTILIZATION_INTERVAL)}'
      f' > {GenerateCpuUtilizationFileName(vm)}'
  )


def ParseAverageCpuUtilization(output) -> float:
  """Parses the output of the sar command."""
  average_cpu_utilization = re.findall(
      r'^Average.*$', output, flags=re.MULTILINE
  )
  if not average_cpu_utilization:
    logging.error('No average cpu utilization found in sar output.')
    return 0
  per_process_cpu_utilization = re.sub(
      ' +', ' ', average_cpu_utilization[0]
  ).split(' ')
  return float(per_process_cpu_utilization[2].strip())


def CalculateNumberOfSarRequestsFromDuration(duration, freq):
  """Calculates the number of sar requests to be sent from the duration of the test."""
  if duration is None:
    # If duration is not set, we don't need to send sar requests.
    return 0
  duration = duration.replace('m', 'min')  # In units, m is meter
  quantity = units.ParseExpression(duration)
  seconds = quantity.m_as(units.second)
  return int(seconds / freq)


def GetLoadAverage(vm):
  stdout, _ = vm.RemoteCommand('uptime')
  load_last_5m = ParseUptimeOutput(stdout)
  return load_last_5m


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


def GenerateCpuUtilizationFileName(vm):
  return f'{vm_util.VM_TMP_DIR}/{vm.name}-cpu_utilization.log'


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
  return RunTestNTimes(client_vms, cassandra_vms, metadata)


def RunTestNTimes(client_vms, cassandra_vms, metadata):
  """Run the cassandra stress test max_allowed_runs times.

  Args:
    client_vms: client vms.
    cassandra_vms: cassandra server vms.
    metadata: dict. Contains metadata for this benchmark.

  Returns:
    A list of sample.Sample objects.

  Running cassandra stress test with different thread counts.
  - We increase the thread count gradually by THREAD_INCREMENT_COUNT
  till op rate increases.
  - We decrease the thread count by THREAD_INCREMENT_COUNT/2
  if the operation rate is lower than the previous run.
  """
  samples = []
  last_operation_rate = 0
  run_count = 0
  thread_count = STARTING_THREAD_COUNT
  max_op_rate = 0
  max_op_rate_metadata = None
  while run_count < CASSANDRA_STRESS_MAX_RUNS.value:
    logging.info('running thread count: %s', thread_count)
    run_count += 1
    cpu_loads = RunCassandraStressTest(
        cassandra_vms,
        client_vms,
        metadata['num_keys'],
        metadata['command'],
        thread_count,
        metadata.get('operations'),
        metadata['population_size'],
        metadata['population_dist'],
        metadata['population_parameters'],
        is_preload=False,
    )
    cpu_utilization = GetCpuUtilization(cassandra_vms)
    metadata['server_load_cpu_utilization'] = [
        cpu_loads[i] / cassandra_vms[i].num_cpus * 100
        for i in range(len(cassandra_vms))
    ]
    metadata['server_cpu_utilization'] = cpu_utilization
    metadata['num_cassandra_stress_threads'] = thread_count
    # Trackig disk and memory usage for auditing and debugging purposes.
    for vm in cassandra_vms:
      vm.RemoteCommand('df -H')
      vm.RemoteCommand('free -h')
    current_samples = CollectResults(client_vms, copy.deepcopy(metadata))
    samples.extend(current_samples)
    latest_operation_rate = GetOperationRate(current_samples)
    median_latency = GetMedianLatency(current_samples)
    max_cpu_usage = max(cpu_utilization)

    if max_op_rate < latest_operation_rate:
      max_op_rate = latest_operation_rate
      max_op_rate_metadata = metadata

    if (
        int(median_latency) >= MAX_MEDIAN_LATENCY_MS
        or max_cpu_usage > CASSANDRA_CPU_UTILIZATION_LIMIT.value
    ):
      next_thread_count = thread_count - THREAD_INCREMENT_COUNT / 2
    elif latest_operation_rate > last_operation_rate:
      next_thread_count = thread_count + THREAD_INCREMENT_COUNT
    else:
      next_thread_count = thread_count - THREAD_INCREMENT_COUNT / 2

    last_operation_rate = latest_operation_rate
    thread_count = next_thread_count
    WaitForCompactionTasks(cassandra_vms)
  samples.append(
      sample.Sample(
          'max_op_rate',
          max_op_rate,
          'operations per second',
          max_op_rate_metadata,
      )
  )
  return samples


@vm_util.Retry(
    max_retries=10,
    retryable_exceptions=(
        CassandraCompactionNotCompletedError,
    ),
    poll_interval=WAIT_BETWEEN_COMPACTION_TASKS_CHECK,
)
def WaitForCompactionTasks(cassandra_vms):
  """Waits for cassandra's pending compaction tasks to be completed."""
  pending_compaction_tasks = cassandra.GetPendingTaskCountFromCompactionStats(
      cassandra_vms
  )
  max_pending_compaction_tasks = max(pending_compaction_tasks)
  logging.info(
      'Remaining compaction tasks: %s',
      max_pending_compaction_tasks,
  )
  if max_pending_compaction_tasks > 0:
    raise CassandraCompactionNotCompletedError(
        f'{max_pending_compaction_tasks} compaction tasks not completed'
    )


def GetCpuUtilization(cassandra_vms):
  """Get cpu utilization during the test from sar output.

  Args:
    cassandra_vms: cassandra server vms.

  Returns:
    A list of cpu utilization for each cassandra node.
  """
  cpu_utilization = []
  if (
      CalculateNumberOfSarRequestsFromDuration(
          CASSANDRA_STRESS_RUN_DURATION.value, SAR_CPU_UTILIZATION_INTERVAL
      )
      == 0
  ):
    return cpu_utilization
  for vm in cassandra_vms:
    stdout, _ = vm.RobustRemoteCommand(
        f'cat {GenerateCpuUtilizationFileName(vm)}'
    )
    cpu_utilization.append(ParseAverageCpuUtilization(stdout))
  return cpu_utilization


def ParseUptimeOutput(uptime_output) -> float:
  """Parses the output of the uptime command.

  Args:
    uptime_output: The output of the uptime command.

  Returns:
    The load average of the last 5 minutes.

  Sample uptime command output:
  20:11:37 up 172 days, 22 min, 4 users, load average: 0.23, 0.54, 0.31
  load average displays the load average of the last 1 minute, 5 minutes and 15
  minutes.
  """
  load_average_str = re.sub('.*(load average: )(.*)', r'\2', uptime_output)
  return float(load_average_str.split(', ')[1])


def GetOperationRate(samples):
  for s in samples:
    if s.metric == 'op rate':
      return s.value
  return 0


def GetMedianLatency(samples):
  for s in samples:
    if s.metric == 'latency median':
      return s.value
  return 0


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
