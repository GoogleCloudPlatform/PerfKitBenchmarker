# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run YCSB benchmark against Google Cloud Spanner.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Spanner. Configure the number of VMs via --ycsb_client_vms.
"""

import logging
import time
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_spanner

BENCHMARK_NAME = 'cloud_spanner_ycsb'

BENCHMARK_DESCRIPTION = 'YCSB'
BENCHMARK_TABLE = 'usertable'
BENCHMARK_ZERO_PADDING = 12

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/spanner.admin',
    'https://www.googleapis.com/auth/spanner.data')

BENCHMARK_CONFIG = f"""
cloud_spanner_ycsb:
  description: >
      Run YCSB against Google Cloud Spanner.
      Configure the number of VMs via --ycsb_client_vms.
  relational_db:
    cloud: GCP
    engine: spanner-googlesql
    spanner_nodes: 1
    spanner_description: {BENCHMARK_DESCRIPTION}
    enable_freeze_restore: True
    vm_groups:
      default:
        vm_spec: *default_single_core
        vm_count: 1
  flags:
    openjdk_version: 8
    gcloud_scopes: >
      {' '.join(REQUIRED_SCOPES)}"""

FLAGS = flags.FLAGS
flags.DEFINE_integer('cloud_spanner_ycsb_batchinserts',
                     1,
                     'The Cloud Spanner batch inserts used in the YCSB '
                     'benchmark.')
flags.DEFINE_integer('cloud_spanner_ycsb_boundedstaleness',
                     0,
                     'The Cloud Spanner bounded staleness used in the YCSB '
                     'benchmark.')
flags.DEFINE_enum('cloud_spanner_ycsb_readmode',
                  'query', ['query', 'read'],
                  'The Cloud Spanner read mode used in the YCSB benchmark.')
flags.DEFINE_list('cloud_spanner_ycsb_custom_vm_install_commands', [],
                  'A list of strings. If specified, execute them on every '
                  'VM during the installation phase.')
_CPU_OPTIMIZATION = flags.DEFINE_bool(
    'cloud_spanner_ycsb_cpu_optimization', False,
    'Whether to run in CPU-optimized mode. The test will increase QPS until '
    'CPU is between --cloud_spanner_ycsb_cpu_optimization_target and '
    '--cloud_spanner_ycsb_cpu_optimization_target_max.')
_CPU_TARGET_HIGH_PRIORITY = flags.DEFINE_float(
    'cloud_spanner_ycsb_cpu_optimization_target', 0.65,
    'Minimum target CPU utilization at which to stop the test. The default is '
    'the recommended Spanner high priority CPU utilization, see: '
    'https://cloud.google.com/spanner/docs/cpu-utilization#recommended-max.')
_CPU_TARGET_HIGH_PRIORITY_UPPER_BOUND = flags.DEFINE_float(
    'cloud_spanner_ycsb_cpu_optimization_target_max', 0.75,
    'Maximum target CPU utilization after which the benchmark will throw an '
    'exception. This is needed so that in CPU-optimized mode, the increase in '
    'QPS does not overshoot the target CPU percentage by too much.')
_CPU_OPTIMIZATION_SLEEP_MINUTES = flags.DEFINE_integer(
    'cloud_spanner_ycsb_cpu_optimization_sleep_mins', 0,
    'Time in minutes to sleep in between run steps that increase the target '
    'QPS. This allows for Spanner to run compactions and other background '
    'tasks after a write-heavy workload. See '
    'https://cloud.google.com/spanner/docs/pre-warm-database.')


def _ValidateCpuTargetFlags(flags_dict):
  return (flags_dict['cloud_spanner_ycsb_cpu_optimization_target_max'] >
          flags_dict['cloud_spanner_ycsb_cpu_optimization_target'])


flags.register_multi_flags_validator(
    [
        'cloud_spanner_ycsb_cpu_optimization_target',
        'cloud_spanner_ycsb_cpu_optimization_target_max'
    ], _ValidateCpuTargetFlags,
    'CPU optimization max target must be greater than target.')

_CPU_OPTIMIZATION_INCREMENT_MINUTES = flags.DEFINE_integer(
    'cloud_spanner_ycsb_cpu_optimization_workload_mins', 30,
    'Length of time to run YCSB until incrementing QPS.')
_CPU_OPTIMIZATION_MEASUREMENT_MINUTES = flags.DEFINE_integer(
    'cloud_spanner_ycsb_cpu_optimization_measurement_mins', 5,
    'Length of time to measure average CPU at the end of a test. For example, '
    'the default 5 means that only the last 5 minutes of the test will be '
    'used for representative CPU utilization.')
_STARTING_QPS = flags.DEFINE_integer(
    'cloud_spanner_ycsb_min_target', None,
    'Starting QPS to set as YCSB target. Defaults to a value which uses the '
    'published throughput expectations for each node, see READ/WRITE caps per '
    'node below.')
_CPU_OPTIMIZATION_TARGET_QPS_INCREMENT = flags.DEFINE_integer(
    'cloud_spanner_ycsb_cpu_optimization_target_qps_increment', 1000,
    'The amount to increase target QPS by when running in CPU-optimized mode.')


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['relational_db']['vm_groups']['default'][
        'vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(_):
  """Validates correct flag usages before running this benchmark."""
  for scope in REQUIRED_SCOPES:
    if scope not in FLAGS.gcloud_scopes:
      raise ValueError('Scope {0} required.'.format(scope))
  if _CPU_OPTIMIZATION.value:
    workloads = ycsb.GetWorkloadFileList()
    if len(workloads) != 1:
      raise errors.Setup.InvalidFlagConfigurationError(
          'Running with --cloud_spanner_ycsb_cpu_optimization requires using '
          '1 workload file in --ycsb_workload_files.')
    if FLAGS.ycsb_dynamic_load:
      raise errors.Setup.InvalidFlagConfigurationError(
          '--ycsb_dynamic_load and --cloud_spanner_ycsb_cpu_optimization are '
          'mutually exclusive.')
    if _CPU_OPTIMIZATION_INCREMENT_MINUTES.value < (
        _CPU_OPTIMIZATION_MEASUREMENT_MINUTES.value +
        gcp_spanner.CPU_API_DELAY_MINUTES):
      raise errors.Setup.InvalidFlagConfigurationError(
          f'workload_mins {_CPU_OPTIMIZATION_INCREMENT_MINUTES.value} must be '
          'greater than measurement_mins '
          f'{_CPU_OPTIMIZATION_MEASUREMENT_MINUTES.value} '
          f'+ CPU_API_DELAY_MINUTES {gcp_spanner.CPU_API_DELAY_MINUTES}')


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud spanner benchmarks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  background_tasks.RunThreaded(_Install, vms)

  benchmark_spec.executor = ycsb.YCSBExecutor('cloudspanner')

  spanner: gcp_spanner.GcpSpannerInstance = benchmark_spec.relational_db
  spanner.CreateTables(_BuildSchema())


def _GetCpuOptimizationMetadata() -> Dict[str, Any]:
  return {
      'cloud_spanner_cpu_optimization':
          True,
      'cloud_spanner_cpu_target':
          _CPU_TARGET_HIGH_PRIORITY.value,
      'cloud_spanner_cpu_increment_minutes':
          _CPU_OPTIMIZATION_INCREMENT_MINUTES.value,
      'cloud_spanner_cpu_measurement_minutes':
          _CPU_OPTIMIZATION_MEASUREMENT_MINUTES.value,
      'cloud_spanner_cpu_qps_increment':
          _CPU_OPTIMIZATION_TARGET_QPS_INCREMENT.value,
  }


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  spanner: gcp_spanner.GcpSpannerInstance = benchmark_spec.relational_db

  run_kwargs = {
      'table': BENCHMARK_TABLE,
      'zeropadding': BENCHMARK_ZERO_PADDING,
      'cloudspanner.instance': spanner.instance_id,
      'cloudspanner.database': spanner.database,
      'cloudspanner.readmode': FLAGS.cloud_spanner_ycsb_readmode,
      'cloudspanner.boundedstaleness':
          FLAGS.cloud_spanner_ycsb_boundedstaleness,
      'cloudspanner.batchinserts': FLAGS.cloud_spanner_ycsb_batchinserts,
  }
  # Uses overridden cloud spanner endpoint in gcloud configuration
  end_point = spanner.GetApiEndPoint()
  if end_point:
    run_kwargs['cloudspanner.host'] = end_point

  load_kwargs = run_kwargs.copy()
  samples = []
  metadata = {'ycsb_client_type': 'java'}
  if not spanner.restored:
    samples += list(benchmark_spec.executor.Load(vms, load_kwargs=load_kwargs))
  if _CPU_OPTIMIZATION.value:
    samples += CpuUtilizationRun(benchmark_spec.executor, spanner, vms,
                                 run_kwargs)
    metadata.update(_GetCpuOptimizationMetadata())
  else:
    samples += list(benchmark_spec.executor.Run(vms, run_kwargs=run_kwargs))

  for result in samples:
    result.metadata.update(metadata)
    result.metadata.update(spanner.GetResourceMetadata())

  return samples


def _ExtractThroughput(samples: List[sample.Sample]) -> float:
  for result in samples:
    if result.metric == 'overall Throughput':
      return result.value
  return 0.0


def CpuUtilizationRun(executor: ycsb.YCSBExecutor,
                      spanner: gcp_spanner.GcpSpannerInstance,
                      vms: List[virtual_machine.VirtualMachine],
                      run_kwargs: Dict[str, Any]) -> List[sample.Sample]:
  """Runs YCSB until the CPU utilization is over 65%."""
  workload = ycsb.GetWorkloadFileList()[0]
  with open(workload) as f:
    workload_args = ycsb.ParseWorkload(f.read())
  read_proportion = float(workload_args['readproportion'])
  write_proportion = float(workload_args['updateproportion'])
  qps = _STARTING_QPS.value or int(spanner.CalculateRecommendedThroughput(
      read_proportion, write_proportion) * 0.5)  # Leave space for increment.
  first_run = True
  while True:
    run_kwargs['target'] = qps
    run_kwargs['maxexecutiontime'] = (
        _CPU_OPTIMIZATION_INCREMENT_MINUTES.value * 60)
    run_samples = executor.Run(vms, run_kwargs=run_kwargs)
    throughput = _ExtractThroughput(run_samples)
    cpu_utilization = spanner.GetAverageCpuUsage(
        _CPU_OPTIMIZATION_MEASUREMENT_MINUTES.value)
    logging.info(
        'Run had throughput target %s and measured throughput %s, '
        'with average high-priority CPU utilization %s.',
        qps, throughput, cpu_utilization)
    if cpu_utilization > _CPU_TARGET_HIGH_PRIORITY.value:
      logging.info('CPU utilization is higher than cap %s, stopping test',
                   _CPU_TARGET_HIGH_PRIORITY.value)
      if first_run:
        raise errors.Benchmarks.RunError(
            f'Initial QPS {qps} already above cpu utilization cap. '
            'Please lower the starting QPS.')
      if cpu_utilization > _CPU_TARGET_HIGH_PRIORITY_UPPER_BOUND.value:
        raise errors.Benchmarks.RunError(
            f'CPU utilization measured was {cpu_utilization}, over the '
            f'{_CPU_TARGET_HIGH_PRIORITY_UPPER_BOUND.value} threshold. '
            'Decrease step size to avoid overshooting.')
      for s in run_samples:
        s.metadata['cloud_spanner_cpu_utilization'] = cpu_utilization
      return run_samples

    # Sleep between steps for some workloads.
    if _CPU_OPTIMIZATION_SLEEP_MINUTES.value:
      logging.info(
          'Run phase finished, sleeping for %s minutes before starting the '
          'next run.', _CPU_OPTIMIZATION_SLEEP_MINUTES.value)
      time.sleep(_CPU_OPTIMIZATION_SLEEP_MINUTES.value * 60)

    qps += _CPU_OPTIMIZATION_TARGET_QPS_INCREMENT.value
    first_run = False


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  del benchmark_spec


def _BuildSchema():
  """BuildSchema.

  Returns:
    A string of DDL for creating a Spanner table.
  """
  fields = ',\n'.join(
      [f'field{i} STRING(MAX)' for i in range(FLAGS.ycsb_field_count)]
  )
  return f"""
  CREATE TABLE {BENCHMARK_TABLE} (
    id     STRING(MAX),
    {fields}
  ) PRIMARY KEY(id)
  """


def _Install(vm):
  vm.Install('ycsb')

  # Run custom VM installation commands.
  for command in FLAGS.cloud_spanner_ycsb_custom_vm_install_commands:
    _, _ = vm.RemoteCommand(command)
