# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs YCSB against Cloud Bigtable.

Cloud Bigtable (https://cloud.google.com/bigtable/) is a managed NoSQL database
with an HBase-compatible API.

Compared to hbase_ycsb, this benchmark:
  * Modifies hbase-site.xml to work with Cloud Bigtable.
  * Adds the Bigtable client JAR.
"""

import datetime
import json
import logging
import os
import shlex
import posixpath
import subprocess
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import hbase_ycsb_benchmark as hbase_ycsb
from perfkitbenchmarker.linux_packages import google_cloud_bigtable_client
from perfkitbenchmarker.linux_packages import google_cloud_cbt
from perfkitbenchmarker.linux_packages import hbase
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_bigtable

FLAGS = flags.FLAGS

_STATIC_TABLE_NAME = flags.DEFINE_string(
    'google_bigtable_static_table_name',
    None,
    'Bigtable table name. If not specified, a temporary table '
    'will be created and deleted on the fly.',
)
_DELETE_STATIC_TABLE = flags.DEFINE_boolean(
    'google_bigtable_delete_static_table',
    False,
    'Whether or not to delete a static table during cleanup. Temporary tables '
    'are always cleaned up.',
)
_GET_CPU_UTILIZATION = flags.DEFINE_boolean(
    'get_bigtable_cluster_cpu_utilization',
    False,
    'If true, will gather bigtable cluster cpu utilization '
    'for the duration of performance test run stage, and add '
    'samples to the result. Table loading phase is excluded '
    'the metric collection. To enable this functionality, '
    'need to set environment variable '
    'GOOGLE_APPLICATION_CREDENTIALS as described in '
    'https://cloud.google.com/docs/authentication/'
    'getting-started.',
)
_MONITORING_ADDRESS = flags.DEFINE_string(
    'google_monitoring_endpoint',
    'monitoring.googleapis.com',
    'Google API endpoint for monitoring requests. Used when '
    '--get_bigtable_cluster_cpu_utilization is enabled.',
)
_USE_JAVA_VENEER_CLIENT = flags.DEFINE_boolean(
    'google_bigtable_use_java_veneer_client',
    False,
    'If true, will use the googlebigtableclient with ycsb.',
)
# Temporary until old driver is deprecated.
_USE_UPGRADED_DRIVER = flags.DEFINE_boolean(
    'google_bigtable_use_upgraded_driver',
    False,
    'If true, will use the googlebigtableclient2 with ycsb. Requires'
    ' --google_bigtable_use_java_veneer_client to be true.',
)
_ENABLE_DIRECT_PATH = flags.DEFINE_boolean(
    'google_bigtable_enable_direct_path',
    False,
    'If true, sets an environment variable to enable DirectPath.',
)
_ENABLE_TRAFFIC_DIRECTOR = flags.DEFINE_boolean(
    'google_bigtable_enable_traffic_director',
    False,
    'If true, will use the googlebigtable'
    'client with ycsb to enable traffic through traffic director.',
)
_ENABLE_RLS_ROUTING = flags.DEFINE_boolean(
    'google_bigtable_enable_rls_routing',
    False,
    'If true, will use the googlebigtableclient with ycsb to enable traffic'
    'through RLS with direct path',
)
_ENABLE_EXPERIMENTAL_LB_POLICY = flags.DEFINE_boolean(
    'google_bigtable_enable_experimental_lb_policy',
    False,
    'If true, will use the googlebigtableclient with ycsb to enable beta LB'
    'policy.',
)
_CHANNEL_COUNT = flags.DEFINE_integer(
    'google_bigtable_channel_count',
    None,
    (
        'If specified, will use this many channels (i.e. connections) for '
        'Bigtable RPCs instead of the default number.'
    ),
)

BENCHMARK_NAME = 'cloud_bigtable_ycsb'
BENCHMARK_CONFIG = """
cloud_bigtable_ycsb:
  description: >
      Run YCSB against an existing Cloud Bigtable
      instance. Configure the number of client VMs via --num_vms.
  non_relational_db:
    service_type: bigtable
    enable_freeze_restore: True
  vm_groups:
    default:
      os_type: ubuntu2204  # Python 2
      vm_spec: *default_single_core
      vm_count: null
  flags:
    openjdk_version: 8
    gcloud_scopes: >
      https://www.googleapis.com/auth/monitoring.write
      https://www.googleapis.com/auth/bigtable.admin
      https://www.googleapis.com/auth/bigtable.data"""

METRICS_CORE_JAR = 'metrics-core-3.1.2.jar'
DROPWIZARD_METRICS_CORE_URL = posixpath.join(
    'https://search.maven.org/remotecontent?filepath='
    'io/dropwizard/metrics/metrics-core/3.1.2/',
    METRICS_CORE_JAR,
)
HBASE_SITE = 'cloudbigtable/hbase-site.xml.j2'
HBASE_CONF_FILES = [HBASE_SITE]

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/monitoring.write',
    'https://www.googleapis.com/auth/bigtable.admin',
    'https://www.googleapis.com/auth/bigtable.data',
)

# TODO(user): Make table parameters configurable.
COLUMN_FAMILY = 'cf'
BENCHMARK_DATA = {
    METRICS_CORE_JAR: (
        '245ba2a66a9bc710ce4db14711126e77bcb4e6d96ef7e622659280f3c90cbb5c'
    ),
}
BENCHMARK_DATA_URL = {
    METRICS_CORE_JAR: DROPWIZARD_METRICS_CORE_URL,
}

_Bigtable = gcp_bigtable.GcpBigtableInstance


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(benchmark_config: Dict[str, Any]) -> None:
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  if not _USE_JAVA_VENEER_CLIENT.value:
    # HBase prereqs
    for resource in HBASE_CONF_FILES:
      data.ResourcePath(resource)
    hbase.CheckPrerequisites()

  ycsb.CheckPrerequisites()

  for scope in REQUIRED_SCOPES:
    if scope not in FLAGS.gcloud_scopes:
      if (
          scope == 'https://www.googleapis.com/auth/monitoring.write'
          and not _USE_JAVA_VENEER_CLIENT.value
      ):
        # Client side metrics are only required with the Veneer client.
        continue
      raise ValueError('Scope {} required.'.format(scope))

  if ycsb.CPU_OPTIMIZATION.value and (
      ycsb.CPU_OPTIMIZATION_MEASUREMENT_MINS.value
      <= gcp_bigtable.CPU_API_DELAY_MINUTES
  ):
    raise errors.Setup.InvalidFlagConfigurationError(
        f'measurement_mins {ycsb.CPU_OPTIMIZATION_MEASUREMENT_MINS.value} must'
        ' be greater than CPU_API_DELAY_MINUTES'
        f' {gcp_bigtable.CPU_API_DELAY_MINUTES}.'
    )

  # Temporary until old driver is deprecated.
  if _USE_UPGRADED_DRIVER.value and not _USE_JAVA_VENEER_CLIENT.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--google_bigtable_use_upgraded_driver requires'
        ' --google_bigtable_use_java_veneer_client to be true.'
    )


def _GetTableName() -> str:
  return _STATIC_TABLE_NAME.value or 'ycsb{}'.format(FLAGS.run_uri)


def _GetDefaultProject() -> str:
  """Returns the default project used for this test."""
  cmd = [FLAGS.gcloud_path, 'config', 'list', '--format', 'json']
  stdout, _, return_code = vm_util.IssueCommand(cmd)
  if return_code:
    raise subprocess.CalledProcessError(return_code, cmd, stdout)

  config = json.loads(stdout)

  try:
    return config['core']['project']
  except KeyError as key_error:
    raise KeyError(f'No default project found in {config}') from key_error


def _Install(vm: virtual_machine.VirtualMachine, bigtable: _Bigtable) -> None:
  """Install YCSB and CBT HBase client on 'vm'."""
  vm.Install('ycsb')
  vm.Install('google_cloud_cbt')  # we use the CLI to create and delete tables
  vm.Install('maven')

  if _ENABLE_TRAFFIC_DIRECTOR.value and _USE_JAVA_VENEER_CLIENT.value:
    vm.RemoteCommand(
        'echo "export GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS=true" | sudo tee -a'
        ' /etc/environment'
    )
    if _ENABLE_RLS_ROUTING.value:
      vm.RemoteCommand(
          'echo "export GRPC_EXPERIMENTAL_XDS_RLS_LB=true" | sudo tee -a'
          ' /etc/environment'
      )
    if _ENABLE_EXPERIMENTAL_LB_POLICY.value:
      vm.RemoteCommand(
          'echo "export GRPC_EXPERIMENTAL_PICKFIRST_LB_CONFIG=true" | sudo tee'
          ' -a /etc/environment'
      )

  if not _USE_JAVA_VENEER_CLIENT.value:
    # Install HBase deps and the HBase client.
    vm.Install('hbase')
    vm.Install('google_cloud_bigtable_client')

    vm.RemoteCommand(
        f'echo "export JAVA_HOME=/usr" >> {hbase.HBASE_CONF_DIR}/hbase-env.sh'
    )

    context = {
        'google_bigtable_endpoint': gcp_bigtable.ENDPOINT.value,
        'google_bigtable_admin_endpoint': gcp_bigtable.ADMIN_ENDPOINT.value,
        'project': FLAGS.project or _GetDefaultProject(),
        'instance': bigtable.name,
        'app_profile': gcp_bigtable.APP_PROFILE_ID.value,
        'hbase_major_version': FLAGS.hbase_version.split('.')[0],
        'channel_count': _CHANNEL_COUNT.value,
    }

    for file_name in HBASE_CONF_FILES:
      file_path = data.ResourcePath(file_name)
      remote_path = posixpath.join(
          hbase.HBASE_CONF_DIR, os.path.basename(file_name)
      )
      if file_name.endswith('.j2'):
        vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
      else:
        vm.RemoteCopy(file_path, remote_path)

  if _ENABLE_DIRECT_PATH.value:
    # Requires minimum client bigtable 2.45.0 or bigtable-hbase 2.14.5.
    vm.RemoteCommand(
        'echo "export CBT_ENABLE_DIRECTPATH=true" | sudo tee -a'
        ' /etc/environment'
    )


@vm_util.Retry()
def _GetCpuUtilizationSample(
    samples: List[sample.Sample], instance_id: str
) -> List[sample.Sample]:
  """Gets a list of cpu utilization samples - one per cluster per workload.

  Note that the utilization only covers the workload run stage.

  Args:
    samples: list of sample.Sample. Used to find the timestamp information to
      determine the time windows for the cpu metrics.
    instance_id: the bigtable instance id.

  Returns:
    a list of samples for metrics "cpu_load" and "cpu_load_hottest_node",
  """
  runtime_samples = [
      s
      for s in samples
      if s.metadata.get('stage') == 'run' and s.metric == 'overall RunTime'
  ]

  # pylint: disable=g-import-not-at-top
  from google.cloud import monitoring_v3
  from google.cloud.monitoring_v3 import query

  client = monitoring_v3.MetricServiceClient(
      transport=monitoring_v3.services.metric_service.transports.grpc.MetricServiceGrpcTransport(
          host=_MONITORING_ADDRESS.value
      )
  )

  cpu_samples = []
  time_units_in_secs = {'s': 1, 'ms': 0.001, 'us': 0.000001}
  for runtime_sample in runtime_samples:
    if runtime_sample.unit not in time_units_in_secs:
      logging.warning(
          'The unit of overall RunTime is not supported: %s',
          runtime_sample.unit,
      )
      continue

    duration_sec = runtime_sample.value * time_units_in_secs.get(
        runtime_sample.unit
    )
    workload_duration_minutes = max(1, int(duration_sec / 60))

    # workload_index helps associate the cpu metrics with the current run stage.
    workload_index = runtime_sample.metadata.get('workload_index')

    # Query the cpu utilization, which are gauged values at each minute in the
    # time window determined by end_timestamp and workload_duration_minutes.
    end_timestamp = runtime_sample.timestamp
    for metric in ['cpu_load', 'cpu_load_hottest_node']:
      cpu_query = query.Query(
          client,
          project=(FLAGS.project or _GetDefaultProject()),
          metric_type=f'bigtable.googleapis.com/cluster/{metric}',
          end_time=datetime.datetime.fromtimestamp(
              end_timestamp, datetime.timezone.utc
          ),
          minutes=workload_duration_minutes,
      )
      cpu_query = cpu_query.select_resources(instance=instance_id)
      time_series = list(cpu_query)
      if not time_series:
        logging.debug(
            'Time series for computing %s could not be found.', metric
        )
        continue

      # Build and add the cpu samples from the query results.
      for cluster_number, cluster_time_series in enumerate(time_series):
        utilization = [
            round(point.value.double_value, 3)
            for point in cluster_time_series.points
        ]

        average_utilization = round(sum(utilization) / len(utilization), 3)
        metadata = {
            'cluster_number': cluster_number,
            'workload_index': workload_index,
            'cpu_utilization_per_minute': utilization,
            'cpu_average_utilization': average_utilization,
        }

        cpu_utilization_sample = sample.Sample(
            f'{metric}_array', -1, '', metadata
        )

        cpu_samples.append(cpu_utilization_sample)
  return cpu_samples


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepare the virtual machines to run cloud bigtable.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  vms = benchmark_spec.vms

  instance: _Bigtable = benchmark_spec.non_relational_db
  args = [((vm, instance), {}) for vm in vms]
  background_tasks.RunThreaded(_Install, args)

  _CreateTable(benchmark_spec)


def _GetYcsbExecutor(
    vms: list[virtual_machine.VirtualMachine],
) -> ycsb.YCSBExecutor:
  """Gets the YCSB executor class for loading and running the benchmark."""
  ycsb_memory = min(vms[0].total_memory_kb // 1024, 4096)
  jvm_args = shlex.quote(f' -Xmx{ycsb_memory}m')
  env = {}
  if _ENABLE_DIRECT_PATH.value:
    env['CBT_ENABLE_DIRECTPATH'] = str(_ENABLE_DIRECT_PATH.value)

  if _USE_JAVA_VENEER_CLIENT.value:
    executor_flags = {'jvm-args': jvm_args, 'table': _GetTableName()}
    # Temporary until old driver is deprecated.
    if _USE_UPGRADED_DRIVER.value:
      client_version = google_cloud_bigtable_client.CLIENT_VERSION.value
      if client_version:
        env['MAVEN_ARGS'] = f'-Dgooglebigtable2.version={client_version}'
      return ycsb.YCSBExecutor(
          'googlebigtable2', environment=env, **executor_flags
      )
    return ycsb.YCSBExecutor(
        'googlebigtable', environment=env, **executor_flags
    )

  # Add hbase conf dir to the classpath.
  executor_flags = {
      'cp': hbase.HBASE_CONF_DIR,
      'jvm-args': jvm_args,
      'table': _GetTableName(),
  }
  return ycsb.YCSBExecutor(
      FLAGS.hbase_binding, environment=env, **executor_flags
  )


def _LoadDatabase(
    executor: ycsb.YCSBExecutor,
    bigtable: gcp_bigtable.GcpBigtableInstance,
    vms: list[virtual_machine.VirtualMachine],
    load_kwargs: dict[str, Any],
) -> list[sample.Sample]:
  """Loads the database with the specified infrastructure capacity."""
  if bigtable.restored or ycsb.SKIP_LOAD_STAGE.value:
    return []
  bigtable.UpdateCapacityForLoad()
  results = list(executor.Load(vms, load_kwargs=load_kwargs))
  bigtable.UpdateCapacityForRun()
  return results


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  instance: _Bigtable = benchmark_spec.non_relational_db

  metadata = {
      'ycsb_client_vms': len(vms),
      'direct_path': _ENABLE_DIRECT_PATH.value,
  }
  metadata.update(instance.GetResourceMetadata())

  samples = []
  load_kwargs = _GenerateLoadKwargs(instance)
  run_kwargs = _GenerateRunKwargs(instance)
  executor: ycsb.YCSBExecutor = _GetYcsbExecutor(vms)
  samples += _LoadDatabase(executor, instance, vms, load_kwargs)
  samples += list(executor.Run(vms, run_kwargs=run_kwargs, database=instance))

  # Optionally add new samples for cluster cpu utilization.
  if _GET_CPU_UTILIZATION.value:
    cpu_utilization_samples = _GetCpuUtilizationSample(samples, instance.name)
    samples.extend(cpu_utilization_samples)

  for current_sample in samples:
    current_sample.metadata.update(metadata)

  return samples


def _CommonArgs(instance: _Bigtable) -> Dict[str, str]:
  """Generates args for both run/load for YCSB.

  Args:
    instance: The instance the test will be run against.

  Returns:
    Arguments dict for YCSB.
  """

  kwargs = {'columnfamily': COLUMN_FAMILY}
  if _USE_JAVA_VENEER_CLIENT.value:
    # Temporary until old driver is deprecated.
    if _USE_UPGRADED_DRIVER.value:
      kwargs['googlebigtable2.project'] = FLAGS.project or _GetDefaultProject()
      kwargs['googlebigtable2.instance'] = instance.name
      kwargs['googlebigtable2.app-profile'] = gcp_bigtable.APP_PROFILE_ID.value
      kwargs['googlebigtable2.family'] = COLUMN_FAMILY
      kwargs['googlebigtable2.timestamp'] = '0'
      kwargs['googlebigtable2.data-endpoint'] = (
          gcp_bigtable.ENDPOINT.value + ':443'
      )
      kwargs.pop('columnfamily')
    else:
      kwargs['google.bigtable.instance.id'] = instance.name
      kwargs['google.bigtable.app_profile.id'] = (
          gcp_bigtable.APP_PROFILE_ID.value
      )
      kwargs['google.bigtable.project.id'] = (
          FLAGS.project or _GetDefaultProject()
      )
      kwargs['google.bigtable.data.endpoint'] = (
          gcp_bigtable.ENDPOINT.value + ':443'
      )
  return kwargs


def _GenerateRunKwargs(instance: _Bigtable) -> Dict[str, str]:
  """Generates run arguments for YCSB.

  Args:
    instance: The instance the test will be run against.

  Returns:
    Run arguments for YCSB.
  """

  run_kwargs = _CommonArgs(instance)

  if _USE_JAVA_VENEER_CLIENT.value and _USE_UPGRADED_DRIVER.value:
    run_kwargs['googlebigtable2.use-batching'] = 'false'
  else:
    # By default YCSB uses a BufferedMutator for Puts / Deletes.
    # This leads to incorrect update latencies, since since the call returns
    # before the request is acked by the server.
    # Disable this behavior during the benchmark run.
    run_kwargs['clientbuffering'] = 'false'

  return run_kwargs


def _GenerateLoadKwargs(instance: _Bigtable) -> Dict[str, str]:
  """Generates load arguments for YCSB.

  Args:
    instance: The instance the test will be run against.

  Returns:
    Load arguments for YCSB.
  """

  load_kwargs = _CommonArgs(instance)

  if _USE_JAVA_VENEER_CLIENT.value and _USE_UPGRADED_DRIVER.value:
    load_kwargs['googlebigtable2.use-batching'] = 'true'
  else:
    # During the load stage, use a buffered mutator with a single thread.
    # The BufferedMutator will handle multiplexing RPCs.
    load_kwargs['clientbuffering'] = 'true'

  if not FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = '1'

  return load_kwargs


@vm_util.Retry()
def _CreateTable(benchmark_spec: bm_spec.BenchmarkSpec):
  """Creates a table when not using a static (ie user managed) table.

  If the table is user-managed, this is a no-op.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  instance: _Bigtable = benchmark_spec.non_relational_db
  vm = benchmark_spec.vms[0]
  splits = ','.join([
      f'user{1000 + i * (9999 - 1000) // hbase_ycsb.TABLE_SPLIT_COUNT}'
      for i in range(hbase_ycsb.TABLE_SPLIT_COUNT)
  ])
  command = [
      google_cloud_cbt.CBT_BIN,
      f'-project={FLAGS.project or _GetDefaultProject()}',
      f'-instance={instance.name}',
      f'-admin-endpoint={gcp_bigtable.ADMIN_ENDPOINT.value}:443',
      'createtable',
      _GetTableName(),
      # Settings derived from data/hbase/create-ycsb-table.hbaseshell.j2
      f'families={COLUMN_FAMILY}:maxversions=1',
      f'splits={splits}',
  ]

  try:
    vm.RemoteCommand(' '.join(command))
  except errors.VirtualMachine.RemoteCommandError as e:
    # Expected if --google_bigtable_static_table_name is set. We don't want to
    # skip table creation in this case because it is convenient to have PKB
    # create the table on the first run, but skip creation on any repeated runs.
    if 'AlreadyExists' not in str(e):
      raise


@vm_util.Retry()
def _CleanupTable(benchmark_spec: bm_spec.BenchmarkSpec):
  """Deletes a table under a user managed instance.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  instance: _Bigtable = benchmark_spec.non_relational_db
  command = [
      google_cloud_cbt.CBT_BIN,
      f'-project={FLAGS.project or _GetDefaultProject()}',
      f'-instance={instance.name}',
      f'-admin-endpoint={gcp_bigtable.ADMIN_ENDPOINT.value}:443',
      'deletetable',
      _GetTableName(),
  ]
  vm.RemoteCommand(' '.join(command))


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  instance: _Bigtable = benchmark_spec.non_relational_db
  if instance.user_managed and (
      _STATIC_TABLE_NAME.value is None or _DELETE_STATIC_TABLE.value
  ):
    # Only need to drop the temporary tables if we're not deleting the instance.
    _CleanupTable(benchmark_spec)
