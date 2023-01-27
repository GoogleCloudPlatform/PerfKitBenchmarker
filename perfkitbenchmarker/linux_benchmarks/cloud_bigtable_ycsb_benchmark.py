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
import pipes
import posixpath
import subprocess
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
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

HBASE_CLIENT_VERSION = '1.x'
BIGTABLE_CLIENT_VERSION = '1.4.0'

# TODO(user): remove the custom ycsb build once the head version of YCSB
# is updated to share Bigtable table object. The source code of the patched YCSB
# 0.14.0 can be found at 'https://storage.googleapis.com/cbt_ycsb_client_jar/'
# 'YCSB-0.14.0-Bigtable-table-object-sharing.zip'.
YCSB_BIGTABLE_TABLE_SHARING_TAR_URL = (
    'https://storage.googleapis.com/cbt_ycsb_client_jar/ycsb-0.14.0.tar.gz')

_STATIC_TABLE_NAME = flags.DEFINE_string(
    'google_bigtable_static_table_name', None,
    'Bigtable table name. If not specified, a temporary table '
    'will be created and deleted on the fly.')
_DELETE_STATIC_TABLE = flags.DEFINE_boolean(
    'google_bigtable_delete_static_table', False,
    'Whether or not to delete a static table during cleanup. Temporary tables '
    'are always cleaned up.')
_TABLE_OBJECT_SHARING = flags.DEFINE_boolean(
    'google_bigtable_enable_table_object_sharing', False,
    'If true, will use a YCSB binary that shares the same '
    'Bigtable table object across all the threads on a VM.')
_HBASE_JAR_URL = flags.DEFINE_string(
    'google_bigtable_hbase_jar_url',
    'https://oss.sonatype.org/service/local/repositories/releases/content/'
    'com/google/cloud/bigtable/bigtable-hbase-{0}-hadoop/'
    '{1}/bigtable-hbase-{0}-hadoop-{1}.jar'.format(HBASE_CLIENT_VERSION,
                                                   BIGTABLE_CLIENT_VERSION),
    'URL for the Bigtable-HBase client JAR. Deprecated: Prefer to use '
    '--google_bigtable_client_version instead.')
_GET_CPU_UTILIZATION = flags.DEFINE_boolean(
    'get_bigtable_cluster_cpu_utilization', False,
    'If true, will gather bigtable cluster cpu utilization '
    'for the duration of performance test run stage, and add '
    'samples to the result. Table loading phase is excluded '
    'the metric collection. To enable this functionality, '
    'need to set environment variable '
    'GOOGLE_APPLICATION_CREDENTIALS as described in '
    'https://cloud.google.com/docs/authentication/'
    'getting-started.')
_MONITORING_ADDRESS = flags.DEFINE_string(
    'google_monitoring_endpoint', 'monitoring.googleapis.com',
    'Google API endpoint for monitoring requests. Used when '
    '--get_bigtable_cluster_cpu_utilization is enabled.')

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
      vm_spec: *default_single_core
      vm_count: null
  flags:
    openjdk_version: 8
    gcloud_scopes: >
      https://www.googleapis.com/auth/bigtable.admin
      https://www.googleapis.com/auth/bigtable.data"""

METRICS_CORE_JAR = 'metrics-core-3.1.2.jar'
DROPWIZARD_METRICS_CORE_URL = posixpath.join(
    'https://search.maven.org/remotecontent?filepath='
    'io/dropwizard/metrics/metrics-core/3.1.2/', METRICS_CORE_JAR)
HBASE_SITE = 'cloudbigtable/hbase-site.xml.j2'
HBASE_CONF_FILES = [HBASE_SITE]

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/bigtable.admin',
    'https://www.googleapis.com/auth/bigtable.data')

# TODO(user): Make table parameters configurable.
COLUMN_FAMILY = 'cf'
BENCHMARK_DATA = {
    METRICS_CORE_JAR:
        '245ba2a66a9bc710ce4db14711126e77bcb4e6d96ef7e622659280f3c90cbb5c',
}
BENCHMARK_DATA_URL = {
    METRICS_CORE_JAR: DROPWIZARD_METRICS_CORE_URL,
}

_Bigtable = gcp_bigtable.GcpBigtableInstance


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config: Dict[str, Any]) -> None:
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Unused.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  del benchmark_config
  for resource in HBASE_CONF_FILES:
    data.ResourcePath(resource)

  hbase.CheckPrerequisites()
  ycsb.CheckPrerequisites()

  for scope in REQUIRED_SCOPES:
    if scope not in FLAGS.gcloud_scopes:
      raise ValueError('Scope {0} required.'.format(scope))


def _GetInstanceDescription(project: str, instance_name: str) -> Dict[str, Any]:
  """Gets the description for a Cloud Bigtable instance.

  Args:
    project: str. Name of the project in which the instance was created.
    instance_name: str. ID of the desired Bigtable instance.

  Returns:
    A dictionary containing an instance description.

  Raises:
    KeyError: when the instance was not found.
    IOError: when the list bigtable command fails.
  """
  env = {'CLOUDSDK_CORE_DISABLE_PROMPTS': '1'}
  env.update(os.environ)

  cmd = [FLAGS.gcloud_path, 'beta', 'bigtable', 'instances', 'describe',
         instance_name,
         '--format', 'json',
         '--project', project]
  stdout, stderr, returncode = vm_util.IssueCommand(cmd, env=env)
  if returncode:
    cmd_str = ' '.join(cmd)
    raise IOError(f'Command "{cmd_str}" failed:\n'
                  f'STDOUT:\n{stdout}\nSTDERR:\n{stderr}')
  return json.loads(stdout)


def _GetTableName() -> str:
  return _STATIC_TABLE_NAME.value or 'ycsb{0}'.format(FLAGS.run_uri)


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


def _InstallClientLegacy(vm: virtual_machine.VirtualMachine):
  """Legacy function for installing the bigtable client from a given URL."""
  vm.Install('curl')

  hbase_lib = posixpath.join(hbase.HBASE_DIR, 'lib')

  if 'hbase-1.x' in _HBASE_JAR_URL.value:
    preprovisioned_pkgs = [METRICS_CORE_JAR]
    ycsb_hbase_lib = posixpath.join(ycsb.YCSB_DIR,
                                    FLAGS.hbase_binding + '-binding', 'lib')
    vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, preprovisioned_pkgs,
                                          ycsb_hbase_lib)
    vm.InstallPreprovisionedBenchmarkData(BENCHMARK_NAME, preprovisioned_pkgs,
                                          hbase_lib)

  url = _HBASE_JAR_URL.value
  jar_name = os.path.basename(url)
  jar_path = posixpath.join(ycsb_hbase_lib, jar_name)
  vm.RemoteCommand(f'curl -Lo {jar_path} {url}')
  vm.RemoteCommand(f'cp {jar_path} {hbase_lib}')


def _Install(vm: virtual_machine.VirtualMachine, bigtable: _Bigtable) -> None:
  """Install YCSB and CBT HBase client on 'vm'."""
  vm.Install('ycsb')
  vm.Install('hbase')
  vm.Install('google_cloud_cbt')  # we use the CLI to create and delete tables

  if google_cloud_bigtable_client.CLIENT_VERSION.value:
    vm.Install('google_cloud_bigtable_client')
  else:
    _InstallClientLegacy(vm)

  vm.RemoteCommand(
      f'echo "export JAVA_HOME=/usr" >> {hbase.HBASE_CONF_DIR}/hbase-env.sh')

  context = {
      'google_bigtable_endpoint': gcp_bigtable.ENDPOINT.value,
      'google_bigtable_admin_endpoint': gcp_bigtable.ADMIN_ENDPOINT.value,
      'project': FLAGS.project or _GetDefaultProject(),
      'instance': bigtable.name,
      'hbase_version': HBASE_CLIENT_VERSION.replace('.', '_'),
  }

  for file_name in HBASE_CONF_FILES:
    file_path = data.ResourcePath(file_name)
    remote_path = posixpath.join(hbase.HBASE_CONF_DIR,
                                 os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def _GetCpuUtilizationSample(samples: List[sample.Sample],
                             instance_id: str) -> List[sample.Sample]:
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
      s for s in samples
      if s.metadata.get('stage') == 'run' and s.metric == 'overall RunTime'
  ]

  # pylint: disable=g-import-not-at-top
  from google.cloud import monitoring_v3
  from google.cloud.monitoring_v3 import query
  from google.cloud.monitoring_v3.gapic.transports import metric_service_grpc_transport

  client = monitoring_v3.MetricServiceClient(
      transport=metric_service_grpc_transport.MetricServiceGrpcTransport(
          address=_MONITORING_ADDRESS.value))

  cpu_samples = []
  time_units_in_secs = {'s': 1, 'ms': 0.001, 'us': 0.000001}
  for runtime_sample in runtime_samples:
    if runtime_sample.unit not in time_units_in_secs:
      logging.warning('The unit of overall RunTime is not supported: %s',
                      runtime_sample.unit)
      continue

    duration_sec = runtime_sample.value * time_units_in_secs.get(
        runtime_sample.unit)
    workload_duration_minutes = max(1, int(duration_sec / 60))

    # workload_index helps associate the cpu metrics with the current run stage.
    workload_index = runtime_sample.metadata.get('workload_index')

    # Query the cpu utilization, which are gauged values at each minute in the
    # time window determined by end_timestamp and workload_duration_minutes.
    end_timestamp = runtime_sample.timestamp
    for metric in ['cpu_load', 'cpu_load_hottest_node']:
      cpu_query = query.Query(
          client, project=(FLAGS.project or _GetDefaultProject()),
          metric_type=f'bigtable.googleapis.com/cluster/{metric}',
          end_time=datetime.datetime.utcfromtimestamp(end_timestamp),
          minutes=workload_duration_minutes)
      cpu_query = cpu_query.select_resources(instance=instance_id)
      time_series = list(cpu_query)
      if not time_series:
        logging.debug(
            'Time series for computing %s could not be found.', metric)
        continue

      # Build and add the cpu samples from the query results.
      for cluster_number, cluster_time_series in enumerate(time_series):
        utilization = [
            round(point.value.double_value, 3)
            for point in cluster_time_series.points]

        average_utilization = round(sum(utilization) / len(utilization), 3)
        metadata = {
            'cluster_number': cluster_number,
            'workload_index': workload_index,
            'cpu_utilization_per_minute': utilization,
            'cpu_average_utilization': average_utilization,
        }

        cpu_utilization_sample = sample.Sample(
            f'{metric}_array', -1, '', metadata)

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
  if _TABLE_OBJECT_SHARING.value:
    ycsb.SetYcsbTarUrl(YCSB_BIGTABLE_TABLE_SHARING_TAR_URL)

  instance: _Bigtable = benchmark_spec.non_relational_db
  args = [((vm, instance), {}) for vm in vms]
  vm_util.RunThreaded(_Install, args)

  vm = benchmark_spec.vms[0]
  splits = ','.join([
      f'user{1000 + i * (9999 - 1000) / hbase_ycsb.TABLE_SPLIT_COUNT}'
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
  vm.RemoteCommand(' '.join(command), should_log=True, ignore_failure=True)


def _GetYcsbExecutor(
    vms: list[virtual_machine.VirtualMachine]) -> ycsb.YCSBExecutor:
  """Gets the YCSB executor class for loading and running the benchmark."""
  # Add hbase conf dir to the classpath.
  ycsb_memory = min(vms[0].total_memory_kb // 1024, 4096)
  jvm_args = pipes.quote(f' -Xmx{ycsb_memory}m')

  executor_flags = {
      'cp': hbase.HBASE_CONF_DIR,
      'jvm-args': jvm_args,
      'table': _GetTableName()}

  return ycsb.YCSBExecutor(FLAGS.hbase_binding, **executor_flags)


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
  }
  metadata.update(instance.GetResourceMetadata())

  # By default YCSB uses a BufferedMutator for Puts / Deletes.
  # This leads to incorrect update latencies, since since the call returns
  # before the request is acked by the server.
  # Disable this behavior during the benchmark run.
  run_kwargs = {
      'columnfamily': COLUMN_FAMILY,
      'clientbuffering': 'false'}
  load_kwargs = run_kwargs.copy()

  # During the load stage, use a buffered mutator with a single thread.
  # The BufferedMutator will handle multiplexing RPCs.
  load_kwargs['clientbuffering'] = 'true'
  if not FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = 1

  samples = []
  executor: ycsb.YCSBExecutor = _GetYcsbExecutor(vms)
  if not instance.restored:
    samples += list(executor.Load(vms, load_kwargs=load_kwargs))
  samples += list(executor.Run(vms, run_kwargs=run_kwargs))

  # Optionally add new samples for cluster cpu utilization.
  if _GET_CPU_UTILIZATION.value:
    cpu_utilization_samples = _GetCpuUtilizationSample(samples, instance.name)
    samples.extend(cpu_utilization_samples)

  for current_sample in samples:
    current_sample.metadata.update(metadata)

  return samples


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
  vm.RemoteCommand(' '.join(command), should_log=True)


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  instance: _Bigtable = benchmark_spec.non_relational_db
  if (instance.user_managed and
      (_STATIC_TABLE_NAME.value is None or _DELETE_STATIC_TABLE.value)):
    # Only need to drop the temporary tables if we're not deleting the instance.
    _CleanupTable(benchmark_spec)
