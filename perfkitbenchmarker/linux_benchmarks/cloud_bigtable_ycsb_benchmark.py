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
  * Adds netty-tcnative-boringssl, used for communication with Bigtable.
"""

import datetime
import json
import logging
import os
import pipes
import posixpath
import subprocess

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import hbase_ycsb_benchmark as hbase_ycsb
from perfkitbenchmarker.linux_packages import hbase
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_bigtable

FLAGS = flags.FLAGS

HBASE_CLIENT_VERSION = '1.x'
BIGTABLE_CLIENT_VERSION = '1.4.0'

flags.DEFINE_string('google_bigtable_endpoint', 'bigtable.googleapis.com',
                    'Google API endpoint for Cloud Bigtable.')
flags.DEFINE_string('google_bigtable_admin_endpoint',
                    'bigtableadmin.googleapis.com',
                    'Google API endpoint for Cloud Bigtable table '
                    'administration.')
flags.DEFINE_string('google_bigtable_instance_name', None,
                    'Bigtable instance name. If not specified, new instance '
                    'will be created and deleted on the fly.')
flags.DEFINE_string(
    'google_bigtable_hbase_jar_url',
    'https://oss.sonatype.org/service/local/repositories/releases/content/'
    'com/google/cloud/bigtable/bigtable-hbase-{0}-hadoop/'
    '{1}/bigtable-hbase-{0}-hadoop-{1}.jar'.format(
        HBASE_CLIENT_VERSION,
        BIGTABLE_CLIENT_VERSION),
    'URL for the Bigtable-HBase client JAR.')
flags.DEFINE_boolean('get_bigtable_cluster_cpu_utilization', False,
                     'If true, will gather bigtable cluster cpu utilization '
                     'for the duration of performance test run stage, and add '
                     'a sample for the data. To enable this '
                     'functionality, need to set environment variable '
                     'GOOGLE_APPLICATION_CREDENTIALS as described in '
                     'https://cloud.google.com/docs/authentication/'
                     'getting-started.')

BENCHMARK_NAME = 'cloud_bigtable_ycsb'
BENCHMARK_CONFIG = """
cloud_bigtable_ycsb:
  description: >
      Run YCSB against an existing Cloud Bigtable
      instance. Configure the number of client VMs via --num_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
  flags:
    gcloud_scopes: >
      https://www.googleapis.com/auth/bigtable.admin
      https://www.googleapis.com/auth/bigtable.data"""

# Starting from version 1.4.0, there is no need to install a separate boring ssl
# via TCNATIVE_BORINGSSL_URL.
TCNATIVE_BORINGSSL_JAR = (
    'netty-tcnative-boringssl-static-1.1.33.Fork13-linux-x86_64.jar')
TCNATIVE_BORINGSSL_URL = posixpath.join(
    'https://search.maven.org/remotecontent?filepath='
    'io/netty/netty-tcnative-boringssl-static/'
    '1.1.33.Fork13/', TCNATIVE_BORINGSSL_JAR)
METRICS_CORE_JAR = 'metrics-core-3.1.2.jar'
DROPWIZARD_METRICS_CORE_URL = posixpath.join(
    'https://search.maven.org/remotecontent?filepath='
    'io/dropwizard/metrics/metrics-core/3.1.2/', METRICS_CORE_JAR)
HBASE_SITE = 'cloudbigtable/hbase-site.xml.j2'
HBASE_CONF_FILES = [HBASE_SITE]
HBASE_BINDING = 'hbase10-binding'
YCSB_HBASE_LIB = posixpath.join(ycsb.YCSB_DIR, HBASE_BINDING, 'lib')
YCSB_HBASE_CONF = posixpath.join(ycsb.YCSB_DIR, HBASE_BINDING, 'conf')

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/bigtable.admin',
    'https://www.googleapis.com/auth/bigtable.data')

# TODO(connormccoy): Make table parameters configurable.
COLUMN_FAMILY = 'cf'
BENCHMARK_DATA = {
    METRICS_CORE_JAR:
        '245ba2a66a9bc710ce4db14711126e77bcb4e6d96ef7e622659280f3c90cbb5c',
    TCNATIVE_BORINGSSL_JAR:
        '027d87e77a08dedf2005d9333db49aa37e08d599aff64ea18da9893912bdf314'
}
BENCHMARK_DATA_URL = {
    METRICS_CORE_JAR: DROPWIZARD_METRICS_CORE_URL,
    TCNATIVE_BORINGSSL_JAR: TCNATIVE_BORINGSSL_URL
}


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
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

  # TODO: extract from gcloud config if available.
  if FLAGS.google_bigtable_instance_name:
    instance = _GetInstanceDescription(FLAGS.project or _GetDefaultProject(),
                                       FLAGS.google_bigtable_instance_name)
    logging.info('Found instance: %s', instance)
  else:
    logging.info('No instance; will create in Prepare.')


def _GetInstanceDescription(project, instance_name):
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
  # List clusters and get the cluster associated with this instance so we can
  # read the number of nodes in the cluster.
  cmd = [FLAGS.gcloud_path, 'beta', 'bigtable', 'clusters', 'list', '--quiet',
         '--format', 'json', '--project', project]
  stdout, stderr, returncode = vm_util.IssueCommand(cmd, env=env)
  if returncode:
    raise IOError('Command "{0}" failed:\nSTDOUT:\n{1}\nSTDERR:\n{2}'.format(
        ' '.join(cmd), stdout, stderr))
  result = json.loads(stdout)
  instances = {
      instance['name'].split('/')[3]: instance for instance in result
  }
  try:
    return instances[instance_name]
  except KeyError:
    raise KeyError('Instance {0} not found in {1}'.format(
        instance_name, list(instances)))


def _GetTableName():
  return 'ycsb{0}'.format(FLAGS.run_uri)


def _GetDefaultProject():
  cmd = [FLAGS.gcloud_path, 'config', 'list', '--format', 'json']
  stdout, stderr, return_code = vm_util.IssueCommand(cmd)
  if return_code:
    raise subprocess.CalledProcessError(return_code, cmd, stdout)

  config = json.loads(stdout)

  try:
    return config['core']['project']
  except KeyError:
    raise KeyError('No default project found in {0}'.format(config))


def _Install(vm):
  """Install YCSB and HBase on 'vm'."""
  vm.Install('hbase')
  vm.Install('ycsb')
  vm.Install('curl')

  instance_name = (FLAGS.google_bigtable_instance_name or
                   'pkb-bigtable-{0}'.format(FLAGS.run_uri))
  hbase_lib = posixpath.join(hbase.HBASE_DIR, 'lib')

  preprovisioned_pkgs = [TCNATIVE_BORINGSSL_JAR]
  if 'hbase-1.x' in FLAGS.google_bigtable_hbase_jar_url:
    preprovisioned_pkgs.append(METRICS_CORE_JAR)
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, preprovisioned_pkgs, YCSB_HBASE_LIB)
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, preprovisioned_pkgs, hbase_lib)

  url = FLAGS.google_bigtable_hbase_jar_url
  jar_name = os.path.basename(url)
  jar_path = posixpath.join(YCSB_HBASE_LIB, jar_name)
  vm.RemoteCommand('curl -Lo {0} {1}'.format(jar_path, url))
  vm.RemoteCommand('cp {0} {1}'.format(jar_path, hbase_lib))

  vm.RemoteCommand('echo "export JAVA_HOME=/usr" >> {0}/hbase-env.sh'.format(
      hbase.HBASE_CONF_DIR))

  context = {
      'google_bigtable_endpoint': FLAGS.google_bigtable_endpoint,
      'google_bigtable_admin_endpoint': FLAGS.google_bigtable_admin_endpoint,
      'project': FLAGS.project or _GetDefaultProject(),
      'instance': instance_name,
      'hbase_version': HBASE_CLIENT_VERSION.replace('.', '_')
  }

  for file_name in HBASE_CONF_FILES:
    file_path = data.ResourcePath(file_name)
    remote_path = posixpath.join(hbase.HBASE_CONF_DIR,
                                 os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def MaxWithDefault(iterable, key, default):
  """Equivalent to max on python 3.4 or later."""
  try:
    return max(iterable, key=key)
  except ValueError:
    return default


def _GetCpuUtilizationSample(samples, instance_id):
  """Gets a list of cpu utilization samples - one per cluster.

  Note that the utilization only covers the run stage.

  Args:
    samples: list of sample.Sample. Used to find the load and run samples for
             computing the run time.
    instance_id: the bigtable instance id.

  Returns:
    a sample describing the runtime

  Raises:
    Exception:  if the time for running can not be found or if
                querying the cpu sampling fails.
  """
  load_sample = MaxWithDefault(
      (cur_sample for cur_sample in samples
       if cur_sample.metadata.get('stage') == 'load'),
      key=lambda sample: sample.timestamp,
      default=None)

  # get the last sample recorded in the run stage
  last_run_sample = MaxWithDefault(
      (cur_sample for cur_sample in samples
       if cur_sample.metadata.get('stage') == 'run'),
      key=lambda sample: sample.timestamp,
      default=None)

  if not load_sample or not last_run_sample:
    raise Exception('Could not find the load or run sample, '
                    'so cant get the time for cpu utilization')

  # pylint: disable=g-import-not-at-top
  from google.cloud import monitoring_v3
  from google.cloud.monitoring_v3 import query

  # Query the cpu utilization, which are gauged values at each minute in the
  # time window.
  client = monitoring_v3.MetricServiceClient()
  start_timestamp = load_sample.timestamp
  end_timestamp = last_run_sample.timestamp
  cpu_query = query.Query(
      client, project=(FLAGS.project or _GetDefaultProject()),
      metric_type='bigtable.googleapis.com/cluster/cpu_load_hottest_node',
      end_time=datetime.datetime.utcfromtimestamp(end_timestamp),
      minutes=int((end_timestamp - start_timestamp) / 60))
  cpu_query = cpu_query.select_resources(instance=instance_id)
  time_series = list(cpu_query)
  if not time_series:
    raise Exception('Time series could not be found '
                    'for computing cpu utilization.')

  # Build the dict to be added to samples.
  samples = []
  for cluster_number, cluster_time_series in enumerate(time_series):
    utilization = [
        round(point.value.double_value, 3)
        for point in cluster_time_series.points]

    metadata = {
        'cluster_number': cluster_number,
        'cpu_utilization_per_minute': utilization,
    }

    cpu_utilization_sample = sample.Sample(
        'cpu_load_hottest_node_array', -1, 'cpu load hottest node', metadata)

    samples.append(cpu_utilization_sample)
  return samples


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud bigtable.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  vms = benchmark_spec.vms

  # TODO: in the future, it might be nice to change this so that
  # a gcp_bigtable.GcpBigtableInstance can be created with an
  # flag that says don't create/delete the instance.  That would
  # reduce the code paths here.
  if FLAGS.google_bigtable_instance_name is None:
    instance_name = 'pkb-bigtable-{0}'.format(FLAGS.run_uri)
    project = FLAGS.project or _GetDefaultProject()
    logging.info('Creating bigtable instance %s', instance_name)
    zone = FLAGS.google_bigtable_zone
    benchmark_spec.bigtable_instance = gcp_bigtable.GcpBigtableInstance(
        instance_name, project, zone)
    benchmark_spec.bigtable_instance.Create()
    instance = _GetInstanceDescription(project, instance_name)
    logging.info('Instance %s created successfully', instance)

  vm_util.RunThreaded(_Install, vms)

  # Create table
  hbase_ycsb.CreateYCSBTable(vms[0], table_name=_GetTableName(),
                             use_snappy=False, limit_filesize=False)

  table_name = _GetTableName()

  # Add hbase conf dir to the classpath.
  ycsb_memory = min(vms[0].total_memory_kb // 1024, 4096)
  jvm_args = pipes.quote(' -Xmx{0}m'.format(ycsb_memory))

  executor_flags = {
      'cp': hbase.HBASE_CONF_DIR,
      'jvm-args': jvm_args,
      'table': table_name}

  benchmark_spec.executor = ycsb.YCSBExecutor('hbase10', **executor_flags)


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms

  instance_name = (FLAGS.google_bigtable_instance_name or
                   'pkb-bigtable-{0}'.format(FLAGS.run_uri))
  instance_info = _GetInstanceDescription(
      FLAGS.project or _GetDefaultProject(), instance_name)

  metadata = {
      'ycsb_client_vms': len(vms),
      'bigtable_zone': instance_info.get('location').split('/')[-1],
      'bigtable_storage_type': instance_info.get('defaultStorageType'),
      'bigtable_node_count': instance_info.get('serveNodes')
  }

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
  samples = list(benchmark_spec.executor.LoadAndRun(
      vms, load_kwargs=load_kwargs, run_kwargs=run_kwargs))

  # Optionally add new samples for cluster cpu utilization.
  if FLAGS.get_bigtable_cluster_cpu_utilization:
    cpu_utilization_samples = _GetCpuUtilizationSample(samples, instance_name)
    samples.extend(cpu_utilization_samples)

  for current_sample in samples:
    current_sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  # Delete table
  if FLAGS.google_bigtable_instance_name is None:
    benchmark_spec.bigtable_instance.Delete()
  else:
    # Only need to drop the tables if we're not deleting the instance.
    vm = benchmark_spec.vms[0]
    command = ("""echo 'disable "{0}"; drop "{0}"; exit' | """
               """{1}/hbase shell""").format(_GetTableName(), hbase.HBASE_BIN)
    vm.RemoteCommand(command, should_log=True, ignore_failure=True)
