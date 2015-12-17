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
  * Adds alpn-boot-7.0.0.v20140317.jar to the bootclasspath, required to
    operate.

This benchmark requires a Cloud Bigtable cluster to be provisioned before
running.
The benchmark will fail if the specified cluster is not found.
"""

import json
import logging
import os
import pipes
import posixpath
import subprocess

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import hbase_ycsb_benchmark \
    as hbase_ycsb
from perfkitbenchmarker.linux_packages import hbase
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS

flags.DEFINE_string('google_bigtable_endpoint', 'bigtable.googleapis.com',
                    'Google API endpoint for Cloud Bigtable.')
flags.DEFINE_string('google_bigtable_admin_endpoint',
                    'bigtabletableadmin.googleapis.com',
                    'Google API endpoint for Cloud Bigtable table '
                    'administration.')
flags.DEFINE_string('google_bigtable_zone_name', 'us-central1-b',
                    'Bigtable zone.')
flags.DEFINE_string('google_bigtable_cluster_name', None,
                    'Bigtable cluster name.')
flags.DEFINE_string(
    'google_bigtable_alpn_jar_url',
    'http://central.maven.org/maven2/org/mortbay/jetty/alpn/'
    'alpn-boot/7.1.3.v20150130/alpn-boot-7.1.3.v20150130.jar',
    'URL for the ALPN boot JAR, required for HTTP2')
flags.DEFINE_string(
    'google_bigtable_hbase_jar_url',
    'https://oss.sonatype.org/service/local/repositories/releases/content/'
    'com/google/cloud/bigtable/bigtable-hbase-1.0/'
    '0.2.1/bigtable-hbase-1.0-0.2.1.jar',
    'URL for the Bigtable-HBase client JAR.')

BENCHMARK_NAME = 'cloud_bigtable_ycsb'
BENCHMARK_CONFIG = """
cloud_bigtable_ycsb:
  description: >
      Run YCSB against an existing Cloud Bigtable
      cluster. Configure the number of client VMs via --num_vms.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

HBASE_SITE = 'cloudbigtable/hbase-site.xml.j2'
HBASE_CONF_FILES = [HBASE_SITE]
YCSB_HBASE_LIB = posixpath.join(ycsb.YCSB_DIR, 'hbase-binding', 'lib')
YCSB_HBASE_CONF = posixpath.join(ycsb.YCSB_DIR, 'hbase-binding', 'conf')

REQUIRED_SCOPES = (
    'https://www.googleapis.com/auth/bigtable.admin',
    'https://www.googleapis.com/auth/bigtable.data')

# TODO(connormccoy): Make table parameters configurable.
COLUMN_FAMILY = 'cf'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in HBASE_CONF_FILES:
    data.ResourcePath(resource)

  hbase.CheckPrerequisites()
  ycsb.CheckPrerequisites()

  for scope in REQUIRED_SCOPES:
    if scope not in FLAGS.gcloud_scopes:
      raise ValueError('Scope {0} required.'.format(scope))

  # TODO: extract from gcloud config if available.
  if not FLAGS.google_bigtable_cluster_name:
    raise ValueError('Missing --google_bigtable_cluster_name')
  if not FLAGS.google_bigtable_zone_name:
    raise ValueError('Missing --google_bigtable_zone_name')
  cluster = _GetClusterDescription(FLAGS.project or _GetDefaultProject(),
                                   FLAGS.google_bigtable_zone_name,
                                   FLAGS.google_bigtable_cluster_name)
  logging.info('Found cluster: %s', cluster)


def _GetALPNLocalPath():
  bn = os.path.basename(FLAGS.google_bigtable_alpn_jar_url)
  if not bn.endswith('.jar'):
    bn = 'alpn.jar'
  return posixpath.join(vm_util.VM_TMP_DIR, bn)


def _GetClusterDescription(project, zone, cluster_name):
  """Gets the description for a Cloud Bigtable cluster.

  Args:
    project: str. Name of the project in which the cluster was created.
    zone: str. Zone of the project in which the cluster was created.
    cluster_name: str. Cluster ID of the desired Bigtable cluster.

  Returns:
    A dictionary containing a cluster description.

  Raises:
    KeyError: when the cluster was not found.
  """
  env = {'CLOUDSDK_CORE_DISABLE_PROMPTS': '1'}
  env.update(os.environ)
  cmd = [FLAGS.gcloud_path, 'alpha', 'bigtable', 'clusters', 'list', '--quiet',
         '--format', 'json', '--project', project]
  stdout, stderr, returncode = vm_util.IssueCommand(cmd, env=env)
  if returncode:
    raise IOError('Command "{0}" failed:\nSTDOUT:\n{1}\nSTDERR:\n{2}'.format(
        ' '.join(cmd), stdout, stderr))
  result = json.loads(stdout)
  clusters = {cluster['name']: cluster for cluster in result['clusters']}
  expected_cluster_name = 'projects/{0}/zones/{1}/clusters/{2}'.format(
      project, zone, cluster_name)
  try:
    return clusters[expected_cluster_name]
  except KeyError:
    raise KeyError('Cluster {0} not found in {1}'.format(
        expected_cluster_name, list(clusters)))


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

  hbase_lib = posixpath.join(hbase.HBASE_DIR, 'lib')
  for url in [FLAGS.google_bigtable_hbase_jar_url]:
    jar_name = os.path.basename(url)
    jar_path = posixpath.join(YCSB_HBASE_LIB, jar_name)
    vm.RemoteCommand('curl -Lo {0} {1}'.format(jar_path, url))
    vm.RemoteCommand('cp {0} {1}'.format(jar_path, hbase_lib))

  vm.RemoteCommand('curl -Lo {0} {1}'.format(
      _GetALPNLocalPath(),
      FLAGS.google_bigtable_alpn_jar_url))
  vm.RemoteCommand(('echo "export JAVA_HOME=/usr\n'
                    'export HBASE_OPTS=-Xbootclasspath/p:{0}"'
                    ' >> {1}/hbase-env.sh').format(_GetALPNLocalPath(),
                                                   hbase.HBASE_CONF_DIR))

  context = {
      'google_bigtable_endpoint': FLAGS.google_bigtable_endpoint,
      'google_bigtable_admin_endpoint': FLAGS.google_bigtable_admin_endpoint,
      'project': FLAGS.project or _GetDefaultProject(),
      'cluster': FLAGS.google_bigtable_cluster_name,
      'zone': FLAGS.google_bigtable_zone_name,
  }

  for file_name in HBASE_CONF_FILES:
    file_path = data.ResourcePath(file_name)
    remote_path = posixpath.join(hbase.HBASE_CONF_DIR,
                                 os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud bigtable.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  vms = benchmark_spec.vms

  vm_util.RunThreaded(_Install, vms)

  # Create table
  hbase_ycsb.CreateYCSBTable(vms[0], table_name=_GetTableName(),
                             use_snappy=False, limit_filesize=False)


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms

  table_name = _GetTableName()

  # Add hbase conf dir to the classpath, ALPN to the bootclasspath.
  ycsb_memory = ycsb_memory = min(vms[0].total_memory_kb // 1024, 4096)
  jvm_args = pipes.quote('-Xmx{0}m -Xbootclasspath/p:{1}'.format(
      ycsb_memory, _GetALPNLocalPath()))

  executor_flags = {'cp': hbase.HBASE_CONF_DIR,
                    'jvm-args': jvm_args,
                    'table': table_name}

  executor = ycsb.YCSBExecutor('hbase-10', **executor_flags)
  cluster_info = _GetClusterDescription(FLAGS.project or _GetDefaultProject(),
                                        FLAGS.google_bigtable_zone_name,
                                        FLAGS.google_bigtable_cluster_name)

  metadata = {'ycsb_client_vms': len(vms),
              'bigtable_nodes': cluster_info.get('serveNodes')}

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
  samples = list(executor.LoadAndRun(vms,
                                     load_kwargs=load_kwargs,
                                     run_kwargs=run_kwargs))
  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  # Delete table
  command = ("""echo 'disable "{0}"; drop "{0}"; exit' | """
             """{1}/hbase shell""").format(_GetTableName(), hbase.HBASE_BIN)
  vm.RemoteCommand(command, should_log=True, ignore_failure=True)
