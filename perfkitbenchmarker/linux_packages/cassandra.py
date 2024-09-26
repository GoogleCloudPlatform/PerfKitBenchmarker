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

"""Installs/Configures Cassandra.

See 'perfkitbenchmarker/data/cassandra/' for configuration files used.

Cassandra homepage: http://cassandra.apache.org
"""


import logging
import os
import posixpath
import time
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages


JNA_JAR_URL = (
    'https://maven.java.net/content/repositories/releases/'
    'net/java/dev/jna/jna/4.1.0/jna-4.1.0.jar'
)
CASSANDRA_GIT_REPRO = 'https://github.com/apache/cassandra.git'
CASSANDRA_VERSION = 'cassandra-2.1'
CASSANDRA_YAML_TEMPLATE = 'cassandra/cassandra.yaml.j2'
CASSANDRA_RACKDC_TEMPLATE = 'cassandra/cassandra-rackdc.properties.j2'
CASSANDRA_KEYSPACE_TEMPLATE = (
    'cassandra/create-keyspace-cassandra-stress.cql.j2'
)
CASSANDRA_ROW_CACHE_TEMPLATE = 'cassandra/enable-row-caching.cql.j2'
CASSANDRA_VERSION = 'apache-cassandra-4.1.5'
CASSANDRA_DIR = posixpath.join(linux_packages.INSTALL_DIR, CASSANDRA_VERSION)
CASSANDRA_PID = posixpath.join(CASSANDRA_DIR, 'cassandra.pid')
CASSANDRA_OUT = posixpath.join(CASSANDRA_DIR, 'cassandra.out')
CASSANDRA_ERR = posixpath.join(CASSANDRA_DIR, 'cassandra.err')


# Number of times to attempt to start the cluster.
CLUSTER_START_TRIES = 10
CLUSTER_START_SLEEP = 30
# Time, in seconds, to sleep between node starts.
NODE_START_SLEEP = 30
# for setting a maven repo with --cassandra_maven_repo_url
_MAVEN_REPO_PARAMS = """
artifact.remoteRepository.central: {0}
artifact.remoteRepository.apache: {0}
"""

FLAGS = flags.FLAGS
flags.DEFINE_integer('cassandra_replication_factor', 3, 'Num of replicas.')
CASSANDRA_CONCURRENT_READS = flags.DEFINE_integer(
    'cassandra_concurrent_reads',
    32,
    'Concurrent read requests each server accepts.',
)
CASSANDRA_CONCURRENT_WRITES = flags.DEFINE_integer(
    'cassandra_concurrent_writes',
    None,
    'Concurrent write requests each server accepts. Suggested number is'
    ' Number of CPUs in the VM * 8',
)
# Partial list of known mirrors:
# https://repo.maven.apache.org/maven2/.meta/repository-metadata.xml
# See instructions for setting up own mirror:
# https://maven.apache.org/guides/mini/guide-mirror-settings.html
flags.DEFINE_boolean(
    'cassandra_maven_repo_url', None, 'Optional maven repo mirror to use.'
)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in (CASSANDRA_YAML_TEMPLATE, CASSANDRA_RACKDC_TEMPLATE):
    data.ResourcePath(resource)


def _Install(vm):
  """Installs Cassandra as a debian package.

  Args:
    vm: VirtualMachine. The VM to install Cassandra on.
  """
  vm.Install('openjdk')
  vm.Install('curl')
  vm.RemoteCommand(
      'curl -o /opt/pkb/cassandra.tar.gz'
      ' https://archive.apache.org/dist/cassandra/4.1.5/apache-cassandra-4.1.5-bin.tar.gz'
  )
  vm.RemoteCommand('tar xzvf /opt/pkb/cassandra.tar.gz --directory /opt/pkb')


def GetCassandraVersion(vm) -> str:
  """Returns the Cassandra version installed on the VM."""
  stdout, _ = vm.RemoteCommand(f'{GetCassandraPath()} -v')
  return stdout


def YumInstall(vm):
  """Installs Cassandra on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs Cassandra on the VM."""
  _Install(vm)


def JujuInstall(vm, vm_group_name):
  """Installs the Cassandra charm on the VM."""
  vm.JujuDeploy('cs:trusty/cassandra', vm_group_name)

  # The charm defaults to Cassandra 2.2.x, which has deprecated
  # cassandra-cli. Specify the sources to downgrade to Cassandra 2.1.x
  # to match the cassandra benchmark(s) expectations.
  sources = [
      'deb https://www.apache.org/dist/cassandra/debian 21x main',
      'ppa:openjdk-r/ppa',
      'ppa:stub/cassandra',
  ]

  keys = ['F758CE318D77295D', 'null', 'null']

  vm.JujuSet(
      'cassandra',
      [
          # Allow authentication from all units
          'authenticator=AllowAllAuthenticator',
          'install_sources="[%s]"'
          % ', '.join(["'" + x + "'" for x in sources]),
          'install_keys="[%s]"' % ', '.join(keys),
      ],
  )

  # Wait for cassandra to be installed and configured
  vm.JujuWait()

  for unit in vm.units:
    # Make sure the cassandra/conf dir is created, since we're skipping
    # the manual installation to /opt/pkb.
    remote_path = posixpath.join(CASSANDRA_DIR, 'conf')
    unit.RemoteCommand('mkdir -p %s' % remote_path)


def Configure(vm, seed_vms):
  """Configure Cassandra on 'vm'.

  Args:
    vm: VirtualMachine. The VM to configure.
    seed_vms: List of VirtualMachine. The seed virtual machine(s).
  """
  context = {
      'ip_address': vm.internal_ip,
      'data_path': posixpath.join(vm.GetScratchDir(), 'cassandra'),
      'seeds': ','.join(f'{vm.internal_ip}:7000' for vm in seed_vms),
      'num_cpus': vm.NumCpusForBenchmark(),
      'cluster_name': 'Test cluster',
      'concurrent_reads': CASSANDRA_CONCURRENT_READS.value,
      'concurrent_writes': (
          CASSANDRA_CONCURRENT_WRITES.value
          if CASSANDRA_CONCURRENT_WRITES.value
          else 8 * vm.NumCpusForBenchmark()
      ),
      'datacenter': 'datacenter',
      'rack': vm.zone,
      'row_cache_size': (
          f'{FLAGS.row_cache_size}MiB' if FLAGS.is_row_cache_enabled else '0MiB'
      ),
  }
  logging.info('cassandra yaml context: %s', context)
  for template in [CASSANDRA_YAML_TEMPLATE, CASSANDRA_RACKDC_TEMPLATE]:
    local_path = data.ResourcePath(template)
    cassandra_conf_path = posixpath.join(CASSANDRA_DIR, 'conf')
    remote_path = posixpath.join(
        '~', os.path.splitext(os.path.basename(template))[0]
    )
    vm.RenderTemplate(local_path, remote_path, context=context)
    vm.RemoteCommand(f'sudo cp {remote_path} {cassandra_conf_path}')
  vm.RemoteCommand(f'mkdir {vm.GetScratchDir()}/cassandra')
  vm.RemoteCommand(f'sudo chmod -R 755 {vm.GetScratchDir()}/cassandra')


def Start(vm):
  """Start Cassandra on a VM.

  Args:
    vm: The target vm. Should already be configured via 'Configure'.
  """
  vm.RemoteCommand(f'{GetCassandraPath()}')


def CreateKeyspace(vm, replication_factor):
  """Create a keyspace on a VM."""
  RunCql(vm, CASSANDRA_KEYSPACE_TEMPLATE, replication_factor)
  if FLAGS.is_row_cache_enabled:
    RunCql(vm, CASSANDRA_ROW_CACHE_TEMPLATE, replication_factor)


def RunCql(vm, template, replication_factor):
  """Run a CQL file on a VM."""
  cassandra_conf_path = posixpath.join(CASSANDRA_DIR, 'conf')
  template_path = data.ResourcePath(template)
  file_name = os.path.basename(os.path.splitext(template_path)[0])
  remote_path = os.path.join(
      '~',
      file_name,
  )
  vm.RenderTemplate(
      template_path,
      remote_path,
      context={
          'keyspace': 'keyspace1',
          'replication_factor': replication_factor,
      },
  )
  vm.RemoteCommand(f'sudo cp {remote_path} {cassandra_conf_path}')
  vm.RemoteCommand(
      f'cd {CASSANDRA_DIR}/bin && sudo ./cqlsh -f'
      f' {cassandra_conf_path}/{file_name}'
  )


def Stop(vm):
  """Stops Cassandra on 'vm'."""
  vm.RemoteCommand('kill $(cat {})'.format(CASSANDRA_PID), ignore_failure=True)


def IsRunning(vm):
  """Returns a boolean indicating whether Cassandra is running on 'vm'."""
  try:
    _, stderr = vm.RemoteCommand(f'{GetNodetoolPath()} status')
    if stderr:
      return False
    return True
  except errors.VirtualMachine.RemoteCommandError as ex:
    logging.warning('Exception: %s', ex)
    return False


def CleanNode(vm):
  """Remove Cassandra data from 'vm'.

  Args:
    vm: VirtualMachine. VM to clean.
  """
  data_path = posixpath.join(vm.GetScratchDir(), 'cassandra')
  vm.RemoteCommand('rm -rf {}'.format(data_path))


def _StartCassandraIfNotRunning(vm):
  """Starts Cassandra on 'vm' if not currently running."""
  if not IsRunning(vm):
    logging.info('Retrying starting cassandra on %s', vm)
    Start(vm)


def GetCassandraCliPath(_):
  return posixpath.join(CASSANDRA_DIR, 'bin', 'cassandra-cli')


def GetCassandraPath():
  return posixpath.join(CASSANDRA_DIR, 'bin', 'cassandra')


def GetNodetoolPath():
  return posixpath.join(CASSANDRA_DIR, 'bin', 'nodetool')


def GetCassandraStressPath(_):
  return posixpath.join(CASSANDRA_DIR, 'tools', 'bin', 'cassandra-stress')


def GetNumberOfNodesUp(vm):
  """Gets the number of VMs which are up in a Cassandra cluster.

  Args:
    vm: VirtualMachine. The VM to use to check the cluster status.

  Returns:
    int. The number of VMs which are up in a Cassandra cluster.
  """
  vms_up = vm.RemoteCommand(f'{GetNodetoolPath()} status | grep -c "^UN"')[
      0
  ].strip()
  return int(vms_up)


def StartCluster(seed_vm, vms):
  """Starts a Cassandra cluster.

  Starts a Cassandra cluster, first starting 'seed_vm', then remaining VMs in
  'vms'.

  Args:
    seed_vm: VirtualMachine. Machine which will function as the sole seed. It
      will be started before all other VMs.
    vms: list of VirtualMachines. VMs *other than* seed_vm which should be
      started.

  Raises:
    OSError: if cluster startup fails.
  """
  vm_count = len(vms) + 1

  # Cassandra setup
  logging.info('Starting seed VM %s', seed_vm)
  Start(seed_vm)
  logging.info('Waiting %ds for seed to start', NODE_START_SLEEP)
  time.sleep(NODE_START_SLEEP)
  for i in range(5):
    if not IsRunning(seed_vm):
      logging.warn(
          'Seed %s: Cassandra not running yet (try %d). Waiting %ds.',
          seed_vm,
          i,
          NODE_START_SLEEP,
      )
      time.sleep(NODE_START_SLEEP)
    else:
      break
  else:
    raise ValueError('Cassandra failed to start on seed.')

  if vms:
    logging.info('Starting remaining %d nodes', len(vms))
    # Start the VMs with a small pause in between each, to allow the node to
    # join.
    # Starting Cassandra nodes fails when multiple nodes attempt to join the
    # cluster concurrently.
    for i, vm in enumerate(vms):
      logging.info('Starting non-seed VM %d/%d.', i + 1, len(vms))
      Start(vm)
      time.sleep(NODE_START_SLEEP)
    logging.info('Waiting %ds for nodes to join', CLUSTER_START_SLEEP)
    time.sleep(CLUSTER_START_SLEEP)
  for i in range(CLUSTER_START_TRIES):
    vms_up = GetNumberOfNodesUp(seed_vm)
    if vms_up == vm_count:
      logging.info('All %d nodes up!', vm_count)
      break

    logging.warn(
        'Try %d: only %s of %s up. Restarting and sleeping %ds',
        i,
        vms_up,
        vm_count,
        NODE_START_SLEEP,
    )
    background_tasks.RunThreaded(_StartCassandraIfNotRunning, vms)
    time.sleep(NODE_START_SLEEP)
  else:
    raise OSError('Failed to start Cassandra cluster.')
