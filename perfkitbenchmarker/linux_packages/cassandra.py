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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import posixpath
import time

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker.linux_packages.ant import ANT_HOME_DIR
from six.moves import range


JNA_JAR_URL = ('https://maven.java.net/content/repositories/releases/'
               'net/java/dev/jna/jna/4.1.0/jna-4.1.0.jar')
CASSANDRA_GIT_REPRO = 'https://github.com/apache/cassandra.git'
CASSANDRA_VERSION = 'cassandra-2.1'
CASSANDRA_YAML_TEMPLATE = 'cassandra/cassandra.yaml.j2'
CASSANDRA_ENV_TEMPLATE = 'cassandra/cassandra-env.sh.j2'
CASSANDRA_DIR = posixpath.join(INSTALL_DIR, 'cassandra')
CASSANDRA_PID = posixpath.join(CASSANDRA_DIR, 'cassandra.pid')
CASSANDRA_OUT = posixpath.join(CASSANDRA_DIR, 'cassandra.out')
CASSANDRA_ERR = posixpath.join(CASSANDRA_DIR, 'cassandra.err')
NODETOOL = posixpath.join(CASSANDRA_DIR, 'bin', 'nodetool')


# Number of times to attempt to start the cluster.
CLUSTER_START_TRIES = 10
CLUSTER_START_SLEEP = 60
# Time, in seconds, to sleep between node starts.
NODE_START_SLEEP = 5

FLAGS = flags.FLAGS
flags.DEFINE_integer('cassandra_replication_factor', 3, 'Num of replicas.')
flags.DEFINE_integer('cassandra_concurrent_reads', 32,
                     'Concurrent read requests each server accepts.')


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in (CASSANDRA_YAML_TEMPLATE,
                   CASSANDRA_ENV_TEMPLATE):
    data.ResourcePath(resource)


def _Install(vm):
  """Installs Cassandra from a tarball."""
  vm.Install('ant')
  vm.Install('build_tools')
  vm.Install('openjdk')
  vm.Install('curl')
  vm.RemoteCommand(
      'cd {0}; git clone {1}; cd {2}; git checkout {3}; {4}/bin/ant'.format(
          INSTALL_DIR,
          CASSANDRA_GIT_REPRO,
          CASSANDRA_DIR,
          CASSANDRA_VERSION,
          ANT_HOME_DIR))
  # Add JNA
  vm.RemoteCommand('cd {0} && curl -LJO {1}'.format(
      posixpath.join(CASSANDRA_DIR, 'lib'),
      JNA_JAR_URL))


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
  sources = ['deb https://www.apache.org/dist/cassandra/debian 21x main',
             'ppa:openjdk-r/ppa',
             'ppa:stub/cassandra']

  keys = ['F758CE318D77295D',
          'null',
          'null']

  vm.JujuSet(
      'cassandra',
      [
          # Allow authentication from all units
          'authenticator=AllowAllAuthenticator',
          'install_sources="[%s]"' %
          ', '.join(["'" + x + "'" for x in sources]),
          'install_keys="[%s]"' % ', '.join(keys)
      ])

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
  context = {'ip_address': vm.internal_ip,
             'data_path': posixpath.join(vm.GetScratchDir(), 'cassandra'),
             'seeds': ','.join(vm.internal_ip for vm in seed_vms),
             'num_cpus': vm.NumCpusForBenchmark(),
             'cluster_name': 'Test cluster',
             'concurrent_reads': FLAGS.cassandra_concurrent_reads}

  for config_file in [CASSANDRA_ENV_TEMPLATE, CASSANDRA_YAML_TEMPLATE]:
    local_path = data.ResourcePath(config_file)
    remote_path = posixpath.join(
        CASSANDRA_DIR, 'conf',
        os.path.splitext(os.path.basename(config_file))[0])
    vm.RenderTemplate(local_path, remote_path, context=context)


def Start(vm):
  """Start Cassandra on a VM.

  Args:
    vm: The target vm. Should already be configured via 'Configure'.
  """
  if vm.OS_TYPE == os_types.JUJU:
    return

  vm.RemoteCommand(
      'nohup {0}/bin/cassandra -p "{1}" 1> {2} 2> {3} &'.format(
          CASSANDRA_DIR, CASSANDRA_PID, CASSANDRA_OUT, CASSANDRA_ERR))


def Stop(vm):
  """Stops Cassandra on 'vm'."""
  if vm.OS_TYPE == os_types.JUJU:
    return

  vm.RemoteCommand('kill $(cat {0})'.format(CASSANDRA_PID),
                   ignore_failure=True)


def IsRunning(vm):
  """Returns a boolean indicating whether Cassandra is running on 'vm'."""
  cassandra_pid = vm.RemoteCommand(
      'cat {0} || true'.format(CASSANDRA_PID))[0].strip()
  if not cassandra_pid:
    return False

  try:
    vm.RemoteCommand('kill -0 {0}'.format(cassandra_pid))
    return True
  except errors.VirtualMachine.RemoteCommandError:
    logging.warn('%s: Cassandra is not running. '
                 'Startup STDOUT:\n%s\n\nSTDERR:\n%s',
                 vm,
                 vm.RemoteCommand('cat ' + CASSANDRA_OUT),
                 vm.RemoteCommand('cat ' + CASSANDRA_ERR))
    return False


def CleanNode(vm):
  """Remove Cassandra data from 'vm'.

  Args:
    vm: VirtualMachine. VM to clean.
  """
  if vm.OS_TYPE == os_types.JUJU:
    return

  data_path = posixpath.join(vm.GetScratchDir(), 'cassandra')
  vm.RemoteCommand('rm -rf {0}'.format(data_path))


def _StartCassandraIfNotRunning(vm):
  """Starts Cassandra on 'vm' if not currently running."""
  if not IsRunning(vm):
    logging.info('Retrying starting cassandra on %s', vm)
    Start(vm)


def GetCassandraCliPath(vm):
  if vm.OS_TYPE == os_types.JUJU:
    # Replace the stock CASSANDRA_CLI so that it uses the binary
    # installed by the cassandra charm.
    return '/usr/bin/cassandra-cli'

  return posixpath.join(CASSANDRA_DIR, 'bin',
                        'cassandra-cli')


def GetCassandraStressPath(vm):
  if vm.OS_TYPE == os_types.JUJU:
    # Replace the stock CASSANDRA_STRESS so that it uses the binary
    # installed by the cassandra-stress charm.
    return '/usr/bin/cassandra-stress'

  return posixpath.join(CASSANDRA_DIR, 'tools', 'bin',
                        'cassandra-stress')


def GetNumberOfNodesUp(vm):
  """Gets the number of VMs which are up in a Cassandra cluster.

  Args:
    vm: VirtualMachine. The VM to use to check the cluster status.
  """
  vms_up = vm.RemoteCommand(
      '{0} status | grep -c "^UN"'.format(NODETOOL))[0].strip()
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
  """

  if seed_vm.OS_TYPE == os_types.JUJU:
    # Juju automatically configures and starts the Cassandra cluster.
    return

  vm_count = len(vms) + 1

  # Cassandra setup
  logging.info('Starting seed VM %s', seed_vm)
  Start(seed_vm)
  logging.info('Waiting %ds for seed to start', NODE_START_SLEEP)
  time.sleep(NODE_START_SLEEP)
  for i in range(5):
    if not IsRunning(seed_vm):
      logging.warn('Seed %s: Cassandra not running yet (try %d). Waiting %ds.',
                   seed_vm, i, NODE_START_SLEEP)
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
      time.sleep(NODE_START_SLEEP)
      logging.info('Starting non-seed VM %d/%d.', i + 1, len(vms))
      Start(vm)
    logging.info('Waiting %ds for nodes to join', CLUSTER_START_SLEEP)
    time.sleep(CLUSTER_START_SLEEP)

  for i in range(CLUSTER_START_TRIES):
    vms_up = GetNumberOfNodesUp(seed_vm)
    if vms_up == vm_count:
      logging.info('All %d nodes up!', vm_count)
      break

    logging.warn('Try %d: only %s of %s up. Restarting and sleeping %ds', i,
                 vms_up, vm_count, NODE_START_SLEEP)
    vm_util.RunThreaded(_StartCassandraIfNotRunning, vms)
    time.sleep(NODE_START_SLEEP)
  else:
    raise IOError('Failed to start Cassandra cluster.')
