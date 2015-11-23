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

from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages.ant import ANT_HOME_DIR


JNA_JAR_URL = ('https://maven.java.net/content/repositories/releases/'
               'net/java/dev/jna/jna/4.1.0/jna-4.1.0.jar')
CASSANDRA_GIT_REPRO = 'https://github.com/apache/cassandra.git'
CASSANDRA_VERSION = 'cassandra-2.1.10'
CASSANDRA_YAML_TEMPLATE = 'cassandra/cassandra.yaml.j2'
CASSANDRA_ENV_TEMPLATE = 'cassandra/cassandra-env.sh.j2'
CASSANDRA_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'cassandra')
CASSANDRA_PID = posixpath.join(CASSANDRA_DIR, 'cassandra.pid')
CASSANDRA_OUT = posixpath.join(CASSANDRA_DIR, 'cassandra.out')
CASSANDRA_ERR = posixpath.join(CASSANDRA_DIR, 'cassandra.err')
NODETOOL = posixpath.join(CASSANDRA_DIR, 'bin', 'nodetool')


# Number of times to attempt to start the cluster.
CLUSTER_START_TRIES = 10
CLUSTER_START_SLEEP = 120
# Time, in seconds, to sleep between node starts.
NODE_START_SLEEP = 30


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
  vm.Install('openjdk7')
  vm.Install('curl')
  vm.RemoteCommand(
      'cd {0}; git clone {1}; cd {2}; git checkout {3}; {4}/bin/ant'.format(
          vm_util.VM_TMP_DIR,
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


def Configure(vm, seed_vms):
  """Configure Cassandra on 'vm'.

  Args:
    vm: VirtualMachine. The VM to configure.
    seed_vms: List of VirtualMachine. The seed virtual machine(s).
  """
  context = {'ip_address': vm.internal_ip,
             'data_path': posixpath.join(vm.GetScratchDir(), 'cassandra'),
             'seeds': ','.join(vm.internal_ip for vm in seed_vms),
             'num_cpus': vm.num_cpus,
             'cluster_name': 'Test cluster'}

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
  vm.RemoteCommand(
      'nohup {0}/bin/cassandra -p "{1}" 1> {2} 2> {3} &'.format(
          CASSANDRA_DIR, CASSANDRA_PID, CASSANDRA_OUT, CASSANDRA_ERR))


def Stop(vm):
  """Stops Cassandra on 'vm'."""
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
  data_path = posixpath.join(vm.GetScratchDir(), 'cassandra')
  vm.RemoteCommand('rm -rf {0}'.format(data_path))


def _StartCassandraIfNotRunning(vm):
  """Starts Cassandra on 'vm' if not currently running."""
  if not IsRunning(vm):
    logging.info('Retrying starting cassandra on %s', vm)
    Start(vm)


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
  vm_count = len(vms) + 1

  # Cassandra setup
  logging.info('Starting seed VM %s', seed_vm)
  Start(seed_vm)
  logging.info('Waiting %ds for seed to start', NODE_START_SLEEP)
  time.sleep(NODE_START_SLEEP)
  for i in xrange(5):
    if not IsRunning(seed_vm):
      logging.warn('Seed %s: Cassandra not running yet (try %d). Waiting %ds.',
                   seed_vm, i, NODE_START_SLEEP)
      time.sleep(NODE_START_SLEEP)
    else:
      break
  else:
    raise ValueError('Cassandra failed to start on seed.')

  if vms:
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

  for i in xrange(CLUSTER_START_TRIES):
    vms_up = GetNumberOfNodesUp(seed_vm)
    if vms_up == vm_count:
      logging.info('All %d nodes up!', vm_count)
      break

    logging.warn('Try %d: only %s of %s up. Sleeping %ds', i, vms_up,
                 vm_count, NODE_START_SLEEP)
    vm_util.RunThreaded(_StartCassandraIfNotRunning, vms)
    time.sleep(NODE_START_SLEEP)
  else:
    raise IOError('Failed to start Cassandra cluster.')
