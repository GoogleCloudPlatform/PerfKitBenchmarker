# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing Apache Nutch 1.10 installation and cleanup functions."""

import time
import posixpath

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages.openjdk import JAVA_HOME

FABAN_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'faban')

FABAN_TAR_URL = ('faban.org/downloads/faban-kit-latest.tar.gz')

FABAN_PORT = 9980


def _Install(vm):
  """Installs the Faban on the VM."""
  vm.Install('openjdk')
  vm.Install('ant')
  vm.RemoteCommand('cd {0} && '
                   'wget {2} && '
                   'tar -xzf faban-kit-latest.tar.gz'.format(
                       vm_util.VM_TMP_DIR, FABAN_HOME_DIR, FABAN_TAR_URL))


def YumInstall(vm):
  """Installs the Faban on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Faban on the VM."""
  _Install(vm)


def Start(vm):
  """Allows the port on the VM, and starts the Faban using that port"""
  vm.AllowPort(FABAN_PORT)
  vm.RemoteCommand('cd {0} && '
                   'export JAVA_HOME={1} && '
                   'master/bin/startup.sh'.format(
                       FABAN_HOME_DIR, JAVA_HOME))


def Stop(vm):
  """Stops the Faban on the VM."""
  vm.RemoteCommand('cd {0} && '
                   'export JAVA_HOME={1} && '
                   'master/bin/shutdown.sh'.format(
                       FABAN_HOME_DIR, JAVA_HOME))


def StartRegistry(vm, classpath, policy_path):
  vm.RobustRemoteCommand('export FABAN_HOME={2} && '
                         'java -classpath {1} -Djava.security.policy={0} '
                         'com.sun.faban.common.RegistryImpl &'.format(
                             policy_path, classpath, FABAN_HOME_DIR))
  time.sleep(3)


def StopRegistry(vm):
  vm.RemoteCommand('kill -9 `jps | grep RegistryImpl | cut -d " " -f 1`')


def StartAgent(vm, classpath, driver_dir, driver_class, agent_id,
               java_heap_size, policy_path, faban_master):
  """Runs Registry on faban client."""
  vm.RobustRemoteCommand('export FABAN_HOME={6} && '
                         'java -classpath {5} -Xmx{0} -Xms{0} '
                         '-Djava.security.policy={1} '
                         'com.sun.faban.driver.engine.AgentImpl'
                         ' {2} {3} {4} &'.format(
                             java_heap_size, policy_path, driver_class,
                             agent_id, faban_master, classpath, FABAN_HOME_DIR))
  time.sleep(3)


def StartMaster(vm, classpath, java_heap_size, policy_path, benchmark_config):
  vm.RemoteCommand('export FABAN_HOME={4} && '
                   'java -classpath {3} -Xmx{0} -Xms{0} '
                   '-Djava.security.policy={1} -Dbenchmark.config={2} '
                   'com.sun.faban.driver.engine.MasterImpl'.format(
                       java_heap_size, policy_path, benchmark_config,
                       classpath, FABAN_HOME_DIR))
