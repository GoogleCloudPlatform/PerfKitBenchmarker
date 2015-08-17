# Copyright 2015 Google Inc. All rights reserved.
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

import posixpath

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages.openjdk7 import JAVA_HOME

FABAN_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'faban')

FABAN_TAR_URL = ('faban.org/downloads/faban-kit-latest.tar.gz')

FABAN_PORT = 9980


def _Install(vm):
  """Installs the Faban on the VM."""
  vm.Install('openjdk7')
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


def Start(vm, fw):
  """Allows the port on the VM, and starts the Faban using that port"""
  fw.AllowPort(vm, FABAN_PORT)
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
