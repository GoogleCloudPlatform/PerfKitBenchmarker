# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing Ant installation and cleanup functions."""

import posixpath

from perfkitbenchmarker.linux_packages import INSTALL_DIR

ANT_TAR_URL = ('archive.apache.org/dist/ant/binaries/'
               'apache-ant-1.9.6-bin.tar.gz')

ANT_HOME_DIR = posixpath.join(INSTALL_DIR, 'ant')


def _Install(vm):
  """Installs the Ant package on the VM."""
  vm.Install('wget')
  vm.RemoteCommand('mkdir -p {0} && '
                   'cd {0} && '
                   'wget {1} && '
                   'tar -zxf apache-ant-1.9.6-bin.tar.gz && '
                   'ln -s {0}/apache-ant-1.9.6/ {2}'.format(
                       INSTALL_DIR, ANT_TAR_URL, ANT_HOME_DIR))


def YumInstall(vm):
  """Installs the Ant package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Ant package on the VM."""
  _Install(vm)
