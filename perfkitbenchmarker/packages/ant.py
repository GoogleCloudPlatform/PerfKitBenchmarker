# Copyright 2014 Google Inc. All rights reserved.
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

from perfkitbenchmarker import vm_util

ANT_TAR_URL = ('www.pirbot.com/mirrors/apache//ant/binaries/'
               'apache-ant-1.9.6-bin.tar.gz')


def _Install(vm):
    """Installs the Ant package on the VM."""
    vm.Install('wget')
    vm.RemoteCommand('cd {0} && '
                     'wget {1} && '
                     'sudo tar -C /opt/ -zxf apache-ant-1.9.6-bin.tar.gz && '
                     'sudo ln -s /opt/apache-ant-1.9.6/ /opt/ant && '
                     'test -e /usr/bin/ant && '
                     'sudo mv /usr/bin/ant /usr/bin/ant.bak'.format(
                         vm_util.VM_TMP_DIR, ANT_TAR_URL))
    vm.RemoteCommand('sudo ln -s /opt/ant/bin/ant /usr/bin/ant && '
                     'sudo sh -c "echo ANT_HOME=/opt/ant >> /etc/environment"')



def YumInstall(vm):
  """Installs the Ant package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Ant package on the VM."""
  vm.InstallPackages('ant')


def YumUninstall(vm):
  """Uninstalls the Ant package on the VM."""
  vm.RemoteCommand('test -e /usr/bin/ant.bak && '
                   'sudo rm -f /usr/bin/ant && '
                   'sudo mv /usr/bin/ant.bak /usr/bin/ant')
  vm.RemoteCommand('sudo rm -f /opt/ant && '
                   'sudo rm -Rf /opt/apache-ant-1.9.6/ && '
                   'sudo sed -i "/ANT_HOME=\/opt\/ant/d" /etc/environment')
