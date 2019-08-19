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


"""Module containing mongodb installation and cleanup functions."""


def _GetServiceName():
  """Returns the name of the mongodb service."""
  return 'mongod'


def _GetConfigPath():
  """Returns the path to the mongodb config file."""
  return '/etc/mongod.conf'


def _Setup(vm):
  """Setup mongodb."""
  vm.RemoteCommand(
      'sudo sed -i "s|bindIp|# bindIp|" {}'.format(_GetConfigPath()))


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  vm.RemoteCommand('sudo setenforce 0')
  releasever, _ = vm.RemoteCommand(
      'distro=$(sed -n \'s/^distroverpkg=//p\' /etc/yum.conf);'
      'echo $(rpm -q --qf "%{version}" -f /etc/$distro)')
  mongodb_repo = (
      '[mongodb-org-3.0]\nname=MongoDB Repository\nbaseurl='
      'https://repo.mongodb.org/yum/redhat/{0}/mongodb-org/3.0/x86_64/'
      '\ngpgcheck=0\nenabled=1').format(releasever.strip())
  vm.RemoteCommand(
      'echo "%s" | sudo tee /etc/yum.repos.d/mongodb-org-3.0.repo' %
      mongodb_repo)
  vm.InstallPackages('mongodb-org')
  _Setup(vm)


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  vm.RemoteCommand(
      'wget -qO - https://www.mongodb.org/static/pgp/server-3.0.asc'
      ' | sudo apt-key add -')
  vm.RemoteCommand(
      'echo "deb https://repo.mongodb.org/apt/ubuntu '
      '$(lsb_release -c -s)/mongodb-org/3.0 multiverse" | '
      'sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list')
  vm.AptUpdate()
  vm.RemoteCommand('sudo apt-get install mongodb-org -y --force-yes')
  _Setup(vm)


def YumUninstall(vm):
  """Uninstalls the mongodb package on the VM."""
  vm.RemoteCommand('sudo rm /etc/yum.repos.d/mongodb-org-3.0.repo')


def YumGetServiceName(vm):
  """Returns the name of the mongodb service."""
  del vm
  return _GetServiceName()


def AptGetServiceName(vm):
  """Returns the name of the mongodb service."""
  del vm
  return _GetServiceName()


def YumGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  del vm
  return _GetConfigPath()


def AptGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  del vm
  return _GetConfigPath()
