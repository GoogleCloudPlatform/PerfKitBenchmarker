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

from absl import flags

FLAGS = flags.FLAGS

VERSION = flags.DEFINE_string(
    'mongodb_version', '7.0', 'Version of mongodb package.'
)


def _GetServiceName():
  """Returns the name of the mongodb service."""
  return 'mongod'


def _GetConfigPath():
  """Returns the path to the mongodb config file."""
  return '/etc/mongod.conf'


def _Setup(vm):
  """Setup mongodb."""
  vm.RemoteCommand(
      'sudo sed -i "s|bindIp|# bindIp|" {}'.format(_GetConfigPath())
  )


def YumSetup(vm):
  """Performs common pre-install setup for mongodb .rpm packages."""
  vm.RemoteCommand('sudo setenforce 0')
  releasever, _ = vm.RemoteCommand(
      "distro=$(sed -n 's/^distroverpkg=//p' /etc/yum.conf);"
      'echo $(rpm -q --qf "%{version}" -f /etc/$distro)'
  )
  mongodb_repo = (
      f'[mongodb-org-{VERSION.value}]\nname=MongoDB Repository\nbaseurl='
      f'https://repo.mongodb.org/yum/redhat/{releasever.strip()}/mongodb-org/{VERSION.value}/x86_64/'
      '\ngpgcheck=0\nenabled=1'
  )
  vm.RemoteCommand(
      f'echo "{mongodb_repo}" | sudo tee'
      f' /etc/yum.repos.d/mongodb-org-{VERSION.value}.repo'
  )


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  YumSetup(vm)
  vm.InstallPackages('mongodb-org')
  _Setup(vm)


def AptSetup(vm):
  """Performs common pre-install setup for mongodb .deb packages."""
  vm.InstallPackages('gnupg curl')
  vm.RemoteCommand(
      f'sudo rm -rf /usr/share/keyrings/mongodb-server-{VERSION.value}.gpg'
  )
  vm.RemoteCommand(
      f'curl -fsSL https://pgp.mongodb.com/server-{VERSION.value}.asc | sudo'
      f' gpg -o /usr/share/keyrings/mongodb-server-{VERSION.value}.gpg'
      ' --dearmor'
  )
  vm.RemoteCommand(
      'echo "deb [ arch=amd64,arm64'
      f' signed-by=/usr/share/keyrings/mongodb-server-{VERSION.value}.gpg ]'
      ' https://repo.mongodb.org/apt/ubuntu'
      f' focal/mongodb-org/{VERSION.value} multiverse" | sudo tee'
      f' /etc/apt/sources.list.d/mongodb-org-{VERSION.value}.list'
  )
  vm.AptUpdate()


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  AptSetup(vm)
  vm.InstallPackages('mongodb-org')
  _Setup(vm)


def YumUninstall(vm):
  """Uninstalls the mongodb package on the VM."""
  vm.RemoteCommand(f'sudo rm /etc/yum.repos.d/mongodb-org-{VERSION.value}.repo')


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
