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


def YumInstall(vm):
  """Installs the mongodb package on the VM."""
  mongodb_repo = (
      '[mongodb]\nname=MongoDB Repository\nbaseurl='
      'http://downloads-distro.mongodb.org/repo/redhat/os/x86_64/\n'
      'gpgcheck=0\nenabled=1')
  vm.RemoteCommand('echo "%s" | sudo tee /etc/yum.repos.d/mongodb.repo' %
                   mongodb_repo)
  vm.InstallPackages('mongodb-org-server')


def AptInstall(vm):
  """Installs the mongodb package on the VM."""
  vm.InstallPackages('mongodb-server')


def YumUninstall(vm):
  """Uninstalls the mongodb package on the VM."""
  vm.RemoteCommand('sudo rm /etc/yum.repos.d/mongodb.repo')


def YumGetServiceName(vm):
  """Returns the name of the mongodb service."""
  return 'mongod'


def AptGetServiceName(vm):
  """Returns the name of the mongodb service."""
  return 'mongodb'


def YumGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  return '/etc/mongod.conf'


def AptGetPathToConfig(vm):
  """Returns the path to the mongodb config file."""
  return '/etc/mongodb.conf'
