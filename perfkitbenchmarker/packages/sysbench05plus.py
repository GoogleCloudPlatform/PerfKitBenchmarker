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


"""Module containing sysbench 0.5 (and later) installation and cleanup logic.

Sysbench 0.5 contains breaking changes from previous versions. All existing
oltp benchmarks depending on older version of sysbench will break if we
install 0.5 or later for them. Therefore, it's necessary that we have a
seperate installer here for 0.5 and later.
"""


def _Install(vm):
  """Installs the sysbench package on the VM."""
  vm.InstallPackages('sysbench')


def YumInstall(vm):
  """TODO: Implement this for Yum"""
  raise NotImplementedError('Not implemented yet: Sysbench 0.5 or later'
                            ' installation via Yum')


def AptInstall(vm):
  """ Installs the sysbench 0.5 or later versions via APT Install """

  # Setup the proper sources list so apt get will get the latest version
  # of sysbench. By default, it only gets version earlier than 0.5.
  vm.RemoteCommand('sudo bash -c \'echo "deb http://repo.percona.com/apt'
                   ' trusty main">>/etc/apt/sources.list.d/percona.list\'')
  vm.RemoteCommand('sudo bash -c \'echo "deb-src http://repo.percona.com/apt'
                   ' trusty main">>/etc/apt/sources.list.d/percona.list\'')
  vm.RemoteCommand('sudo bash -c \'echo "deb http://security.ubuntu.com/ubuntu'
                   ' trusty-security main">>/etc/apt/sources.list\'')
  vm.RemoteCommand('sudo apt-key adv --keyserver keys.gnupg.net --recv-keys'
                   ' 1C4CBDCDCD2EFD2A')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y libc6')

  _Install(vm)
