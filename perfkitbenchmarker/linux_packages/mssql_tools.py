# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Package for installing the MS SQL Tools.

Installation instructions:
https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-2017

Steps for Apt installs:
1. Install pre-requisites.
2. Install the repo key.
3. Register the new repo.
4. Install the mssql-toools.
"""

from perfkitbenchmarker import os_types

# Debian info
_DEB_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_DEB_REPO_FILE = 'https://packages.microsoft.com/config/ubuntu/{os}/prod.list'
_DEB_FILE_LOCATION = '/etc/apt/sources.list.d/msprod.list'

OS_TYPE_MAPPING = {
    os_types.UBUNTU1604: '16.04',
    os_types.UBUNTU1804: '18.04',
    os_types.UBUNTU2004: '20.04'
}


def AptInstall(vm):
  """Installs the mssql-tools package on the VM for Debian systems.

  Args:
    vm: Virtual Machine to install on.
  """
  vm.Install('unixodbc_dev')
  vm.RemoteCommand(
      'curl {key} | sudo apt-key add -'.format(key=_DEB_REPO_KEY))
  deb_repro_file = _DEB_REPO_FILE.format(os=OS_TYPE_MAPPING[vm.OS_TYPE])
  vm.RemoteCommand(
      'curl {file} | sudo tee {location}'.format(file=deb_repro_file,
                                                 location=_DEB_FILE_LOCATION))
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo ACCEPT_EULA=Y /usr/bin/apt-get -y install mssql-tools')
