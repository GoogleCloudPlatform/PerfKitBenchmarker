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
https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver16

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
_DEB_FILE_LOCATION = '/etc/apt/sources.list.d/mssql-release.list'

OS_TYPE_MAPPING = {
    os_types.UBUNTU2004: '20.04',
    os_types.UBUNTU2204: '22.04',
}


def YumInstall(vm):
  """Installs the ms sql package on the RedHat VM."""
  vm.RemoteCommand(
      'sudo curl -o /etc/yum.repos.d/mssql-server.repo '
      'https://packages.microsoft.com/config/rhel/8/'
      'mssql-server-2022.repo'
  )
  vm.RemoteCommand(
      'sudo curl -o /etc/yum.repos.d/msprod.repo '
      'https://packages.microsoft.com/config/rhel/8/prod.repo'
  )

  vm.RemoteCommand(
      'sudo ACCEPT_EULA=Y yum install -y mssql-tools unixODBC-devel'
  )
  vm.RemoteCommand(r'echo PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile')
  vm.RemoteCommand(
      r'echo export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc'
  )
  vm.RemoteCommand('source ~/.bashrc')


def AptInstall(vm):
  """Installs the mssql-tools package on the VM for Debian systems.

  Args:
    vm: Virtual Machine to install on.
  """
  vm.RemoteCommand('curl {key} | sudo apt-key add -'.format(key=_DEB_REPO_KEY))
  deb_repro_file = _DEB_REPO_FILE.format(os=OS_TYPE_MAPPING[vm.OS_TYPE])
  vm.RemoteCommand(
      'curl {file} | sudo tee {location}'.format(
          file=deb_repro_file, location=_DEB_FILE_LOCATION
      )
  )
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand(
      'sudo ACCEPT_EULA=Y apt-get -y install mssql-tools18 '
      'unixodbc-dev'
  )
  vm.RemoteCommand('sudo ln -s /opt/mssql-tools18 /opt/mssql-tools')
  vm.RemoteCommand(
      r'sudo sed -i "1 i\export PATH=$PATH:/opt/mssql-tools/bin/" ~/.bashrc'
  )
  vm.RemoteCommand(
      r'sudo sed -i "1 i\export PATH=$PATH:/opt/mssql-tools/bin/" '
      '/etc/bash.bashrc'
  )


def ZypperInstall(vm):
  """Installs the ms sql package on the SLES VM."""
  vm.RemoteCommand(
      'sudo ACCEPT_EULA=Y zypper install -y mssql-tools18 unixODBC-devel'
      ' glibc-locale-base'
  )

  vm.RemoteCommand('sudo ln -s /opt/mssql-tools18 /opt/mssql-tools')
  vm.RemoteCommand(r'echo PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile')
  vm.RemoteCommand(
      r'echo export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc'
  )
  vm.RemoteCommand('source ~/.bashrc')
