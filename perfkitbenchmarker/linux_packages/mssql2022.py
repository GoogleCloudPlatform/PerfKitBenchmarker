# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing MS SQL Server 2022 installation and cleanup functions.

Debian instructions:
https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-ubuntu?view=sql-server-ver16

"""
from perfkitbenchmarker import os_types

# version: evaluation, developer, express, web, standard, enterprise

_DEB_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_DEB_REPO_FILE = 'https://packages.microsoft.com/config/ubuntu/{os}/mssql-server-2022.list'
_DEB_FILE_LOCATION = '/etc/apt/sources.list.d/mssql-server-2022.list'

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

  vm.InstallPackages('mssql-server')

  vm.RemoteCommand(
      'sudo ACCEPT_EULA=Y yum install -y mssql-tools unixODBC-devel'
  )
  vm.RemoteCommand(r'echo PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile')
  vm.RemoteCommand(
      r'echo export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc'
  )
  vm.RemoteCommand('source ~/.bashrc')
  vm.RemoteCommand(
      'sudo firewall-cmd --zone=public --add-port=1433/tcp --permanent'
  )
  vm.RemoteCommand('sudo firewall-cmd --reload')


def AptInstall(vm):
  """Installs the mssql-server package on the Debian VM."""
  vm.RemoteCommand(
      'curl {key} | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc'.format(
          key=_DEB_REPO_KEY
      )
  )
  deb_repro_file = _DEB_REPO_FILE.format(os=OS_TYPE_MAPPING[vm.OS_TYPE])
  if vm.OS_TYPE == os_types.UBUNTU2004:
    vm.RemoteCommand(
        'sudo add-apt-repository "$(wget -qO- {file})"'.format(
            file=deb_repro_file
        )
    )
  elif vm.OS_TYPE == os_types.UBUNTU2204:
    vm.RemoteCommand(
        'curl -fsSL {file} | sudo tee {location}'.format(
            file=deb_repro_file, location=_DEB_FILE_LOCATION
        )
    )
  else:
    raise NotImplementedError(
        'Invalid OS version: {}. SQL Server 2022 only supports Ubuntu 20.04'
        ' and 22.04'.format(vm.OS_TYPE)
    )

  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('mssql-server')

  vm.RemoteCommand('sudo ufw allow in 1433')


def ZypperInstall(vm):
  """Installs the ms sql package on the SLES VM."""
  vm.RemoteCommand('sudo zypper install -y glibc')
  vm.RemoteCommand(
      'sudo zypper addrepo -fc'
      ' https://packages.microsoft.com/config/sles/15/mssql-server-2022.repo'
  )
  vm.RemoteCommand(
      'sudo zypper ar'
      ' https://packages.microsoft.com/config/sles/15/prod.repo')
  vm.RemoteCommand('sudo zypper --gpg-auto-import-keys refresh')
  vm.RemoteCommand(
      'sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc'
  )
  vm.RemoteCommand('sudo zypper install -y mssql-server')

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
