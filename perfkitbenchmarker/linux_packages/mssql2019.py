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


"""Module containing MS SQL Server 2019 installation and cleanup functions.

Debian instructions:
https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-ubuntu?view=sql-server-ver15

"""
from perfkitbenchmarker import os_types

# version: evaluation, developer, express, web, standard, enterprise


def YumInstall(vm):
  """Installs the ms sql package on the RedHat VM."""
  vm.RemoteCommand(
      'sudo curl -o /etc/yum.repos.d/mssql-server.repo '
      'https://packages.microsoft.com/config/rhel/8/'
      'mssql-server-2019.repo'
  )
  vm.RemoteCommand(
      'sudo curl -o /etc/yum.repos.d/msprod.repo '
      'https://packages.microsoft.com/config/rhel/8/prod.repo'
  )

  vm.InstallPackages('mssql-server')

  vm.RemoteCommand('sudo yum remove unixODBC-utf16 unixODBC-utf16-devel')
  vm.RemoteCommand(
      'sudo ACCEPT_EULA=Y yum install -y mssql-tools unixODBC-devel'
  )
  vm.RemoteCommand(r'echo PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile')
  vm.RemoteCommand(
      r'echo export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc'
  )
  vm.RemoteCommand('source ~/.bashrc')


def AptInstall(vm):
  """Installs the mssql-server package on the Debian VM."""
  if vm.OS_TYPE != os_types.UBUNTU2004:
    raise NotImplementedError(
        'Invalid OS version: {}. SQL Server 2019 only supports Ubuntu 18.04'
        ' and20.04'.format(vm.OS_TYPE)
    )

  vm.RemoteCommand(
      'wget -qO- https://packages.microsoft.com/keys/microsoft.asc'
      ' | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc'
  )
  vm.RemoteCommand(
      'sudo add-apt-repository "$(wget -qO- '
      'https://packages.microsoft.com/config/ubuntu/20.04/'
      'mssql-server-2022.list)"'
  )

  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('mssql-server')

  vm.RemoteCommand('sudo ufw allow in 1433')


def ZypperInstall(vm):
  """Installs the ms sql package on the SLES VM."""
  vm.RemoteCommand('sudo zypper install -y glibc')
  vm.RemoteCommand(
      'sudo zypper addrepo -fc'
      ' https://packages.microsoft.com/config/sles/15/mssql-server-2019.repo'
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


