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


"""Module containing MS SQL Server 2022 installation and cleanup functions."""


# version: evaluation, developer, express, web, standard, enterprise


def YumInstall(vm):
  """Installs the ms sql package on the RedHat VM."""
  vm.RemoteCommand('sudo curl -o /etc/yum.repos.d/mssql-server.repo '
                   'https://packages.microsoft.com/config/rhel/8/'
                   'mssql-server-2019.repo')
  vm.RemoteCommand('sudo curl -o /etc/yum.repos.d/msprod.repo '
                   'https://packages.microsoft.com/config/rhel/8/prod.repo')

  vm.InstallPackages('mssql-server')

  vm.RemoteCommand('sudo yum remove unixODBC-utf16 unixODBC-utf16-devel')
  vm.RemoteCommand('sudo ACCEPT_EULA=Y yum install -y '
                   'mssql-tools unixODBC-devel')
  vm.RemoteCommand(
      r'echo PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bash_profile')
  vm.RemoteCommand(
      r'echo export PATH="$PATH:/opt/mssql-tools/bin" >> ~/.bashrc')
  vm.RemoteCommand('source ~/.bashrc')
