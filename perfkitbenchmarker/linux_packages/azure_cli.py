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
"""Package for installing the Azure CLI.

Installation instructions:
https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest

Steps for both Apt and Yum installs:
1. Install pre-requisites.
2. Install the new repo with the parameters (repo file name, key, and repo
   contents) found at the top of the file.
3. Install the repo key.
4. Install the azure-cli.
"""

# Debian info
_DEB_REPO_FILE = '/etc/apt/sources.list.d/azure-cli.list'
_DEB_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_DEB_REPO = ('deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ '
             '{az_repo} main')

# RedHat info
_YUM_REPO_FILE = '/etc/yum.repos.d/azure-cli.repo'
_YUM_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_YUM_REPO = """[azure-cli]
name=Azure CLI
baseurl=https://packages.microsoft.com/yumrepos/azure-cli
enabled=1
gpgcheck=1
gpgkey={key}
""".format(key=_YUM_REPO_KEY)


def AptInstall(vm):
  """Installs the azure-cli package on the VM for Debian systems.

  Args:
    vm: Virtual Machine to install on.
  """
  vm.Install('python')
  vm.Install('lsb_release')
  vm.Install('curl')
  vm.InstallPackages('apt-transport-https')
  az_repo, _ = vm.RemoteCommand('lsb_release -cs')
  _CreateFile(vm, _DEB_REPO.format(az_repo=az_repo.strip()), _DEB_REPO_FILE)
  vm.RemoteCommand(
      'curl -L {key} | sudo apt-key add -'.format(key=_DEB_REPO_KEY))
  vm.RemoteCommand('sudo apt-get update')
  vm.InstallPackages('azure-cli')


def YumInstall(vm):
  """Installs the azure-cli package on the VM for RedHat systems.

  Args:
    vm: Virtual Machine to install on.
  """
  _CreateFile(vm, _YUM_REPO, _YUM_REPO_FILE)
  vm.RemoteCommand('sudo rpm --import {key}'.format(key=_YUM_REPO_KEY))
  vm.InstallPackages('azure-cli')


def _CreateFile(vm, content, file_path):
  """Creates the repository file on the remote server.

  Args:
    vm: Remote virtual machine.
    content: Text to put into the file.
    file_path: Path to text output file.
  """
  vm.RemoteCommand('echo "{content}" | sudo tee {file_path}'.format(
      content=content, file_path=file_path))
