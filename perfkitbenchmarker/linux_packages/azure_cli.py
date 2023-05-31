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

from perfkitbenchmarker import vm_util

# Debian info
_DEB_REPO_FILE = '/etc/apt/sources.list.d/azure-cli.list'
_DEB_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_DEB_REPO = ('deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ '
             '{az_repo} main')

# RedHat info
_YUM_REPO_FILE = '/etc/yum.repos.d/azure-cli.repo'
_YUM_REPO_KEY = 'https://packages.microsoft.com/keys/microsoft.asc'
_YUM_REPO_NAME = 'Azure CLI'
_YUM_REPO_URL = 'https://packages.microsoft.com/yumrepos/azure-cli'
_YUM_REPO = f"""[azure-cli]
name={_YUM_REPO_NAME}
baseurl={_YUM_REPO_URL}
enabled=1
gpgcheck=1
gpgkey={_YUM_REPO_KEY}
"""


@vm_util.Retry(poll_interval=15, timeout=5*60)
def ZypperInstall(vm):
  """Installs the azure-cli package on the VM for SUSE systems.

  Args:
    vm: Virtual Machine to install on.
  """
  # Work-around to remove conflicting python packages. See
  # https://github.com/Azure/azure-cli/issues/13209
  vm.RemoteCommand(
      'sudo zypper install -y --oldpackage azure-cli-2.0.45-4.22.noarch')
  vm.RemoteCommand('sudo zypper rm -y --clean-deps azure-cli')
  vm.Install('curl')
  vm.RemoteCommand('sudo rpm --import {key}'.format(key=_YUM_REPO_KEY))
  vm.RemoteCommand(
      f'sudo zypper addrepo --name "{_YUM_REPO_NAME}" '
      f'--check {_YUM_REPO_URL} azure-cli')
  vm.RemoteCommand('sudo zypper install -y --from azure-cli azure-cli')


def _PipInstall(vm):
  """Installs azure-cli via pip on the VM.

  Args:
    vm: Virtual Machine to install on.
  """
  vm.Install('pip3')
  vm.RemoteCommand('sudo pip3 install azure-cli')


def AptInstall(vm):
  """Installs the azure-cli package on the VM for Debian systems.

  Args:
    vm: Virtual Machine to install on.
  """
  if vm.is_aarch64:
    _PipInstall(vm)
    return
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
