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
"""Module containing pip installation and cleanup functions.

Prefer pip module distributed with python. Some OSes like Debian do not install
pip with Python and use get-pip.py for those.

This module does not install pip from the OS, because that gets stale and cannot
safely be upgraded.

Uninstalling the pip package will also remove all python packages
added after installation.
"""

from absl import logging
from packaging import version
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import python

import requests

# NOTE: versionless (latest) URL is in root directory and versions have their
# own subdirectories.
GET_PIP_URL = 'https://bootstrap.pypa.io/pip/get-pip.py'
GET_PIP_VERSIONED_URL = 'https://bootstrap.pypa.io/pip/{python_version}/get-pip.py'


def Install(vm, pip_cmd='pip', python_cmd='python'):
  """Install pip on the VM."""
  # Install Python Dev and build tools apt-get/yum install python-pip
  vm.Install(python_cmd + '_dev')
  vm.Install('build_tools')
  vm.Install('curl')

  if vm.TryRemoteCommand(python_cmd + ' -m pip --version'):
    logging.info('pip bundled with Python re-using that.')
    # Use /usr/bin because /usr/local/bin is sometimes excluded from sudo's PATH
    pip_path = '/usr/bin/' + pip_cmd
    # Create an sh shim that redirects to python -m pip
    vm.RemoteCommand(
        f"echo 'exec {python_cmd} -m pip \"$@\"'| sudo tee {pip_path} "
        f'&& sudo chmod 755 {pip_path}')
  else:
    # get-pip.py has the appropriate latest version of pip for all Python
    # versions. Prefer it over linux packages or easy_install
    logging.info('pip not bundled with Python. Installing with get-pip.py')
    python_version = python.GetPythonVersion(vm, python_cmd)
    python_version = version.Version(python_version)
    # At the time of Jul 2022 pypi has special get-pips for versions up
    # through 3.6. To be future proof check for the existence of a versioned
    # URL using requests.
    versioned_url = GET_PIP_VERSIONED_URL.format(python_version=python_version)
    response = requests.get(versioned_url)
    if response.ok:
      get_pip_url = versioned_url
    else:
      get_pip_url = GET_PIP_URL
    # get_pip can suffer from various network issues.
    @vm_util.Retry(
        max_retries=5,
        retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,))
    def GetPipWithRetries():
      vm.RemoteCommand(
          f'curl {get_pip_url} -o get_pip.py && sudo {python_cmd} get_pip.py')

    GetPipWithRetries()

  # Verify installation
  vm.RemoteCommand(pip_cmd + ' --version')

  # Record installed Python packages
  install_dir = linux_packages.INSTALL_DIR
  vm.RemoteCommand(f'mkdir -p {install_dir} '
                   f'&& {pip_cmd} freeze | tee {install_dir}/requirements.txt')


def Uninstall(vm, pip_cmd='pip'):
  """Uninstalls the pip package on the VM."""
  install_dir = linux_packages.INSTALL_DIR
  vm.RemoteCommand(f'{pip_cmd} freeze | grep --fixed-strings --line-regexp '
                   f'--invert-match --file {install_dir}/requirements.txt | '
                   f'xargs --no-run-if-empty sudo {pip_cmd} uninstall -y')
