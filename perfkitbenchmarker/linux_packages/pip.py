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
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import python
import requests

# NOTE: versionless (latest) URL is in root directory and versions have their
# own subdirectories.
GET_PIP_URL = 'https://bootstrap.pypa.io/pip/get-pip.py'
GET_PIP_VERSIONED_URL = (
    'https://bootstrap.pypa.io/pip/{python_version}/get-pip.py'
)


def _DownloadAndInstallPip(vm):
  """Download and install pip using get-pip.py.

  get-pip.py has the appropriate latest version of pip for all Python versions.
  Prefer it over linux packages or easy_install.

  Args:
    vm: The VM to install pip on.
  """
  logging.info('pip not bundled with Python. Installing with get-pip.py')
  python_version = python.GetPythonVersion(vm)
  python_version = version.Version(python_version)
  # At the time of Aug 2024 pypi has special get-pips for versions up
  # through 3.7. To be future proof check for the existence of a versioned
  # URL using requests.
  versioned_url = GET_PIP_VERSIONED_URL.format(python_version=python_version)

  # Retry in case there are various temporary network issues.
  @vm_util.Retry(
      max_retries=5,
      retryable_exceptions=(ConnectionError,)
  )
  def GetPipUrl():
    return requests.get(versioned_url)

  response = GetPipUrl()
  if response.ok:
    get_pip_url = versioned_url
  else:
    get_pip_url = GET_PIP_URL

  # get_pip can suffer from various network issues.
  @vm_util.Retry(
      max_retries=10,
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,),
  )
  def GetPipWithRetries():
    vm.RemoteCommand(
        f'curl {get_pip_url} -o get_pip.py && sudo python3 get_pip.py'
    )

  GetPipWithRetries()


def Install(vm):
  """Install pip on the VM."""
  # Install Python Dev and build tools apt-get/yum install python-pip
  vm.Install('python_dev')
  vm.Install('build_tools')
  vm.Install('curl')

  # Python 3.11 added https://peps.python.org/pep-0668/, which allows the OS to
  # disable any pip installation outside of a virtual env. This is good in
  # long-lived systems, but counter-productive in short lived test
  # environmements (it is possible a system command or daemon does fail,
  # because PKB installs a root pip module, but relatively unlikely).
  # Delete the file so that pip will continue to install root packages.
  # https://discuss.python.org/t/pep-668-marking-python-base-environments-as-externally-managed/10302/80
  vm.RemoteCommand('sudo rm -f /usr/lib/python3*/EXTERNALLY-MANAGED')
  # Work around Ubuntu distutils weirdness.
  # https://github.com/pypa/get-pip/issues/44
  if vm.HasPackage('python3-distutils'):
    vm.InstallPackages('python3-distutils')

  # Check if pip is already bundled with Python.
  if not vm.TryRemoteCommand('python3 -m pip --version'):
    _DownloadAndInstallPip(vm)

  # Make sure pip and pip3 are available in sudo's PATH.
  for pip_command in ('pip', 'pip3'):
    if not vm.TryRemoteCommand(f'sudo {pip_command} --version'):
      pip_path = f'/usr/bin/{pip_command}'
      # Create an sh shim that redirects to python -m pip
      # Use /usr/bin because /usr/local/bin is sometimes excluded from sudo's
      # PATH
      vm.RemoteCommand(
          f'echo \'exec python3 -m pip "$@"\'| sudo tee {pip_path} '
          f'&& sudo chmod 755 {pip_path}'
      )

  # Verify installation
  vm.RemoteCommand('sudo pip --version')
  vm.RemoteCommand('sudo pip3 --version')
