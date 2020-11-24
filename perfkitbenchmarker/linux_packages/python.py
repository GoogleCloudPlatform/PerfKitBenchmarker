# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing python 2.7 installation and cleanup functions."""

import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util


def YumInstall(vm):
  """Installs the package on the VM."""
  vm.InstallPackages(vm.PYTHON_PACKAGE)
  _SetDefaultPythonIfNeeded(vm, '/usr/bin/{}'.format(vm.PYTHON_PACKAGE))


def AptInstall(vm):
  """Installs the package on the VM."""
  vm.InstallPackages('python python2.7')
  _SetDefaultPythonIfNeeded(vm, '/usr/bin/python2')


def SwupdInstall(vm):
  """Installs the package on the VM."""
  vm.InstallPackages('python-basic')


def _SetDefaultPythonIfNeeded(vm, python_path):
  """Sets the default version of python to the specified path if required.

  Some linux distributions do not set a default version of python to use so that
  running "python ..." will fail.  If the "alternatives" program is found on
  the VM it is used to set the default version to the one specified.

  Logs a warning if the default version is not set and the alternatives program
  could not be run to set it.

  Args:
    vm: The virtual machine to set the default version of python on.
    python_path: Path to the python executable.

  Raises:
    PythonPackageRequirementUnfulfilled: If the default version of python is
    not set and it is possible to set via "alternatives" but the alternatives
    program failed.
  """

  @vm_util.Retry(
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,))
  def _RunCommand(command):
    return vm.RemoteCommandWithReturnCode(command, ignore_failure=True)

  python_version_cmd = 'python --version'
  python_exists_cmd = 'ls {}'.format(python_path)
  alternatives_exists_cmd = 'which update-alternatives'
  alternatives_cmd = 'sudo update-alternatives --set python {}'.format(
      python_path)

  stdout, stderr, return_code = _RunCommand(python_version_cmd)
  if not return_code:
    logging.info(
        'Default version of python: %s', (stdout or stderr).strip().split()[-1])
    return
  logging.info('Trying to set the default python version')
  _, _, return_code = _RunCommand(alternatives_exists_cmd)
  if return_code:
    # Some distros might not include update-alternatives
    logging.warning('Can not set default version of python as '
                    'update-alternatives program does not exist')
    return
  _, _, return_code = _RunCommand(python_exists_cmd)
  if return_code:
    # This is most likely an error but user could specify path to python
    logging.warning('No default version of python set and %s does not exist',
                    python_path)
    return
  _, error_text, return_code = _RunCommand(alternatives_cmd)
  if return_code:
    raise errors.Setup.PythonPackageRequirementUnfulfilled(
        'Could not set via update-alternatives default python version: {}'
        .format(error_text))
  _, txt, return_code = _RunCommand(python_version_cmd)
  if return_code:
    raise errors.Setup.PythonPackageRequirementUnfulfilled(
        'Set default python path to {} but could not use default version'
        .format(python_path))
  logging.info('Set default python version to %s', txt.strip().split()[-1])
