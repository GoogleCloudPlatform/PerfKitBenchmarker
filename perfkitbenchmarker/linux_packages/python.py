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

# TODO(user): Remove all uses of Python 2.

import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

# Gets major.minor version of python
GET_VERSION = ('import sys; '
               'print(".".join(str(v) for v in sys.version_info[:2]))')


def Install(vm):
  """Installs the package on the VM."""
  vm.InstallPackages(vm.PYTHON_2_PACKAGE)
  _SetDefaultPythonIfNeeded(vm)


def _SetDefaultPythonIfNeeded(vm):
  """Points /usr/bin/python -> /usr/bin/python2, if it isn't set.

  This has been deprecated along with Python 2 by new OSes, but old scripts
  require it.

  Args:
    vm: The virtual machine to set the default version of Python on.

  Raises:
    PythonPackageRequirementUnfulfilled: If we could not set the python alias.
  """

  @vm_util.Retry(
      retryable_exceptions=(errors.VirtualMachine.RemoteCommandError,))
  def _RunCommand(command):
    return vm.RemoteCommandWithReturnCode(command, ignore_failure=True)

  python_version_cmd = 'python --version'

  stdout, stderr, return_code = _RunCommand(python_version_cmd)
  python_found = not return_code
  if python_found:
    logging.info(
        'Default version of python: %s', (stdout or stderr).strip().split()[-1])
    return
  logging.info('Trying to set the default python version')
  _, _, python2_not_found = _RunCommand('ls /usr/bin/python2')
  if python2_not_found:
    raise errors.Setup.PythonPackageRequirementUnfulfilled(
        'No default version of python set and /usr/bin/python2 does not exist')
  _, _, update_alternatives_failed = _RunCommand(
      'sudo update-alternatives --set python /usr/bin/python2')
  if update_alternatives_failed:
    # Ubuntu 20 lets you install python2, but not update-alternatives to point
    # python to it. Also some distros might not include update-alternatives.
    # In any case fall back to a symlink
    vm.RemoteCommandWithReturnCode(
        'sudo ln -s /usr/bin/python2 /usr/bin/python')
  _, txt, python_not_found = _RunCommand(python_version_cmd)
  if python_not_found:
    raise errors.Setup.PythonPackageRequirementUnfulfilled(
        "'python' command not working after setting alias.")
  logging.info('Set default python version to %s', txt.strip().split()[-1])


def GetPythonVersion(vm, python_cmd='python') -> str:
  """Get the major.minor version of Python on the vm."""
  python_version, _ = vm.RemoteCommand(f"{python_cmd} -c '{GET_VERSION}'")
  return python_version.strip()
