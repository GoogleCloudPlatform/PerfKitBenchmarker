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

Uninstalling the pip package will also remove all python packages
added after installation.
"""

import logging
import re
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages

# Version of python to match with pip.
_EXPECTED_PIP_PYTHON_VERSION = 2

# Where the correct pip file could be found
_KNOWN_PIP_PATHS = [
    # Some OSes install to /usr/bin/pip2.
    '/usr/bin/pip{}'.format(_EXPECTED_PIP_PYTHON_VERSION),
    # Sometimes pip is available to the PKB user and not root.
    '/usr/local/bin/pip{}'.format(_EXPECTED_PIP_PYTHON_VERSION),
    '/usr/local/bin/pip'
]

# Regex to match the output of "pip --version"
_PIP_VERSION_RE = re.compile(r'^pip (?P<pip_version>\S+) '
                             r'from (?P<pip_path>.*?)\s+'
                             r'\(python (?P<python_version>\S+)\)$')

# Use this file if pip not in the path or a different version of python is used
_DEFAULT_PIP_PATH = '/usr/bin/pip'

# Symlink command for putting pip in the path
_SYMLINK_COMMAND = 'sudo ln -s {} ' + _DEFAULT_PIP_PATH


def Install(vm, package_name='python-pip'):
  """Install pip on the VM."""
  vm.InstallPackages(package_name)
  # Make sure pip is available as the PKB user and as root.
  _MakePipSymlink(vm, as_root=False)
  _MakePipSymlink(vm, as_root=True)
  if vm.PYTHON_PIP_PACKAGE_VERSION:
    vm.RemoteCommand(
        'sudo pip install --upgrade '
        '--force-reinstall pip=={0}'.format(vm.PYTHON_PIP_PACKAGE_VERSION))
  else:
    vm.RemoteCommand('sudo pip install -U pip')  # Make pip upgrade pip

  vm.RemoteCommand('mkdir -p {0} && pip freeze > {0}/requirements.txt'.format(
      linux_packages.INSTALL_DIR))


def YumInstall(vm):
  """Installs the pip package on the VM."""
  vm.InstallEpelRepo()
  Install(vm, vm.PYTHON_PACKAGE + '-pip')


def SwupdInstall(vm):
  """Installs the pip package on the VM."""
  vm.InstallPackages('which')
  package_name = 'python-basic'
  Install(vm, package_name)


def Uninstall(vm):
  """Uninstalls the pip package on the VM."""
  vm.RemoteCommand('pip freeze | grep --fixed-strings --line-regexp '
                   '--invert-match --file {0}/requirements.txt | '
                   'xargs --no-run-if-empty sudo pip uninstall -y'.format(
                       linux_packages.INSTALL_DIR))


def _MakePipSymlink(vm, as_root=False):
  """If needed makes a symlink at /usr/bin/pip for correct pip version.

  Args:
    vm: Virtual Machine to run on.
    as_root: Whether to run the commands as root.
  """
  # first see if we are okay
  major_version, python_version, pip_path = PythonVersionForPip(vm, as_root)
  if major_version == _EXPECTED_PIP_PYTHON_VERSION:
    logging.info('Good: "pip" (root=%s) in PATH is %s and is for python %s',
                 as_root, pip_path, python_version)
    return
  if pip_path == _DEFAULT_PIP_PATH:
    # Only remove if /usr/bin/pip as will later make symlink to it.
    vm.RemoteCommand('sudo rm {}'.format(pip_path))
  for path in _KNOWN_PIP_PATHS:
    if vm.TryRemoteCommand('ls {}'.format(path)):
      vm.RemoteCommand(_SYMLINK_COMMAND.format(path))
      break
  major_version, python_version, pip_path = PythonVersionForPip(vm, as_root)
  if major_version != _EXPECTED_PIP_PYTHON_VERSION:
    raise errors.Setup.InvalidConfigurationError(
        '"pip" {} (root={}) uses python {}'.format(pip_path, as_root,
                                                   python_version))


def PythonVersionForPip(vm, as_root):
  """Returns tuple about the "pip" command in the path at the given location.

  Args:
    vm: Virtual Machine to run on.
    as_root: Whether to run the commands as root.

  Returns:
    Tuple of (python major version, python version, pip path) or (None, None,
    None) if not found.
  """
  cmd_prefix = 'sudo ' if as_root else ''
  real_pip_path, _, exit_code = vm.RemoteCommandWithReturnCode(
      cmd_prefix + 'which pip', ignore_failure=True)
  if exit_code:
    return None, None, None
  pip_text, _ = vm.RemoteCommand(cmd_prefix + 'pip --version')
  m = _PIP_VERSION_RE.match(pip_text)
  if not m:
    raise ValueError('{} --version "{}" does not match expected "{}"'.format(
        real_pip_path, pip_text, _PIP_VERSION_RE.pattern))
  python_version = m.group('python_version')
  major_python_version = int(re.search(r'^(\d+)', python_version).group(1))
  return major_python_version, python_version, real_pip_path.strip()
