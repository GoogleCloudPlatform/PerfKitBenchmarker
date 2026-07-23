# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing MCP Toolbox for Databases install functions.

See:
https://docs.cloud.google.com/bigquery/docs/pre-built-tools-with-mcp-toolbox#install
See: https://github.com/googleapis/mcp-toolbox#install-toolbox
"""

import posixpath
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine

# Note: v0.7.0 recommended in GCP doc
# (https://docs.cloud.google.com/bigquery/docs/pre-built-tools-with-mcp-toolbox#install)
# does not work (returns 404). Use v1.6.0 from GitHub README:
# https://github.com/googleapis/mcp-toolbox#install-toolbox
_MCP_TOOLBOX_VERSION = flags.DEFINE_string(
    'mcp_toolbox_version',
    'v1.6.0',
    'Version of MCP Toolbox for Databases to install.',
)

PACKAGE_NAME = 'mcp_toolbox_for_db'


def GetVersion() -> str:
  """Returns the formatted version string starting with 'v'."""
  version = _MCP_TOOLBOX_VERSION.value
  if not version.startswith('v'):
    return f'v{version}'
  return version


def Install(vm: virtual_machine.BaseVirtualMachine) -> str:
  """Install MCP Toolbox for Databases in the VM's home directory.

  Args:
    vm: The virtual machine to install MCP Toolbox on.

  Returns:
    The installation path of the toolbox binary.

  Raises:
    errors.Setup.InvalidSetupError: If the expected version is not present in
      the post-install check of {toolbox_path} --version.
  """
  vm.Install('curl')
  version = GetVersion()

  home_dir = vm.RemoteCommand('echo $HOME')[0].strip()
  toolbox_path = posixpath.join(home_dir, 'toolbox')

  url = (
      'https://storage.googleapis.com/mcp-toolbox-for-databases/'
      f'{version}/linux/amd64/toolbox'
  )
  vm.RemoteCommand(f'curl -L -o {toolbox_path} {url}')
  vm.RemoteCommand(f'chmod +x {toolbox_path}')

  # Verify installation by checking version in stdout
  stdout, _ = vm.RemoteCommand(f'{toolbox_path} --version')
  expected_version = version.lstrip('v')
  if expected_version not in stdout:
    raise errors.Setup.InvalidSetupError(
        'MCP toolbox was not installed properly. Expected version'
        f' {expected_version}. Got: {stdout}'
    )

  return toolbox_path


def Uninstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Uninstall MCP Toolbox for Databases from the VM."""
  home_dir = vm.RemoteCommand('echo $HOME')[0].strip()
  toolbox_path = posixpath.join(home_dir, 'toolbox')
  vm.RemoteCommand(f'rm -f {toolbox_path}')

