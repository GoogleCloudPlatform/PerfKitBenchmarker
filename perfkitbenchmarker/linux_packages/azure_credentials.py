# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Package for installing the Azure MSAL credentials  for the azure-cli."""

import os

from absl import logging
from packaging import version

from perfkitbenchmarker import errors
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure

AZURE_CREDENTIAL_DIRECTORY = os.path.join('~', '.azure')
# Necessary for user login with MSAL
TOKENS_FILE = os.path.join(AZURE_CREDENTIAL_DIRECTORY, 'msal_token_cache.json')
# Necessary for service_principal login with MSAL
SERVICE_PRINCIPAL_FILE = os.path.join(AZURE_CREDENTIAL_DIRECTORY,
                                      'service_principal_entries.json')
# Determines which login is active
PROFILE_FILE = os.path.join(AZURE_CREDENTIAL_DIRECTORY, 'azureProfile.json')

# See https://docs.microsoft.com/en-us/cli/azure/msal-based-azure-cli
_REQUIRED_MSAL_CLI_VERSION = '2.30.0'


def Install(vm):
  """Copies Azure credentials to the VM."""
  # Validate local azure-cli uses MSAL
  stdout, _, _ = vm_util.IssueCommand(
      [azure.AZURE_PATH, 'version', '--query', '"azure-cli"'])
  az_version = version.Version(stdout.strip('"\n'))
  if az_version < version.Version(_REQUIRED_MSAL_CLI_VERSION):
    raise errors.Benchmarks.MissingObjectCredentialException(
        f'Local Azure CLI version must be at least {_REQUIRED_MSAL_CLI_VERSION}'
        f' to copy credentials into a VM. Found version {az_version}. '
        'The recent CLI on the VM will not be able to use your credentials.')

  # Install CLI to validate credentials
  vm.Install('azure_cli')

  # Copy credentials to VM
  vm.RemoteCommand('mkdir -p {0}'.format(AZURE_CREDENTIAL_DIRECTORY))
  vm.PushFile(
      object_storage_service.FindCredentialFile(PROFILE_FILE), PROFILE_FILE)
  for file in [SERVICE_PRINCIPAL_FILE, TOKENS_FILE]:
    try:
      vm.PushFile(object_storage_service.FindCredentialFile(file), file)
    except errors.Benchmarks.MissingObjectCredentialException:
      logging.info('Optional service account file %s not found.', file)

  # Validate azure-cli is now authenticating correctly.
  try:
    # This token is not used, it is simply used to prove that the CLI on the VM
    # is authenticated.
    vm.RemoteCommand('az account get-access-token')
  except errors.VirtualMachine.RemoteExceptionError as e:
    raise errors.Benchmarks.MissingObjectCredentialException(
        'Failed to install azure_credentials on VM.') from e
