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

"""Package for installing the Azure credentials."""

import os
from perfkitbenchmarker import object_storage_service

AZURE_CREDENTIAL_LOCATION = '.azure'
AZURE_CREDENTIAL_TOKENS_FILE = os.path.join(
    AZURE_CREDENTIAL_LOCATION, 'accessTokens.json')
AZURE_CREDENTIAL_PROFILE_FILE = os.path.join(
    AZURE_CREDENTIAL_LOCATION, 'azureProfile.json')


def Install(vm):
  """Copies Azure credentials to the VM."""
  vm.RemoteCommand('mkdir -p {0}'.format(AZURE_CREDENTIAL_LOCATION))
  vm.PushFile(
      object_storage_service.FindCredentialFile(
          os.path.join('~', AZURE_CREDENTIAL_TOKENS_FILE)),
      AZURE_CREDENTIAL_TOKENS_FILE)
  vm.PushFile(
      object_storage_service.FindCredentialFile(
          os.path.join('~', AZURE_CREDENTIAL_PROFILE_FILE)),
      AZURE_CREDENTIAL_PROFILE_FILE)
