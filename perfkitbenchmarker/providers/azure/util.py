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

"""Verify that Azure CLI is in arm mode."""

from perfkitbenchmarker import events
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure


class BadAzureCLIModeError(Exception):
  pass


def _CheckAzureCLIMode(sender):
  assert sender == providers.AZURE, sender

  stdout, _ = vm_util.IssueRetryableCommand(
      [azure.AZURE_PATH, 'config'])

  if 'Current Mode: arm' not in stdout:
    raise BadAzureCLIModeError('Azure CLI may not be in ARM mode.')


events.provider_imported.connect(_CheckAzureCLIMode, providers.AZURE,
                                 weak=False)
