# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities for working with Azure resources."""

import logging
import sys

from perfkitbenchmarker import events
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

AZURE_PATH = 'azure'
EXPECTED_VERSION = '0.9.9'


def _CheckAzureVersion():
  """Warns the user if the Azure CLI isn't the expected version."""
  version_cmd = [AZURE_PATH, '-v']
  try:
    stdout, _, _ = vm_util.IssueCommand(version_cmd)
  except OSError:
    err_msg = ('Unable to execute the Azure CLI. See log for more details. See '
               'README.md for information about how to set up PKB for Azure.')
    logging.debug(err_msg, exc_info=True)
    sys.exit(err_msg)
  version = stdout.strip()
  if version != EXPECTED_VERSION:
    logging.warning('The version of the Azure CLI (%s) does not match the '
                    'expected version (%s). This may result in '
                    'incompatibilities which will cause commands to fail. '
                    'Please install the reccomended version to ensure '
                    'compatibility.', version, EXPECTED_VERSION)


def _HandleProviderImported(sender):
  assert sender == providers.AZURE, sender
  _CheckAzureVersion()


# Register handler to call _CheckAzureVersion after Azure modules are imported.
events.provider_imported.connect(_HandleProviderImported, providers.AZURE,
                                 weak=False)
