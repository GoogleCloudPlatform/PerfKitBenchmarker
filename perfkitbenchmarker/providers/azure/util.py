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

from perfkitbenchmarker import vm_util

AZURE_PATH = 'azure'
EXPECTED_VERSION = '0.9.9'


def CheckAzureVersion():
  """Warns the user if the Azure CLI isn't the expected version."""
  version_cmd = [AZURE_PATH, '-v']
  try:
    stdout, _, _ = vm_util.IssueCommand(version_cmd)
  except OSError:
    # IssueCommand will raise an OSError if the CLI is not installed on the
    # system. Since we don't want to warn users if they are doing nothing
    # related to Azure, just do nothing if this is the case.
    return
  version = stdout.strip()
  if version != EXPECTED_VERSION:
    logging.warning('The version of the Azure CLI (%s) does not match the '
                    'expected version (%s). This may result in '
                    'incompatibilities which will cause commands to fail. '
                    'Please install the reccomended version to ensure '
                    'compatibility.', version, EXPECTED_VERSION)
