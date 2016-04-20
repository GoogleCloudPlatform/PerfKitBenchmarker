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
import os
import sys

from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure


AZURE_PATH = 'azure'

FLAGS = flags.FLAGS

_CLI_INSTALL_CMD = 'npm install -g azure-cli@%s' % azure.EXPECTED_CLI_VERSION


def _CheckAzureVersion():
  """Checks the version of the installed Azure CLI.

  Exits the process if the version cannot be checked, or if the installed CLI
  version does not match the expected version.

  If the user provides the --azure_ignore_cli_version flag, a mismatched CLI
  version will result in a warning, but execution will continue.
  """
  version_cmd = [AZURE_PATH, '-v']
  try:
    stdout, _, retcode = vm_util.IssueCommand(version_cmd)
  except OSError:
    err_msg = (
        'Unable to execute the Azure CLI. See the log file for more details. '
        'This failure may indicate that the Azure CLI is not installed. '
        'Execute the following command to install the Azure CLI:{sep}'
        '{install_cmd}{sep}See README.md for more information about how to set '
        'up PerfKit Benchmarker for Azure.'.format(install_cmd=_CLI_INSTALL_CMD,
                                                   sep=os.linesep))
    logging.debug(err_msg, exc_info=True)
    sys.exit(err_msg)

  if retcode:
    logging.error(
        'Failed to get the Azure CLI version. This failure may indicate that '
        'the installed version of the Azure CLI is out of date. Execute the '
        'following command to install the recommended version of the Azure '
        'CLI:%s%s', os.linesep, _CLI_INSTALL_CMD)
    sys.exit(retcode)

  version = stdout.strip()
  if version != azure.EXPECTED_CLI_VERSION:
    err_msg = (
        'The version of the installed Azure CLI ({installed}) does not match '
        'the expected version ({expected}). This may result in '
        'incompatibilities that can cause commands to fail. To ensure '
        'compatibility, install the recommended version of the CLI by '
        'executing the following command:{sep}{install_cmd}'.format(
            expected=azure.EXPECTED_CLI_VERSION, install_cmd=_CLI_INSTALL_CMD,
            installed=version, sep=os.linesep))
    if FLAGS.azure_ignore_cli_version:
      logging.warning(err_msg)
    else:
      logging.error(
          '%s%sTo bypass this CLI version check, run PerfKit Benchmarker with '
          'the --azure_ignore_cli_version flag.', err_msg, os.linesep)
      sys.exit(1)


def _HandleProviderImported(sender):
  assert sender == providers.AZURE, sender
  _CheckAzureVersion()


# Register handler to call _CheckAzureVersion after Azure modules are imported.
events.provider_imported.connect(_HandleProviderImported, providers.AZURE,
                                 weak=False)
