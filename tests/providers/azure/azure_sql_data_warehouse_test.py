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
"""Tests for perfkitbenchmarker.providers.azure.azure_sql_data_warehouse."""
import copy
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.azure import azure_sql_data_warehouse
from tests import pkb_common_test_case

_FAKE_IP_ADDRESS = 'fake_ip_address'

_TEST_RUN_URI = 'fakeru'
_AZURE_REGION_WEST_US_2 = 'westus2'
_BASE_AZURE_DW_SPEC = {
    'type': 'azuresqldatawarehouse',
    'server_name': 'helix-ds-1t',
    'db': 'ds-1t-hash',
    'user': 'fake_user',
    'password': 'fake_password',
    'node_type': 'dw5000c',
    'resource_group': 'helix-westus2'
}

FLAGS = flags.FLAGS


class FakeRemoteVM(object):

  def __init__(self):
    self.ip_address = _FAKE_IP_ADDRESS

  def Install(self, package_name):
    if package_name != 'mssql_tools':
      raise RuntimeError


class AzureDWTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(cloud='Azure')
  @flagsaver.flagsaver(run_uri=_TEST_RUN_URI)
  @flagsaver.flagsaver(zones=[_AZURE_REGION_WEST_US_2])
  def testInstallAndAuthenticateRunner(self):
    kwargs = copy.copy(_BASE_AZURE_DW_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    azure_sql_data_warehouse_local = azure_sql_data_warehouse.Azuresqldatawarehouse(
        spec)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=('out_', 'err_', 0)) as mock_issue:
      azure_sql_data_warehouse_local.InstallAndAuthenticateRunner(
          vm=FakeRemoteVM(), benchmark_name='fake_benchmark_name')
      mock_issue.assert_called_once()
      mock_issue.assert_called_with([
          'az', 'sql', 'server', 'firewall-rule', 'create', '--name',
          _FAKE_IP_ADDRESS, '--resource-group', 'helix-westus2', '--server',
          'helix-ds-1t', '--end-ip-address', 'fake_ip_address',
          '--start-ip-address', 'fake_ip_address'
      ])


if __name__ == '__main__':
  unittest.main()
