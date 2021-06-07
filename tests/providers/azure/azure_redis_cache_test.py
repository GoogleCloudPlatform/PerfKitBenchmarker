# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.azure.azure_redis_cache."""
import unittest
from absl import flags
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import azure_redis_cache
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AzureRedisCacheTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzureRedisCacheTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['eastus']
    mock_spec = mock.Mock()
    mock_resource_group = mock.Mock()
    self.resource_group_patch = mock.patch.object(azure_network,
                                                  'GetResourceGroup').start()
    self.resource_group_patch.return_value = mock_resource_group
    mock_resource_group.name = 'az_resource'
    self.redis = azure_redis_cache.AzureRedisCache(mock_spec)
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()

  def testCreate(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'az', 'redis', 'create', '--resource-group', 'az_resource',
        '--location', 'us-central1', '--name', 'pkb-None', '--sku', 'Basic',
        '--vm-size', 'C3', '--enable-non-ssl-port'
    ]
    self.redis._Create()
    self.mock_command.assert_called_once_with(expected_output, timeout=900)

  def testDelete(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'az', 'redis', 'delete', '--resource-group', 'az_resource', '--name',
        'pkb-None', '--yes'
    ]
    self.redis._Delete()
    self.mock_command.assert_called_once_with(expected_output, timeout=900)

  def testExistTrue(self):
    self.mock_command.return_value = (None, '', 0)
    expected_output = [
        'az', 'redis', 'show', '--resource-group', 'az_resource', '--name',
        'pkb-None'
    ]
    self.redis._Exists()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
