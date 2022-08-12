# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.azure.snowflake_azure."""

import copy
import unittest

from absl import flags
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.azure import snowflake_azure
from tests import pkb_common_test_case

_TEST_RUN_URI = 'fakeru'
_AZURE_ZONE_EAST_US_2 = 'eastus2'
_BASE_SNOWFLAKE_SPEC = {'type': 'snowflake_azure'}

FLAGS = flags.FLAGS


class FakeRemoteVMCreateLambdaRole(object):

  def Install(self, package_name):
    if package_name != 'snowsql':
      raise RuntimeError

  def PushFile(self, file_to_push, push_destination):
    del push_destination
    if file_to_push != 'snowflake_snowsql_config_override_file':
      raise RuntimeError


class SnowflakeTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(SnowflakeTestCase, self).setUp()
    FLAGS.cloud = 'AZURE'
    FLAGS.run_uri = _TEST_RUN_URI
    FLAGS.zones = [_AZURE_ZONE_EAST_US_2]
    FLAGS.snowflake_snowsql_config_override_file = 'snowflake_snowsql_config_override_file'
    FLAGS.snowflake_connection = 'fake_connection'

  def testCreateRequestError(self):
    kwargs = copy.copy(_BASE_SNOWFLAKE_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    snowflake_local = snowflake_azure.Snowflake(spec)
    with self.assertRaises(NotImplementedError):
      snowflake_local._Create()

  def testIsAlwaysUserManaged(self):
    kwargs = copy.copy(_BASE_SNOWFLAKE_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    snowflake_local = snowflake_azure.Snowflake(spec)
    self.assertTrue(snowflake_local.IsUserManaged(spec))

  def testAlwaysExists(self):
    kwargs = copy.copy(_BASE_SNOWFLAKE_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    snowflake_local = snowflake_azure.Snowflake(spec)
    self.assertTrue(snowflake_local._Exists())


if __name__ == '__main__':
  unittest.main()
