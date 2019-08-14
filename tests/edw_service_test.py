# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for edw_service.py."""

import copy
import json
import unittest

import mock
from perfkitbenchmarker import edw_service
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import pkb_common_test_case

CLUSTER_PARAMETER_GROUP = 'fake_redshift_cluster_parameter_group'
CLUSTER_SUBNET_GROUP = 'fake_redshift_cluster_subnet_group'
PKB_CLUSTER = 'pkb-cluster'
PKB_CLUSTER_DATABASE = 'pkb-database'
REDSHIFT_NODE_TYPE = 'dc2.large'
USERNAME = 'pkb-username'
PASSWORD = 'pkb-password'
TEST_RUN_URI = 'fakeru'

BASE_REDSHIFT_SPEC = {
    'cluster_identifier': PKB_CLUSTER,
    'db': PKB_CLUSTER_DATABASE,
    'user': USERNAME,
    'password': PASSWORD,
    'node_type': REDSHIFT_NODE_TYPE,
    'node_count': 1
}


class ClientVm(object):
  """A fake VM class that can proxies a remote command to execute query."""

  def RemoteCommand(self, command):
    """Returns sample output for executing a query."""
    pass


class FakeEdwService(edw_service.EdwService):
  """A fake Edw Service class."""

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GenerateScriptExecutionCommand(self, script):
    return ' '.join(
        super(FakeEdwService, self).GenerateScriptExecutionCommand(script))


class EdwServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def testGetScriptExecutionResultsMocked(self):
    kwargs = copy.copy(BASE_REDSHIFT_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    client_vm = ClientVm()
    with mock.patch.object(
        client_vm,
        'RemoteCommand',
        return_value=(json.dumps(
            {'test.sql': {
                'execution_time': 1.0,
                'job_id': 'test_job_id'
            }}), '')):
      performance, _ = edw_local.GetScriptExecutionResults(
          'test.sql', client_vm)
      self.assertEqual(performance, 1.0)


if __name__ == '__main__':
  unittest.main()
