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

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import pkb_common_test_case

_CLUSTER_PARAMETER_GROUP = 'fake_redshift_cluster_parameter_group'
_CLUSTER_SUBNET_GROUP = 'fake_redshift_cluster_subnet_group'
_PKB_CLUSTER = 'pkb-cluster'
_PKB_CLUSTER_DATABASE = 'pkb-database'
_REDSHIFT_NODE_TYPE = 'dc2.large'
_USERNAME = 'pkb-username'
_PASSWORD = 'pkb-password'
_TEST_RUN_URI = 'fakeru'

_AWS_ZONE_US_EAST_1A = 'us-east-1a'

_BASE_REDSHIFT_SPEC = {
    'cluster_identifier': _PKB_CLUSTER,
    'db': _PKB_CLUSTER_DATABASE,
    'user': _USERNAME,
    'password': _PASSWORD,
    'node_type': _REDSHIFT_NODE_TYPE,
    'node_count': 1
}

FLAGS = flags.FLAGS


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

  @flagsaver.flagsaver(run_uri=_TEST_RUN_URI)
  @flagsaver.flagsaver(zones=[_AWS_ZONE_US_EAST_1A])
  def setUp(self):
    super(EdwServiceTest, self).setUp()
    FLAGS.run_uri = _TEST_RUN_URI
    FLAGS.zones = [_AWS_ZONE_US_EAST_1A]

  def testGetScriptExecutionResultsMocked(self):
    kwargs = copy.copy(_BASE_REDSHIFT_SPEC)
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

  def testIsUserManaged(self):
    kwargs = copy.copy({
        'cluster_identifier': _PKB_CLUSTER,
        'db': _PKB_CLUSTER_DATABASE
    })
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertTrue(edw_local.IsUserManaged(spec))

  def testIsPkbManaged(self):
    kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertFalse(edw_local.IsUserManaged(spec))

  def testUserManagedGetClusterIdentifier(self):
    kwargs = copy.copy({
        'cluster_identifier': _PKB_CLUSTER,
        'db': _PKB_CLUSTER_DATABASE
    })
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertEqual(_PKB_CLUSTER, edw_local.GetClusterIdentifier(spec))
    self.assertEqual(_PKB_CLUSTER, edw_local.cluster_identifier)

  def testPkbManagedGetClusterIdentifier(self):
    kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertEqual('pkb-' + FLAGS.run_uri,
                     edw_local.GetClusterIdentifier(spec))
    self.assertEqual('pkb-' + FLAGS.run_uri, edw_local.cluster_identifier)


if __name__ == '__main__':
  unittest.main()
