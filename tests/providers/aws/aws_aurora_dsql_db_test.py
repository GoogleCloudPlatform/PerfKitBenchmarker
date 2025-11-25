# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for Aurora DSQL resource."""

import contextlib
import json
import unittest

from absl import flags
import mock
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_aurora_dsql_db
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

AURORA_DSQL_POSTGRES = sql_engine_utils.AURORA_DSQL_POSTGRES
FLAGS = flags.FLAGS

_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'


class AwsAuroraDsqlRelationalDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].value = '123'
    self.issue_command_stdout = ''
    self.issue_command_stderr = ''
    self.issue_command_retcode = 0

  @contextlib.contextmanager
  def _PatchIssueCommand(self, stdout='', stderr='', return_code=0):
    """A context manager that patches IssueCommand."""
    self.issue_command_stdout = stdout
    self.issue_command_stderr = stderr
    self.issue_command_retcode = return_code
    retval = (
        self.issue_command_stdout,
        self.issue_command_stderr,
        self.issue_command_retcode,
    )
    with mock.patch(
        vm_util.__name__ + '.IssueCommand', return_value=retval
    ) as issue_command:
      yield issue_command

  def CreateMockSpec(self):
    """Creates a mock relational DB spec."""
    spec_dict = {
        'engine': AURORA_DSQL_POSTGRES,
        'engine_version': '16.2',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'db_spec': mock.MagicMock(),
        'enable_freeze_restore': False,
        'create_on_restore_error': False,
        'delete_on_freeze_error': False,
        'zones': ['us-east-1a'],
        'high_availability': True,
        'backup_enabled': True,
        'vm_groups': {
            'clients': mock.Mock(vm_spec=mock.Mock(zone='us-east-1a'))
        },
    }
    mock_db_spec = mock.Mock(spec=relational_db_spec.RelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def CreateDbFromSpec(self):
    """Creates a mock Aurora DSQL DB."""
    db = aws_aurora_dsql_db.AwsAuroraDsqlRelationalDb(self.CreateMockSpec())
    db.region = 'us-east-1'
    return db

  def testCreate(self):
    """Tests that the create command is correct."""
    create_response = {'identifier': 'fake_cluster_id'}
    with self._PatchIssueCommand(
        stdout=json.dumps(create_response)
    ) as issue_command:
      db = self.CreateDbFromSpec()
      db._Create()
      self.assertEqual(db.cluster_id, 'fake_cluster_id')
      command_args = issue_command.call_args[0][0]
      self.assertEqual(command_args[0], 'aws')
      self.assertEqual(command_args[3], 'dsql')
      self.assertEqual(command_args[4], 'create-cluster')
      self.assertIn('--no-deletion-protection-enabled', command_args)
      self.assertIn('--tags', command_args)

  def testMakeDsqlTags(self):
    """Tests that DSQL tags are formatted correctly."""
    with mock.patch.object(
        util,
        'MakeDefaultTags',
        return_value={'tag1': 'value1', 'tag2': 'value2'},
    ):
      db = self.CreateDbFromSpec()
      tags = db._MakeDsqlTags()
      self.assertEqual(tags, ['tag1=value1,tag2=value2'])

  def testDescribeCluster(self):
    """Tests that the describe cluster command is correct."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with self._PatchIssueCommand(
        stdout=json.dumps(describe_response)
    ) as issue_command:
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      result = db._DescribeCluster()
      self.assertEqual(result, describe_response)
      command_args = issue_command.call_args[0][0]
      self.assertEqual(command_args[4], 'get-cluster')
      self.assertIn('--identifier=fake_cluster_id', command_args)

  def testIsReadyActive(self):
    """Tests that _IsReady returns true when cluster is active."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with self._PatchIssueCommand(stdout=json.dumps(describe_response)):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertTrue(db._IsReady())

  def testIsReadyNotActive(self):
    """Tests that _IsReady returns false when cluster is not active."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'CREATING'}
    with self._PatchIssueCommand(stdout=json.dumps(describe_response)):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertFalse(db._IsReady())

  def testExists(self):
    """Tests that _Exists returns true when cluster exists."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with self._PatchIssueCommand(stdout=json.dumps(describe_response)):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertTrue(db._Exists())

  def testDoesNotExist(self):
    """Tests that _Exists returns false when cluster does not exist."""
    with self._PatchIssueCommand(return_code=1):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertFalse(db._Exists())

  def testDelete(self):
    """Tests that delete command is correct and polling works."""
    db = self.CreateDbFromSpec()
    db.cluster_id = 'fake_cluster_id'

    with mock.patch.object(
        vm_util, 'IssueCommand'
    ) as issue_command, mock.patch.object(
        db, '_Exists', side_effect=[True, False]
    ):
      db._Delete()
      command_args = issue_command.call_args[0][0]
      self.assertEqual(command_args[4], 'delete-cluster')
      self.assertIn('--identifier=fake_cluster_id', command_args)

  def testGetDefaultEngineVersion(self):
    """Tests that correct default engine version is returned."""
    self.assertEqual(
        aws_aurora_dsql_db.AwsAuroraDsqlRelationalDb.GetDefaultEngineVersion(
            AURORA_DSQL_POSTGRES
        ),
        '16.2',
    )

  def testGetResourceMetadata(self):
    """Tests that cluster ID is added to metadata."""
    db = self.CreateDbFromSpec()
    db.cluster_id = 'fake_cluster_id'
    metadata = db.GetResourceMetadata()
    self.assertEqual(metadata['dsql_cluster_id'], 'fake_cluster_id')


if __name__ == '__main__':
  unittest.main()
