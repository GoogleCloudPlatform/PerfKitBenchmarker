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

import inspect
import json
import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import errors
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
    self.enter_context(
        mock.patch.object(util, 'GetAccount', return_value='123456789012')
    )

  def CreateMockSpec(self):
    """Creates a mock relational DB spec."""
    spec_dict = {
        'engine': AURORA_DSQL_POSTGRES,
        'engine_version': '16.2',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_username': 'fakeusername',
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
        'db_disk_spec': None,
        'db_flags': None,
    }
    mock_db_spec = mock.Mock(spec=aws_aurora_dsql_db.AwsAuroraDsqlSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def CreateDbFromSpec(self):
    """Creates a mock Aurora DSQL DB."""
    db = aws_aurora_dsql_db.AwsAuroraDsqlRelationalDb(self.CreateMockSpec())
    db.region = 'us-east-1'
    db.client_vm = mock.MagicMock()
    db.client_vm.OS_TYPE = 'ubuntu2204'
    return db

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn='arn:foo')
  def testCreateFromBackup(self):
    """Tests that the create from backup command is correct."""
    db = self.CreateDbFromSpec()
    start_restore_response = '{"RestoreJobId": "1"}'
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(start_restore_response, '', 0),
    ) as issue_command:
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      start_restore_call = issue_command.call_args_list[0]
      self.assertIn('start-restore-job', start_restore_call[0][0])
      self.assertIn('backup', start_restore_call[0][0])
      self.assertIn('--recovery-point-arn', start_restore_call[0][0])
      self.assertIn('arn:foo', start_restore_call[0][0])
      self.assertIn('--region', start_restore_call[0][0])
      self.assertIn('us-east-1', start_restore_call[0][0])
      self.assertIn('--iam-role-arn', start_restore_call[0][0])
      self.assertIn(
          'arn:aws:iam::123456789012:role/service-role/AWSBackupDefaultServiceRole',
          start_restore_call[0][0],
      )
      self.assertIn('--metadata', start_restore_call[0][0])
      self.assertIn(
          '{"regionalConfig": "[{\\"region\\": \\"us-east-1\\",'
          ' \\"isDeletionProtectionEnabled\\": false}]"}',
          start_restore_call[0][0],
      )
      self.assertEqual(db.restore_job_id, '1')

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn=None)
  @mock.patch.object(util, 'MakeDefaultTags', return_value={})
  def testCreateFromRaw(self, mock_make_tags):
    """Tests that the create from raw command is correct."""
    db = self.CreateDbFromSpec()
    create_cluster_response = '{"identifier": "cluster-id"}'
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(create_cluster_response, '', 0),
    ) as issue_command:
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      create_cluster_call = issue_command.call_args_list[0]
      self.assertIn('create-cluster', create_cluster_call[0][0])
      self.assertIn('dsql', create_cluster_call[0][0])
      self.assertIn('--region', create_cluster_call[0][0])
      self.assertIn('us-east-1', create_cluster_call[0][0])
      self.assertIn(
          '--no-deletion-protection-enabled', create_cluster_call[0][0]
      )
      self.assertIn('--tags', create_cluster_call[0][0])
      self.assertEqual(db.cluster_id, 'cluster-id')

  def testMakeDsqlTags(self):
    """Tests that DSQL tags are formatted correctly."""
    with mock.patch.object(
        util,
        'MakeDefaultTags',
        return_value={'tag1': 'value1', 'tag2': 'value2'},
    ):
      db = self.CreateDbFromSpec()
      tags = db._MakeDsqlTags()
      self.assertEqual(tags, ['tag1=value1,tag2=value2,Name=pkb-123'])

  def testDescribeCluster(self):
    """Tests that the describe cluster command is correct."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(json.dumps(describe_response), '', 0),
    ) as issue_command:
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      result = db._DescribeCluster()
      self.assertEqual(result, describe_response)
      command_args = issue_command.call_args[0][0]
      self.assertEqual(command_args[4], 'get-cluster')
      self.assertIn('--identifier=fake_cluster_id', command_args)
      self.assertIn('--region', command_args)
      self.assertIn('us-east-1', command_args)

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn=None)
  def testIsReadyFromRawActive(self):
    """Tests that _IsReady returns true when cluster is active."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(json.dumps(describe_response), '', 0),
    ):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertTrue(db._IsReady())

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn=None)
  def testIsReadyFromRawNotActive(self):
    """Tests that _IsReady returns false when cluster is not active."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'CREATING'}
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(json.dumps(describe_response), '', 0),
    ):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertFalse(db._IsReady())

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn='arn:foo')
  def testIsReadyFromBackupCompleted(self):
    """Tests _IsReady with backup restore completed."""
    db = self.CreateDbFromSpec()
    db.restore_job_id = '1'
    describe_restore_response = (
        '{"Status": "COMPLETED", "CreatedResourceArn":'
        ' "arn:aws:dsql:us-east-1:123456789012:cluster/restored-cluster-id"}'
    )
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(describe_restore_response, '', 0),
    ) as issue_command:
      self.assertTrue(db._IsReady())
      self.assertEqual(db.cluster_id, 'restored-cluster-id')
      self.assertEqual(
          db.cluster_arn,
          'arn:aws:dsql:us-east-1:123456789012:cluster/restored-cluster-id',
      )
      issue_command.assert_called_once()
      restore_call = issue_command.call_args[0][0]
      self.assertIn('describe-restore-job', restore_call)
      self.assertIn('--region', restore_call)
      self.assertIn('us-east-1', restore_call)

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn='arn:foo')
  def testIsReadyFromBackupPending(self):
    """Tests _IsReady with backup restore pending."""
    db = self.CreateDbFromSpec()
    db.restore_job_id = '1'
    describe_restore_response = '{"Status": "PENDING"}'
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(describe_restore_response, '', 0),
    ):
      self.assertFalse(db._IsReady())

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn='arn:foo')
  def testIsReadyFromBackupFailed(self):
    """Tests _IsReady with backup restore failed."""
    db = self.CreateDbFromSpec()
    db.restore_job_id = '1'
    describe_restore_response = '{"Status": "FAILED"}'
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(describe_restore_response, '', 0),
    ):
      with self.assertRaises(errors.Resource.CreationError):
        db._IsReady()

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn='arn:foo')
  def testPostCreateFromBackup(self):
    """Tests _PostCreate with backup."""
    db = self.CreateDbFromSpec()
    db.cluster_arn = (
        'arn:aws:dsql:us-east-1:123456789012:cluster/restored-cluster-id'
    )
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=('', '', 0),
    ) as issue_command:
      db._PostCreate()
      self.assertEqual(issue_command.call_count, 1)
      tag_call = issue_command.call_args_list[0]
      self.assertIn('tag-resource', tag_call[0][0])
      self.assertIn('dsql', tag_call[0][0])
      self.assertIn('--region', tag_call[0][0])
      self.assertIn('us-east-1', tag_call[0][0])
      self.assertIn(
          '--resource-arn=arn:aws:dsql:us-east-1:123456789012:cluster/restored-cluster-id',
          tag_call[0][0],
      )

  @flagsaver.flagsaver(aws_aurora_dsql_recovery_point_arn=None)
  def testPostCreateFromRaw(self):
    """Tests _PostCreate with raw creation."""
    db = self.CreateDbFromSpec()
    with mock.patch.object(
        vm_util,
        'IssueCommand',
    ) as issue_command:
      db._PostCreate()
      issue_command.assert_not_called()

  def testExists(self):
    """Tests that _Exists returns true when cluster exists."""
    describe_response = {'identifier': 'fake_cluster_id', 'status': 'ACTIVE'}
    with mock.patch.object(
        vm_util,
        'IssueCommand',
        return_value=(json.dumps(describe_response), '', 0),
    ):
      db = self.CreateDbFromSpec()
      db.cluster_id = 'fake_cluster_id'
      self.assertTrue(db._Exists())

  def testDoesNotExist(self):
    """Tests that _Exists returns false when cluster does not exist."""
    with mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', 1)):
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
      self.assertIn('--region', command_args)
      self.assertIn('us-east-1', command_args)

  def testGetHostname(self):
    """Tests that hostname is formatted correctly."""
    db = self.CreateDbFromSpec()
    db.cluster_id = 'fake_cluster_id'
    self.assertEqual(db._GetHostname(), 'fake_cluster_id.dsql.us-east-1.on.aws')

  def testRunSqlQuery(self):
    """Tests that RunSqlQuery command is correct."""
    db = self.CreateDbFromSpec()
    db.cluster_id = 'fake_cluster_id'
    with mock.patch('boto3.client') as mock_boto3_client:
      mock_client = mock.MagicMock()
      mock_boto3_client.return_value = mock_client
      mock_client.generate_db_connect_admin_auth_token.return_value = 'token'
      db.RunSqlQuery('SELECT 1;')
      mock_boto3_client.assert_called_once_with('dsql', region_name='us-east-1')
      mock_client.generate_db_connect_admin_auth_token.assert_called_once_with(
          'fake_cluster_id.dsql.us-east-1.on.aws', 'us-east-1'
      )
      cmd = db.client_vm.RemoteCommand.call_args[0][0]
      self.assertStartsWith(cmd, 'PGSSLMODE=require psql')
      self.assertIn(
          "'host=fake_cluster_id.dsql.us-east-1.on.aws user=admin"
          " password=token dbname=postgres'",
          cmd,
      )
      self.assertIn('-c "SELECT 1;"', cmd)

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


class AwsAuroraDsqlSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].value = '123'
    FLAGS.ignore_package_requirements = True
    FLAGS.cloud = 'AWS'
    self.enter_context(
        mock.patch.object(util, 'GetAccount', return_value='123456789012')
    )

  def testDsqlSpecWithoutDiskSpecFromYaml(self):
    """Tests that DSQL spec can be created without disk spec."""
    yaml_string = inspect.cleandoc("""
    benchbase:
      description: Runs Benchbase benchmark.
      relational_db:
        cloud: AWS
        engine: aurora-dsql-postgres
        db_spec:
          AWS:
            zone: us-east-1a
    """)
    test_bm_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
        yaml_string=yaml_string, benchmark_name='benchbase'
    )
    test_bm_spec.ConstructRelationalDb()
    self.assertIsInstance(
        test_bm_spec.relational_db,
        aws_aurora_dsql_db.AwsAuroraDsqlRelationalDb,
    )


if __name__ == '__main__':
  unittest.main()
