# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_relational_db."""

import builtins
import contextlib
import datetime
import json
import os
import textwrap
import unittest

from absl import flags
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_aurora_db  # pylint: disable=unused-import
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_rds_db  # pylint: disable=unused-import
from perfkitbenchmarker.providers.aws import aws_relational_db
from perfkitbenchmarker.sql_engine_utils import AURORA_POSTGRES
from perfkitbenchmarker.sql_engine_utils import MYSQL
from tests import matchers
from tests import pkb_common_test_case


FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None
_AWS_PREFIX = 'aws --output json'


def _ReadTestDataFile(filename):
  path = os.path.join(os.path.dirname(__file__), '../../data', filename)
  with open(path) as fp:
    return fp.read()


class AwsRelationalDbSpecTestCase(pkb_common_test_case.PkbCommonTestCase):
  """Class that tests the creation of an AwsRelationalDbSpec."""

  pass


class AwsRelationalDbFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):
  """Class that tests the flags defined in AwsRelationalDb."""

  pass


class AwsRelationalDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].value = '123'
    FLAGS['use_managed_db'].parse(True)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand', return_value=retval
    ) as issue_command, mock.patch(builtins.__name__ + '.open'), mock.patch(
        vm_util.__name__ + '.NamedTemporaryFile'
    ):
      yield issue_command

  def VmGroupSpec(self):
    return {
        'clients': {
            'vm_spec': {
                'GCP': {
                    'zone': 'us-central1-c',
                    'machine_type': 'n1-standard-1',
                }
            },
            'disk_spec': {'GCP': {'disk_size': 500, 'disk_type': 'pd-ssd'}},
        },
        'servers': {
            'vm_spec': {
                'GCP': {
                    'zone': 'us-central1-c',
                    'machine_type': 'n1-standard-1',
                }
            },
            'disk_spec': {'GCP': {'disk_size': 500, 'disk_type': 'pd-ssd'}},
        },
    }

  def CreateMockSpec(self, additional_spec_items=None):
    default_server_db_disk_spec = aws_disk.AwsDiskSpec(
        _COMPONENT, disk_size=5, disk_type=aws_disk.IO1, provisioned_iops=1000
    )

    default_server_db_spec = virtual_machine.BaseVmSpec(
        'NAME', **{'machine_type': 'db.t1.micro', 'zone': 'us-west-2b'}
    )
    spec_dict = {
        'engine': MYSQL,
        'engine_version': '5.7.11',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'database_username': 'fakeusername',
        'high_availability': False,
        'db_spec': default_server_db_spec,
        'db_disk_spec': default_server_db_disk_spec,
        'vm_groups': self.VmGroupSpec(),
        'enable_freeze_restore': False,
        'create_on_restore_error': False,
        'delete_on_freeze_error': False,
        'db_flags': '',
        'backup_enabled': True,
    }
    if additional_spec_items:
      spec_dict.update(additional_spec_items)

    mock_db_spec = mock.Mock(spec=relational_db_spec.RelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def CreateMockClientVM(self, db_class):
    m = mock.MagicMock()
    m.HasIpAddress = True
    m.ip_address = '192.168.0.1'
    db_class.client_vm = m

  def CreateMockServerVM(self, db_class):
    m = mock.MagicMock()
    m.HasIpAddress = True
    m.ip_address = '192.168.2.1'
    m.internal_ip = '192.168.2.3'
    db_class.server_vm = m

  def CreateIAASDbFromSpec(self):
    mock_spec = self.CreateMockSpec()
    aws_db = aws_relational_db.AWSMysqlIAASRelationalDb(mock_spec)
    self.CreateMockClientVM(aws_db)
    self.CreateMockServerVM(aws_db)
    return aws_db

  def CreateDbFromMockSpec(self, mock_spec):
    cls = relational_db.GetRelationalDbClass('AWS', True, mock_spec.engine)
    aws_db = cls(mock_spec)

    # Set necessary instance attributes that are not part of the spec
    aws_db.security_group_name = 'fake_security_group'
    aws_db.db_subnet_group_name = 'fake_db_subnet'
    aws_db.security_group_id = 'fake_security_group_id'
    self.CreateMockClientVM(aws_db)
    return aws_db

  def CreateDbFromSpec(self, additional_spec_items=None):
    mock_spec = self.CreateMockSpec(additional_spec_items)
    return self.CreateDbFromMockSpec(mock_spec)

  def Create(self, additional_spec_items=None):
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateDbFromSpec(additional_spec_items)
      db._Create()
      return ' '.join(issue_command.call_args[0][0])

  def testCreate(self):
    command_string = self.Create()

    self.assertTrue(
        command_string.startswith('%s rds create-db-instance' % _AWS_PREFIX)
    )
    self.assertIn(
        '--db-instance-identifier=pkb-db-instance-123', command_string
    )
    self.assertIn('--db-instance-class=db.t1.micro', command_string)
    self.assertIn('--engine=mysql', command_string)
    self.assertIn('--master-user-password=fakepassword', command_string)

  def testCorrectVmGroupsPresent(self):
    with self._PatchCriticalObjects():
      db = self.CreateIAASDbFromSpec()
      db._Create()
      vms = relational_db.VmsToBoot(db.spec.vm_groups)
      self.assertNotIn('servers', vms)

  def CreateAuroraMockSpec(self, additional_spec_items=None):
    default_server_db_spec = virtual_machine.BaseVmSpec(
        'NAME', **{'machine_type': 'db.t1.micro', 'zone': 'us-west-2b'}
    )

    spec_dict = {
        'engine': AURORA_POSTGRES,
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'database_username': 'fakeusername',
        'db_spec': default_server_db_spec,
        'zones': ['us-east-1a', 'us-east-1d'],
        'engine_version': '9.6.2',
        'high_availability': True,
        'enable_freeze_restore': False,
        'create_on_restore_error': False,
        'delete_on_freeze_error': False,
        'load_machine_type': None,
    }
    if additional_spec_items:
      spec_dict.update(additional_spec_items)

    mock_db_spec = mock.Mock(spec=relational_db_spec.RelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def CreateAuroraDbFromSpec(self, additional_spec_items=None):
    mock_spec = self.CreateAuroraMockSpec(additional_spec_items)
    return self.CreateDbFromMockSpec(mock_spec)

  def CreateAurora(self, additional_spec_items=None):
    if additional_spec_items is None:
      additional_spec_items = {}
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateAuroraDbFromSpec(additional_spec_items)
      db._Create()
      call_results = []
      for call in issue_command.call_args_list:
        call_results.append(' '.join(call[0][0]))
      return call_results

  def testCreateAurora(self):
    command_strings = self.CreateAurora()
    self.assertListEqual(
        command_strings,
        [
            (
                'aws --output json rds create-db-cluster'
                ' --db-cluster-identifier=pkb-db-cluster-123'
                ' --engine=aurora-postgresql --engine-version=9.6.2'
                ' --master-username=fakeusername'
                ' --master-user-password=fakepassword --region=us-east-1'
                ' --db-subnet-group-name=fake_db_subnet'
                ' --vpc-security-group-ids=fake_security_group_id'
                ' --availability-zones=us-east-1a --storage-type=aurora'
                ' --backup-retention-period=1 --tags'
            ),
            (
                'aws --output json rds create-db-instance'
                ' --db-instance-identifier=pkb-db-instance-123'
                ' --db-cluster-identifier=pkb-db-cluster-123'
                ' --engine=aurora-postgresql --engine-version=9.6.2'
                ' --no-auto-minor-version-upgrade'
                ' --db-instance-class=db.t1.micro --region=us-east-1'
                ' --availability-zone=us-east-1a --tags'
            ),
            (
                'aws --output json rds create-db-instance'
                ' --db-instance-identifier=pkb-db-instance-123-us-east-1d'
                ' --db-cluster-identifier=pkb-db-cluster-123'
                ' --engine=aurora-postgresql --engine-version=9.6.2'
                ' --no-auto-minor-version-upgrade'
                ' --db-instance-class=db.t1.micro --region=us-east-1'
                ' --availability-zone=us-east-1d --tags'
            ),
        ],
    )

  def testNoHighAvailability(self):
    spec_dict = {
        'multi_az': False,
    }
    command_string = self.Create(spec_dict)

    self.assertNotIn('--multi-az', command_string)

  def testHighAvailability(self):
    command_string = self.Create()

    self.assertNotIn('--multi-az', command_string)

  def testDiskWithIops(self):
    command_string = self.Create()

    self.assertIn('--allocated-storage=5', command_string)
    self.assertIn('--storage-type=%s' % aws_disk.IO1, command_string)
    self.assertIn('--iops=1000', command_string)

  def testDiskWithoutIops(self):
    spec_dict = {
        'db_disk_spec': aws_disk.AwsDiskSpec(
            _COMPONENT, disk_size=5, disk_type=aws_disk.GP2
        )
    }
    command_string = self.Create(spec_dict)

    self.assertIn('--allocated-storage=5', command_string)
    self.assertIn('--storage-type=%s' % aws_disk.GP2, command_string)
    self.assertNotIn('--iops', command_string)

  def testUnspecifiedDatabaseVersion(self):
    command_string = self.Create()

    self.assertIn('--engine-version=5.7.11', command_string)

  def testSpecifiedDatabaseVersion(self):
    spec_dict = {
        'engine_version': '5.6.29',
    }
    command_string = self.Create(spec_dict)

    self.assertIn('--engine-version=5.6.29', command_string)

  def testIsNotReady(self):
    test_data = _ReadTestDataFile('aws-describe-db-instances-creating.json')
    with self._PatchCriticalObjects(stdout=test_data):
      db = self.CreateDbFromSpec()
      db.all_instance_ids.append('pkb-db-instance-123')

      self.assertEqual(False, db._IsReady(timeout=0))

  def testIsReady(self):
    test_data = _ReadTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects(stdout=test_data):
      db = self.CreateDbFromSpec()
      db.all_instance_ids.append('pkb-db-instance-123')

      self.assertEqual(True, db._IsReady())

  def testSetEndpoint(self):
    test_data = _ReadTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects(stdout=test_data):
      db = self.CreateDbFromSpec()
      db._SetEndpoint()

      self.assertEqual(
          'pkb-db-instance-a4499926.cqxeajwjbqne.us-west-2.rds.amazonaws.com',
          db.endpoint,
      )

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateDbFromSpec()
      db.all_instance_ids.append('pkb-db-instance-123')
      db._InstanceExists = mock.MagicMock(return_value=False)
      db._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('aws --output json rds delete-db-instance', command_string)
      self.assertIn(
          '--db-instance-identifier=pkb-db-instance-123', command_string
      )
      self.assertIn('--skip-final-snapshot', command_string)

  def testCreateUnmanagedDb(self):
    FLAGS['use_managed_db'].parse(False)
    FLAGS['innodb_buffer_pool_size'].parse(100)
    FLAGS['innodb_log_file_size'].parse(1000)
    FLAGS['default_timeout'].parse(300)
    FLAGS['mysql_bin_log'].parse(False)
    FLAGS['ip_addresses'].parse('REACHABLE')
    FLAGS['postgres_shared_buffer_size'].parse(100)
    FLAGS['db_flags'].parse('')
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateIAASDbFromSpec()
      self.CreateMockServerVM(db)
      db._Create()
      self.assertTrue(db._Exists())
      self.assertEqual(db.spec.database_username, 'root')
      self.assertEqual(db.spec.database_password, 'perfkitbenchmarker')
      self.assertIsNone(issue_command.call_args)
      db._PostCreate()
      self.assertEqual(db.endpoint, db.server_vm.internal_ip)

  def testUpdateClusterClass(self):
    fake_describe_output = textwrap.dedent("""
    {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "mydbinstance",
                "DBInstanceClass": "db.t3.small",
                "Engine": "mysql",
                "DBInstanceStatus": "available",
                "MasterUsername": "masterawsuser",
                "Endpoint": {
                    "Address": "mydbinstancecf.abcexample.us-east-1.rds.amazonaws.com",
                    "Port": 3306,
                    "HostedZoneId": "Z2R2ITUGPM61AM"
                }
            }
        ]
    }
    """)
    db = self.CreateAuroraDbFromSpec()
    db.all_instance_ids.append('mydbinstance')
    mock_issue_command = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )
    self.enter_context(
        mock.patch.object(db, '_IsInstanceReady', return_value=True)
    )
    self.enter_context(
        mock.patch.object(
            db,
            '_DescribeInstance',
            return_value=json.loads(fake_describe_output),
        )
    )

    db._UpdateClusterClass('db.m5.large')

    mock_issue_command.assert_called_once_with(
        matchers.HAS('--db-instance-class=db.m5.large'),
        raise_on_failure=mock.ANY,
    )

  def testUpdateClusterClassNotChanged(self):
    fake_describe_output = textwrap.dedent("""
    {
        "DBInstances": [
            {
                "DBInstanceIdentifier": "mydbinstance",
                "DBInstanceClass": "db.t3.small",
                "Engine": "mysql",
                "DBInstanceStatus": "available",
                "MasterUsername": "masterawsuser",
                "Endpoint": {
                    "Address": "mydbinstancecf.abcexample.us-east-1.rds.amazonaws.com",
                    "Port": 3306,
                    "HostedZoneId": "Z2R2ITUGPM61AM"
                }
            }
        ]
    }
    """)
    db = self.CreateAuroraDbFromSpec()
    db.all_instance_ids.append('mydbinstance')
    mock_issue_command = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )
    self.enter_context(
        mock.patch.object(
            db,
            '_DescribeInstance',
            return_value=json.loads(fake_describe_output),
        )
    )

    db._UpdateClusterClass('db.t3.small')

    mock_issue_command.assert_not_called()

  def testCollectMetrics(self):
    db = self.CreateDbFromSpec()
    db.instance_id = 'pkb-db-instance-123'
    db.region = 'us-west-2'

    # Mock the response from AWS CloudWatch
    mock_response = {
        'Datapoints': [
            {
                'Timestamp': 1766084460,
                'Average': 10.0,
            },
            {
                'Timestamp': 1766084400,
                'Average': 20.0,
            },
        ]
    }
    self.enter_context(
        mock.patch.object(
            aws_relational_db.util,
            'IssueRetryableCommand',
            return_value=(json.dumps(mock_response), ''),
        )
    )

    start_time = datetime.datetime(2025, 11, 26, 10, 0, 0)
    end_time = datetime.datetime(2025, 11, 26, 10, 1, 0)
    samples = db.CollectMetrics(start_time, end_time)

    # Spot check a few sample values
    sample_names = [s.metric for s in samples]
    self.assertIn('cpu_utilization_average', sample_names)
    self.assertIn('cpu_utilization_min', sample_names)
    self.assertIn('cpu_utilization_max', sample_names)
    self.assertIn('disk_read_iops_average', sample_names)

    cpu_avg = next(s for s in samples if s.metric == 'cpu_utilization_average')
    self.assertEqual(cpu_avg.value, 15.0)
    self.assertEqual(cpu_avg.unit, '%')

    cpu_min = next(s for s in samples if s.metric == 'cpu_utilization_min')
    self.assertEqual(cpu_min.value, 10.0)
    self.assertEqual(cpu_min.unit, '%')

    cpu_max = next(s for s in samples if s.metric == 'cpu_utilization_max')
    self.assertEqual(cpu_max.value, 20.0)
    self.assertEqual(cpu_max.unit, '%')


if __name__ == '__main__':
  unittest.main()
