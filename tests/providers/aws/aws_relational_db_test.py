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

import contextlib
import json
import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_relational_db
from perfkitbenchmarker.sql_engine_utils import AURORA_POSTGRES
from perfkitbenchmarker.sql_engine_utils import MYSQL
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None
_AWS_PREFIX = 'aws --output json'


def _ReadTestDataFile(filename):
  path = os.path.join(
      os.path.dirname(__file__), '../../data',
      filename)
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
    super(AwsRelationalDbTestCase, self).setUp()
    FLAGS['run_uri'].value = '123'
    FLAGS['use_managed_db'].parse(True)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=retval) as issue_command, mock.patch(
            builtins.__name__ + '.open'), mock.patch(vm_util.__name__ +
                                                     '.NamedTemporaryFile'):
      yield issue_command

  def VmGroupSpec(self):
    return {
        'clients': {
            'vm_spec': {
                'GCP': {
                    'zone': 'us-central1-c',
                    'machine_type': 'n1-standard-1'
                }
            },
            'disk_spec': {
                'GCP': {
                    'disk_size': 500,
                    'disk_type': 'pd-ssd'
                }
            }
        },
        'servers': {
            'vm_spec': {
                'GCP': {
                    'zone': 'us-central1-c',
                    'machine_type': 'n1-standard-1'
                }
            },
            'disk_spec': {
                'GCP': {
                    'disk_size': 500,
                    'disk_type': 'pd-ssd'
                }
            }
        }
    }

  def CreateMockSpec(self, additional_spec_items={}):
    default_server_db_disk_spec = aws_disk.AwsDiskSpec(
        _COMPONENT, disk_size=5, disk_type=aws_disk.IO1, iops=1000)

    default_server_db_spec = virtual_machine.BaseVmSpec(
        'NAME', **{
            'machine_type': 'db.t1.micro',
            'zone': 'us-west-2b'
        })
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
    }
    spec_dict.update(additional_spec_items)

    mock_db_spec = mock.Mock(spec=benchmark_config_spec._RelationalDbSpec)
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
    db_class.server_vm = m

  def CreateDbFromMockSpec(self, mock_spec):
    aws_db = aws_relational_db.AwsRelationalDb(mock_spec)

    # Set necessary instance attributes that are not part of the spec
    aws_db.security_group_name = 'fake_security_group'
    aws_db.db_subnet_group_name = 'fake_db_subnet'
    aws_db.security_group_id = 'fake_security_group_id'
    self.CreateMockClientVM(aws_db)
    return aws_db

  def CreateDbFromSpec(self, additional_spec_items={}):
    mock_spec = self.CreateMockSpec(additional_spec_items)
    return self.CreateDbFromMockSpec(mock_spec)

  def Create(self, additional_spec_items={}):
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateDbFromSpec(additional_spec_items)
      db._Create()
      return ' '.join(issue_command.call_args[0][0])

  def testCreate(self):
    command_string = self.Create()

    self.assertTrue(
        command_string.startswith('%s rds create-db-instance' % _AWS_PREFIX))
    self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                  command_string)
    self.assertIn('--db-instance-class=db.t1.micro', command_string)
    self.assertIn('--engine=mysql', command_string)
    self.assertIn('--master-user-password=fakepassword', command_string)

  def testCorrectVmGroupsPresent(self):
    with self._PatchCriticalObjects():
      db = self.CreateDbFromSpec()
      self.CreateMockServerVM(db)
      db._Create()
      vms = relational_db.VmsToBoot(db.spec.vm_groups)
      self.assertNotIn('servers', vms)

  def CreateAuroraMockSpec(self, additional_spec_items={}):
    default_server_db_spec = virtual_machine.BaseVmSpec(
        'NAME', **{
            'machine_type': 'db.t1.micro',
            'zone': 'us-west-2b'
        })

    spec_dict = {
        'engine': AURORA_POSTGRES,
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'database_username': 'fakeusername',
        'db_spec': default_server_db_spec,
        'zones': ['us-east-1a', 'us-east-1d'],
        'engine_version': '9.6.2',
        'high_availability': True
    }
    spec_dict.update(additional_spec_items)

    mock_db_spec = mock.Mock(spec=benchmark_config_spec._RelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def CreateAuroraDbFromSpec(self, additional_spec_items={}):
    mock_spec = self.CreateAuroraMockSpec(additional_spec_items)
    return self.CreateDbFromMockSpec(mock_spec)

  def CreateAurora(self, additional_spec_items={}):
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateAuroraDbFromSpec(additional_spec_items)
      db._Create()
      call_results = []
      for call in issue_command.call_args_list:
        call_results.append(' '.join(call[0][0]))
      return call_results

  def testCreateAurora(self):
    command_strings = self.CreateAurora()

    self.assertIn(
        '%s rds create-db-cluster' % _AWS_PREFIX, command_strings[0])
    self.assertIn('--db-cluster-identifier=pkb-db-cluster-123',
                  command_strings[0])
    self.assertIn('--engine=aurora-postgresql', command_strings[0])
    self.assertIn('--master-user-password=fakepassword', command_strings[0])

    self.assertIn(
        '%s rds create-db-instance' % _AWS_PREFIX, command_strings[1])
    self.assertIn('--db-cluster-identifier=pkb-db-cluster-123',
                  command_strings[1])
    self.assertIn('--engine=aurora-postgresql', command_strings[1])

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
        'db_disk_spec':
            aws_disk.AwsDiskSpec(
                _COMPONENT, disk_size=5, disk_type=aws_disk.GP2)
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

  def testParseEndpoint(self):
    test_data = _ReadTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects():
      db = self.CreateDbFromSpec()

      self.assertEqual(
          'pkb-db-instance-a4499926.cqxeajwjbqne.us-west-2.rds.amazonaws.com',
          db._ParseEndpointFromInstance(json.loads(test_data)))

  def testParsePort(self):
    test_data = _ReadTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects():
      db = self.CreateDbFromSpec()

      self.assertEqual(3306, db._ParsePortFromInstance(json.loads(test_data)))

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateDbFromSpec()
      db.all_instance_ids.append('pkb-db-instance-123')
      db._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('aws --output json rds delete-db-instance', command_string)
      self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                    command_string)
      self.assertIn('--skip-final-snapshot', command_string)

  @mock.patch.object(aws_network.AwsFirewall, '_RuleExists', return_value=True,
                     autospec=True)
  def testCreateUnmanagedDb(self, rule_check_mock):  # pylint: disable=unused-argument
    FLAGS['use_managed_db'].parse(False)
    FLAGS['innodb_buffer_pool_size'].parse(100)
    FLAGS['innodb_log_file_size'].parse(1000)
    FLAGS['default_timeout'].parse(300)
    FLAGS['mysql_bin_log'].parse(False)
    FLAGS['ip_addresses'].parse('REACHABLE')
    FLAGS['postgres_shared_buffer_size'].parse(100)
    with self._PatchCriticalObjects() as issue_command:
      db = self.CreateDbFromSpec()
      self.CreateMockServerVM(db)
      db._Create()
      self.assertTrue(db._Exists())
      self.assertTrue(hasattr(db, 'firewall'))
      self.assertEqual(db.endpoint, db.server_vm.ip_address)
      self.assertEqual(db.spec.database_username, 'root')
      self.assertEqual(db.spec.database_password, 'perfkitbenchmarker')
      self.assertIsNone(issue_command.call_args)


if __name__ == '__main__':
  unittest.main()
