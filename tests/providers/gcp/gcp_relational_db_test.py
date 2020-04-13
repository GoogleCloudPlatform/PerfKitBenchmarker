# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.providers.gcp.gcp_relational_db."""

import contextlib
import json
import os
import unittest
from absl import flags
import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gcp_relational_db
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.relational_db import MYSQL
from perfkitbenchmarker.relational_db import POSTGRES
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'


def CreateMockClientVM(db_class):
  m = mock.MagicMock()
  m.HasIpAddress = True
  m.ip_address = '192.168.0.1'
  db_class.client_vm = m


def CreateMockServerVM(db_class):
  m = mock.MagicMock()
  m.HasIpAddress = True
  m.ip_address = '192.168.2.1'
  db_class.server_vm = m


def CreateDbFromSpec(spec_dict):
  mock_db_spec = mock.Mock(spec=benchmark_config_spec._RelationalDbSpec)
  mock_db_spec.configure_mock(**spec_dict)
  db_class = gcp_relational_db.GCPRelationalDb(mock_db_spec)
  CreateMockClientVM(db_class)
  return db_class


@contextlib.contextmanager
def PatchCriticalObjects(stdout='', stderr='', return_code=0):
  """A context manager that patches a few critical objects with mocks."""
  retval = (stdout, stderr, return_code)
  with mock.patch(
      vm_util.__name__ + '.IssueCommand',
      return_value=retval) as issue_command, mock.patch(
          builtins.__name__ +
          '.open'), mock.patch(vm_util.__name__ +
                               '.NamedTemporaryFile'), mock.patch(
                                   util.__name__ + '.GetDefaultProject',
                                   return_value='fakeproject'):
    yield issue_command


def VmGroupSpec():
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


class GcpMysqlRelationalDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def createMySQLSpecDict(self):
    db_spec = virtual_machine.BaseVmSpec(
        'NAME', **{
            'machine_type': 'db-n1-standard-1',
            'zone': 'us-west1-b',
        })
    db_spec.cpus = None
    db_spec.memory = None
    db_disk_spec = disk.BaseDiskSpec('NAME', **{'disk_size': 50})
    return {
        'engine': MYSQL,
        'engine_version': '5.7',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'db_spec': db_spec,
        'db_disk_spec': db_disk_spec,
        'high_availability': False,
        'backup_enabled': True,
        'backup_start_time': '07:00',
        'vm_groups': VmGroupSpec(),
    }

  def setUp(self):
    super(GcpMysqlRelationalDbTestCase, self).setUp()
    FLAGS['run_uri'].parse('123')
    FLAGS['gcloud_path'].parse('gcloud')
    FLAGS['use_managed_db'].parse(True)

    mock_db_spec_attrs = self.createMySQLSpecDict()
    self.mock_db_spec = mock.Mock(spec=benchmark_config_spec._RelationalDbSpec)
    self.mock_db_spec.configure_mock(**mock_db_spec_attrs)

  def testNoHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertNotIn('--failover-replica-name', command_string)
      self.assertNotIn('replica-pkb-db-instance-123', command_string)

  def testCreate(self):
    with PatchCriticalObjects() as issue_command:
      db = gcp_relational_db.GCPRelationalDb(self.mock_db_spec)
      CreateMockClientVM(db)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertTrue(
          command_string.startswith(
              'gcloud beta sql instances create pkb-db-instance-123'),
          command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--tier=db-n1-standard-1', command_string)
      self.assertIn('--storage-size=50', command_string)
      self.assertIn('--backup', command_string)
      self.assertIn('--backup-start-time=07:00', command_string)
      self.assertIn('--zone=us-west1-b', command_string)

  def testCorrectVmGroupsPresent(self):
    with PatchCriticalObjects():
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      CreateMockServerVM(db)
      db._Create()
      vms = relational_db.VmsToBoot(db.spec.vm_groups)
      self.assertNotIn('servers', vms)

  def testCreateWithBackupDisabled(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.mock_db_spec
      spec.backup_enabled = False
      db = gcp_relational_db.GCPRelationalDb(self.mock_db_spec)
      CreateMockClientVM(db)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertTrue(
          command_string.startswith(
              'gcloud beta sql instances create pkb-db-instance-123'),
          command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--tier=db-n1-standard-1', command_string)
      self.assertIn('--no-backup', command_string)
      self.assertNotIn('--backup-start-time=07:00', command_string)

  def testDelete(self):
    with PatchCriticalObjects() as issue_command:
      db = gcp_relational_db.GCPRelationalDb(self.mock_db_spec)
      db._Delete()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud sql instances delete pkb-db-instance-123'))

  def testIsReady(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with PatchCriticalObjects(stdout=test_output):
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._IsReady())

  def testExists(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with PatchCriticalObjects(stdout=test_output):
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._Exists())

  def testHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.createMySQLSpecDict()
      spec['high_availability'] = True
      db = CreateDbFromSpec(spec)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--failover-replica-name', command_string)
      self.assertIn('replica-pkb-db-instance-123', command_string)

  def testParseEndpoint(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with PatchCriticalObjects():
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual('', db._ParseEndpoint(None))
      self.assertIn('10.10.0.35',
                    db._ParseEndpoint(json.loads(test_output)))

  def testCreateUnmanagedDb(self):
    FLAGS['use_managed_db'].parse(False)
    FLAGS['project'].parse('test')
    with PatchCriticalObjects() as issue_command:
      db = CreateDbFromSpec(self.createMySQLSpecDict())
      CreateMockServerVM(db)
      db._Create()
      self.assertTrue(db._Exists())
      self.assertTrue(hasattr(db, 'firewall'))
      self.assertEqual(db.endpoint, db.server_vm.ip_address)
      self.assertEqual(db.spec.database_username, 'root')
      self.assertEqual(db.spec.database_password, 'perfkitbenchmarker')
      self.assertIsNone(issue_command.call_args)


class GcpPostgresRelationlDbTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpPostgresRelationlDbTestCase, self).setUp()
    FLAGS.project = ''
    FLAGS.run_uri = ''
    FLAGS.gcloud_path = ''

  def createPostgresSpecDict(self):
    machine_type = {
        'machine_type': {'cpus': 1, 'memory': '3840MiB'},
        'zone': 'us-west1-b',
    }
    db_spec = gce_virtual_machine.GceVmSpec('NAME', **machine_type)
    db_disk_spec = disk.BaseDiskSpec('NAME', **{'disk_size': 50})
    return {
        'engine': POSTGRES,
        'engine_version': '5.7',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'db_spec': db_spec,
        'db_disk_spec': db_disk_spec,
        'high_availability': False,
        'backup_enabled': True,
        'backup_start_time': '07:00'
    }

  def testValidateSpec(self):
    with PatchCriticalObjects():
      db_postgres = CreateDbFromSpec(self.createPostgresSpecDict())
      db_postgres._ValidateSpec()

  def testValidateMachineType(self):
    with PatchCriticalObjects():
      db = CreateDbFromSpec(self.createPostgresSpecDict())
      self.assertRaises(ValueError, db._ValidateMachineType, 0, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 3840, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 255, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 256000000000, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 2560, 1)
      db._ValidateMachineType(db.spec.db_spec.memory, db.spec.db_spec.cpus)

  def testCreateNonHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.createPostgresSpecDict()
      spec['engine'] = 'postgres'
      spec['engine_version'] = '9.6'
      db = CreateDbFromSpec(spec)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertIn('database-version=POSTGRES_9_6', command_string)
      self.assertIn('--cpu=1', command_string)
      self.assertIn('--memory=3840MiB', command_string)
      self.assertNotIn('--availability-type=REGIONAL', command_string)

  def testCreateHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.createPostgresSpecDict()
      spec['high_availability'] = True
      spec['engine'] = 'postgres'
      spec['engine_version'] = '9.6'
      db = CreateDbFromSpec(spec)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--availability-type=REGIONAL', command_string)


if __name__ == '__main__':
  unittest.main()
