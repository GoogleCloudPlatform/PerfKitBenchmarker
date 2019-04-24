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

"""Tests for perfkitbenchmarker.providers.gcp.gcp_managed_relational_db."""

import contextlib
import json
import os
import unittest
import mock

from perfkitbenchmarker import disk
from perfkitbenchmarker import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.managed_relational_db import MYSQL
from perfkitbenchmarker.managed_relational_db import POSTGRES
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gcp_managed_relational_db
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case
from six.moves import builtins

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'


def CreateManagedDbFromSpec(spec_dict):
  mock_db_spec = mock.Mock(
      spec=benchmark_config_spec._ManagedRelationalDbSpec)
  mock_db_spec.configure_mock(**spec_dict)
  db_class = gcp_managed_relational_db.GCPManagedRelationalDb(mock_db_spec)
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


class GcpMysqlManagedRelationalDbTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def createMySQLSpecDict(self):
    vm_spec = virtual_machine.BaseVmSpec('NAME',
                                         **{
                                             'machine_type': 'db-n1-standard-1',
                                             'zone': 'us-west1-b',
                                         })
    vm_spec.cpus = None
    vm_spec.memory = None
    disk_spec = disk.BaseDiskSpec('NAME', **{'disk_size': 50})
    return {
        'engine': MYSQL,
        'engine_version': '5.7',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'vm_spec': vm_spec,
        'disk_spec': disk_spec,
        'high_availability': False,
        'backup_enabled': True,
        'backup_start_time': '07:00',
    }

  def setUp(self):
    super(GcpMysqlManagedRelationalDbTestCase, self).setUp()
    FLAGS.project = ''
    FLAGS.run_uri = '123'
    FLAGS.gcloud_path = 'gcloud'

    mock_db_spec_attrs = self.createMySQLSpecDict()
    self.mock_db_spec = mock.Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    self.mock_db_spec.configure_mock(**mock_db_spec_attrs)

  def testNoHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      db = CreateManagedDbFromSpec(self.createMySQLSpecDict())
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertNotIn('--failover-replica-name', command_string)
      self.assertNotIn('replica-pkb-db-instance-123', command_string)

  def testCreate(self):
    with PatchCriticalObjects() as issue_command:
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Create()
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

  def testCreateWithBackupDisabled(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.mock_db_spec
      spec.backup_enabled = False
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Create()
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
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Delete()
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
      db = CreateManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._IsReady())

  def testExists(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with PatchCriticalObjects(stdout=test_output):
      db = CreateManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._Exists())

  def testHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.createMySQLSpecDict()
      spec['high_availability'] = True
      db = CreateManagedDbFromSpec(spec)
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
      db = CreateManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual('', db._ParseEndpoint(None))
      self.assertIn('10.10.0.35',
                    db._ParseEndpoint(json.loads(test_output)))


class GcpPostgresManagedRelationlDbTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpPostgresManagedRelationlDbTestCase, self).setUp()
    FLAGS.project = ''
    FLAGS.run_uri = ''
    FLAGS.gcloud_path = ''

  def createPostgresSpecDict(self):
    machine_type = {
        'machine_type': {'cpus': 1, 'memory': '3840MiB'},
        'zone': 'us-west1-b',
    }
    vm_spec = gce_virtual_machine.GceVmSpec('NAME', **machine_type)
    disk_spec = disk.BaseDiskSpec('NAME', **{'disk_size': 50})
    return {
        'engine': POSTGRES,
        'engine_version': '5.7',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'vm_spec': vm_spec,
        'disk_spec': disk_spec,
        'high_availability': False,
        'backup_enabled': True,
        'backup_start_time': '07:00'
    }

  def testValidateSpec(self):
    with PatchCriticalObjects():
      db_postgres = CreateManagedDbFromSpec(self.createPostgresSpecDict())
      db_postgres._ValidateSpec()

  def testValidateMachineType(self):
    with PatchCriticalObjects():
      db = CreateManagedDbFromSpec(self.createPostgresSpecDict())
      self.assertRaises(ValueError, db._ValidateMachineType, 0, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 3840, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 255, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 256000000000, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 2560, 1)
      db._ValidateMachineType(db.spec.vm_spec.memory,
                              db.spec.vm_spec.cpus)

  def testCreateNonHighAvailability(self):
    with PatchCriticalObjects() as issue_command:
      spec = self.createPostgresSpecDict()
      spec['engine'] = 'postgres'
      spec['engine_version'] = '9.6'
      db = CreateManagedDbFromSpec(spec)
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
      db = CreateManagedDbFromSpec(spec)
      db._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--availability-type=REGIONAL', command_string)


if __name__ == '__main__':
  unittest.main()
