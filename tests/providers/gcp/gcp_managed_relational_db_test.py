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

"""Tests for perfkitbenchmarker.providers.gcp.gcp_managed_relational_db"""

import contextlib
import unittest
import json
import mock
import os

from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.managed_relational_db import MYSQL
from perfkitbenchmarker.managed_relational_db import POSTGRES
from perfkitbenchmarker.providers.gcp import gcp_managed_relational_db
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker import disk
from perfkitbenchmarker import data

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None


class GcpManagedRelationalDbTestCase(unittest.TestCase):

  def createMySQLSpecDict(self):
    vm_spec = virtual_machine.BaseVmSpec('NAME',
                                         **{
                                             'machine_type': 'db-n1-standard-1',
                                             'zone': 'us-west1-b',
                                         })
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

  def createManagedDbFromSpec(self, spec_dict):
    mock_db_spec = mock.Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    db_class = gcp_managed_relational_db.GCPManagedRelationalDb(mock_db_spec)
    return db_class

  def setUp(self):
    flag_values = {'run_uri': '123', 'project': None}

    p = mock.patch(gcp_managed_relational_db.__name__ + '.FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)
    mock_db_spec_attrs = self.createMySQLSpecDict()
    self.mock_db_spec = mock.Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    self.mock_db_spec.configure_mock(**mock_db_spec_attrs)

  def testNoHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createMySQLSpecDict())
      db._Create()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertNotIn('--failover-replica-name', command_string)
      self.assertNotIn('replica-pkb-db-instance-123', command_string)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=retval) as issue_command, \
            mock.patch('__builtin__.open'), \
            mock.patch(vm_util.__name__ + '.NamedTemporaryFile'), \
            mock.patch(util.__name__ + '.GetDefaultProject',
                       return_value='fakeproject'):
      yield issue_command

  def testCreate(self):
    with self._PatchCriticalObjects() as issue_command:
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Create()
      self.assertEquals(issue_command.call_count, 1)
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
      self.assertIn('--gce-zone=us-west1-b', command_string)
      self.assertIn('--region=us-west1', command_string)

  def testCreatePostgres(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createPostgresSpecDict()
      spec['engine'] = 'postgres'
      spec['engine_version'] = '9.6'
      db = self.createManagedDbFromSpec(spec)
      db._Create()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertIn('database-version=POSTGRES_9_6', command_string)
      self.assertIn('--cpu=1', command_string)
      self.assertIn('--memory=3840MiB', command_string)

  def testCreateWithBackupDisabled(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.mock_db_spec
      spec.backup_enabled = False
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Create()
      self.assertEquals(issue_command.call_count, 1)
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
    with self._PatchCriticalObjects() as issue_command:
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Delete()
      self.assertEquals(issue_command.call_count, 1)
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

    with self._PatchCriticalObjects(stdout=test_output):
      db = self.createManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._IsReady())

  def testExists(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects(stdout=test_output):
      db = self.createManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEqual(True, db._Exists())

  def testHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createMySQLSpecDict()
      spec['high_availability'] = True
      db = self.createManagedDbFromSpec(spec)
      db._Create()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--failover-replica-name', command_string)
      self.assertIn('replica-pkb-db-instance-123', command_string)

  def testParseEndpoint(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'gcloud-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertEquals('', db._ParseEndpoint(None))
      self.assertIn('10.10.0.35',
                    db._ParseEndpoint(json.loads(test_output)))

  def testValidateSpec(self):
    with self._PatchCriticalObjects():
      db_sql = self.createManagedDbFromSpec(self.createMySQLSpecDict())
      self.assertRaises(data.ResourceNotFound, db_sql._ValidateSpec)
      db_postgres = self.createManagedDbFromSpec(self.createPostgresSpecDict())
      db_postgres._ValidateSpec()

  def testValidateMachineType(self):
    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec(self.createPostgresSpecDict())
      self.assertRaises(ValueError, db._ValidateMachineType, 0, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 3840, 0)
      self.assertRaises(ValueError, db._ValidateMachineType, 255, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 256000000000, 1)
      self.assertRaises(ValueError, db._ValidateMachineType, 2560, 1)
      db._ValidateMachineType(db.spec.vm_spec.memory,
                              db.spec.vm_spec.cpus)

if __name__ == '__main__':
  unittest.main()
