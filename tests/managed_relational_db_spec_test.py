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
"""Tests for _ManagedRelationalDbSpec"""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from tests import mock_flags

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None


def _mergeDicts(dict1, dict2):
  result = dict1.copy()
  result.update(dict2)
  return result


class FakeManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):

  def GetEndpoint(self):
    pass

  def GetPort(self):
    pass

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetDefaultEngineVersion(self, _):
    pass


class ManagedRelationalDbSpecTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.minimal_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'vm_spec': {
            'GCP': {
                'machine_type': 'n1-standard-1'
            }
        },
        'disk_spec': {
            'GCP': {
                'disk_size': 500
            }
        }
    }

    managed_relational_db._MANAGED_RELATIONAL_DB_REGISTRY = {
        'GCP': FakeManagedRelationalDb(None)
    }

  def tearDown(self):
    managed_relational_db._MANAGED_RELATIONAL_DB_REGISTRY = {}

  def testMinimalConfig(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.engine, 'mysql')
    self.assertEqual(result.cloud, 'GCP')
    self.assertIsInstance(result.vm_spec, gce_virtual_machine.GceVmSpec)

  def testDefaultDatabaseName(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.database_name, 'pkb-db-123')

  def testCustomDatabaseName(self):
    spec = _mergeDicts(self.minimal_spec, {'database_name': 'fakename'})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.database_name, 'fakename')

  def testCustomDatabaseVersion(self):
    spec = _mergeDicts(self.minimal_spec, {'engine_version': '6.6'})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.engine_version, '6.6')

  def testDefaultDatabasePassword(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertIsInstance(result.database_password, str)
    self.assertTrue(len(result.database_password) == 10)

  def testRandomDatabasePassword(self):
    spec = _mergeDicts(self.minimal_spec, {'database_password': 'fakepassword'})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.database_password, 'fakepassword')

  def testDefaultHighAvailability(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.high_availability, False)

  def testCustomHighAvailability(self):
    spec = _mergeDicts(self.minimal_spec, {'high_availability': True})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.high_availability, True)

  def testDefaultBackupEnabled(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.backup_enabled, True)

  def testCustomBackupEnabled(self):
    spec = _mergeDicts(self.minimal_spec, {'backup_enabled': False})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.backup_enabled, False)

  def testDefaultBackupTime(self):
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.minimal_spec)
    self.assertEqual(result.backup_start_time, '07:00')

  def testCustomBackupTime(self):
    spec = _mergeDicts(self.minimal_spec, {'backup_start_time': '08:00'})
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **spec)
    self.assertEqual(result.backup_start_time, '08:00')


class ManagedRelationalDbMinimalSpecTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'vm_spec': {
            'GCP': {
                'machine_type': 'n1-standard-1'
            }
        },
        'disk_spec': {
            'GCP': {
                'disk_size': 500
            }
        }
    }

  def testDiskSpecRequired(self):
    del self.spec['disk_spec']
    with self.assertRaisesRegexp(errors.Config.MissingOption, 'disk_spec'):
      benchmark_config_spec._ManagedRelationalDbSpec(
          _COMPONENT, flag_values=self.flags, **self.spec)

  def testVmSpecRequired(self):
    del self.spec['vm_spec']
    with self.assertRaisesRegexp(errors.Config.MissingOption, 'vm_spec'):
      benchmark_config_spec._ManagedRelationalDbSpec(
          _COMPONENT, flag_values=self.flags, **self.spec)


class ManagedRelationalDbFlagsTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.MockFlags()
    self.flags['run_uri'].parse('123')

    self.full_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'database_name': 'fake_name',
        'database_password': 'fake_password',
        'backup_enabled': True,
        'backup_start_time': '07:00',
        'vm_spec': {
            'GCP': {
                'machine_type': 'n1-standard-1',
                'zone': 'us-west1-a',
            }
        },
        'disk_spec': {
            'GCP': {
                'disk_size': 500,
            }
        }
    }

    managed_relational_db._MANAGED_RELATIONAL_DB_REGISTRY = {
        'GCP': FakeManagedRelationalDb(None)
    }

  def tearDown(self):
    managed_relational_db._MANAGED_RELATIONAL_DB_REGISTRY = {}

  # Not testing this yet, because it requires the implementation
  # of a managed_relational_db provider for the specified
  # cloud (other than GCP). We could mock it perhaps.
  def testCloudFlag(self):
    pass

  def testDatabaseFlag(self):
    self.flags['managed_db_engine'].parse('postgres')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.engine, 'postgres')

  def testDatabaseNameFlag(self):
    self.flags['managed_db_database_name'].parse('fakedbname')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.database_name, 'fakedbname')

  def testDatabasePasswordFlag(self):
    self.flags['managed_db_database_password'].parse('fakepassword')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.database_password, 'fakepassword')

  def testHighAvailabilityFlag(self):
    self.flags['managed_db_high_availability'].parse(True)
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.high_availability, True)

  def testDatabaseVersionFlag(self):
    self.flags['managed_db_engine_version'].parse('5.6')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.engine_version, '5.6')

  def testBackupEnabledFlag(self):
    self.flags['managed_db_backup_enabled'].parse(False)
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.backup_enabled, False)

  def testBackupStartTimeFlag(self):
    self.flags['managed_db_backup_start_time'].parse('12:23')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.backup_start_time, '12:23')

  def testZoneFlag(self):
    self.flags['managed_db_zone'].parse('us-east1-b')
    result = benchmark_config_spec._ManagedRelationalDbSpec(
        _COMPONENT, flag_values=self.flags, **self.full_spec)
    self.assertEqual(result.vm_spec.zone, 'us-east1-b')

if __name__ == '__main__':
  unittest.main()
