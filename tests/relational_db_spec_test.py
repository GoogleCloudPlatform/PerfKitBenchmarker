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
"""Tests for RelationalDbSpec."""

import unittest
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'


def _mergeDicts(dict1, dict2):
  result = dict1.copy()
  result.update(dict2)
  return result


class FakeRelationalDb(relational_db.BaseRelationalDb):

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

  def _FailoverHA(self):
    pass


class RelationalDbSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse('123')
    FLAGS['use_managed_db'].parse(True)

    self.minimal_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'db_spec': {'GCP': {'machine_type': 'n1-standard-1'}},
        'db_disk_spec': {'GCP': {'disk_size': 500}},
    }

  def testMinimalConfig(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.engine, 'mysql')
    self.assertEqual(result.cloud, 'GCP')
    self.assertIsInstance(result.db_spec, gce_virtual_machine.GceVmSpec)

  def testDefaultDatabaseName(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.database_name, 'pkb-db-123')

  def testCustomDatabaseName(self):
    spec = _mergeDicts(self.minimal_spec, {'database_name': 'fakename'})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.database_name, 'fakename')

  def testCustomDatabaseVersion(self):
    spec = _mergeDicts(self.minimal_spec, {'engine_version': '6.6'})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.engine_version, '6.6')

  def testDefaultDatabasePassword(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertIsInstance(result.database_password, str)
    self.assertEqual(len(result.database_password), 13)

  def testRandomDatabasePassword(self):
    spec = _mergeDicts(self.minimal_spec, {'database_password': 'fakepassword'})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.database_password, 'fakepassword')

  def testDefaultHighAvailability(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.high_availability, False)

  def testCustomHighAvailability(self):
    spec = _mergeDicts(self.minimal_spec, {'high_availability': True})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.high_availability, True)

  def testDefaultBackupEnabled(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.backup_enabled, True)

  def testCustomBackupEnabled(self):
    spec = _mergeDicts(self.minimal_spec, {'backup_enabled': False})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.backup_enabled, False)

  def testDefaultBackupTime(self):
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec
    )
    self.assertEqual(result.backup_start_time, '07:00')

  def testCustomBackupTime(self):
    spec = _mergeDicts(self.minimal_spec, {'backup_start_time': '08:00'})
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **spec
    )
    self.assertEqual(result.backup_start_time, '08:00')


class RelationalDbMinimalSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse('123')

    self.spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'db_spec': {'GCP': {'machine_type': 'n1-standard-1'}},
        'db_disk_spec': {'GCP': {'disk_size': 500}},
    }

  def testDiskSpecRequired(self):
    del self.spec['db_disk_spec']
    with self.assertRaisesRegex(errors.Config.MissingOption, 'db_disk_spec'):
      relational_db_spec.RelationalDbSpec(
          _COMPONENT, flag_values=FLAGS, **self.spec
      )

  def testVmSpecRequired(self):
    del self.spec['db_spec']
    with self.assertRaisesRegex(errors.Config.MissingOption, 'db_spec'):
      relational_db_spec.RelationalDbSpec(
          _COMPONENT, flag_values=FLAGS, **self.spec
      )


class RelationalDbFlagsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse('123')

    self.full_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'database_name': 'fake_name',
        'database_password': 'fake_password',
        'backup_enabled': True,
        'backup_start_time': '07:00',
        'db_spec': {
            'GCP': {
                'machine_type': 'n1-standard-1',
                'zone': 'us-west1-a',
            }
        },
        'db_disk_spec': {
            'GCP': {
                'disk_size': 500,
            }
        },
        'vm_groups': {
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
                'vm_count': 1,
            },
            'servers_replicas': {
                'vm_spec': {
                    'GCP': {
                        'zone': 'us-central1-c',
                        'machine_type': 'n1-standard-1',
                    }
                },
                'disk_spec': {'GCP': {'disk_size': 500, 'disk_type': 'pd-ssd'}},
                'vm_count': 1,
            },
        },
    }

  # Not testing this yet, because it requires the implementation
  # of a relational_db provider for the specified
  # cloud (other than GCP). We could mock it perhaps.
  def testCloudFlag(self):
    pass

  def testDatabaseFlag(self):
    FLAGS['db_engine'].parse('postgres')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.engine, 'postgres')

  def testDatabaseNameFlag(self):
    FLAGS['database_name'].parse('fakedbname')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.database_name, 'fakedbname')

  def testDatabasePasswordFlag(self):
    FLAGS['database_password'].parse('fakepassword')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.database_password, 'fakepassword')

  def testHighAvailabilityFlag(self):
    FLAGS['db_high_availability'].parse(True)
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.high_availability, True)

  def testDatabaseVersionFlag(self):
    FLAGS['db_engine_version'].parse('5.6')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.engine_version, '5.6')

  def testBackupEnabledFlag(self):
    FLAGS['db_backup_enabled'].parse(False)
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.backup_enabled, False)

  def testBackupStartTimeFlag(self):
    FLAGS['db_backup_start_time'].parse('12:23')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.backup_start_time, '12:23')

  def testZoneFlag(self):
    FLAGS['db_zone'].parse('us-east1-b')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.db_spec.zone, 'us-east1-b')
    self.assertEqual(result.vm_groups['servers'].vm_spec.zone, 'us-east1-b')

  def testClientVmZoneFlag(self):
    FLAGS['client_vm_zone'].parse('us-east1-b')
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.vm_groups['clients'].vm_spec.zone, 'us-east1-b')

  def testDiskSizeFlag(self):
    FLAGS['db_disk_size'].parse(2000)
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.db_disk_spec.disk_size, 2000)
    self.assertEqual(result.vm_groups['servers'].disk_spec.disk_size, 2000)

  def testClientVmDiskSizeFlag(self):
    FLAGS['client_vm_disk_size'].parse(2000)
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(result.vm_groups['clients'].disk_spec.disk_size, 2000)

  def testDbReplicaZonesFlag(self):
    FLAGS['db_replica_zones'].parse(['us-central1-c', 'us-central1-d'])
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(
        result.vm_groups['servers_replicas'].vm_spec.zone, 'us-central1-d'
    )

  def testReverseOrderedDbReplicaZonesFlag(self):
    FLAGS['db_replica_zones'].parse(['us-central1-d', 'us-central1-c'])
    result = relational_db_spec.RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.full_spec
    )
    self.assertEqual(
        result.vm_groups['servers_replicas'].vm_spec.zone, 'us-central1-d'
    )

if __name__ == '__main__':
  unittest.main()
