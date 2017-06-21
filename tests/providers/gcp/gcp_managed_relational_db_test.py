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
from perfkitbenchmarker.providers.gcp import gcp_managed_relational_db
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker import disk

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None


class GcpManagedRelationalDbSpecTestCase(unittest.TestCase):
  pass


class GcpManagedRelationalDbFlagsTestCase(unittest.TestCase):
  pass


class GcpManagedRelationalDbTestCase(unittest.TestCase):

  def createSpecDict(self):
    vm_spec = virtual_machine.BaseVmSpec('NAME',
                                         **{'machine_type': 'db-n1-standard-1'})
    disk_spec = disk.BaseDiskSpec('NAME', **{'disk_size': 50})
    # TODO: Database version has more than one supported value. Test should
    # reflect that by not declaring a database version and letting the default
    # version be returned.
    return {
        'database': MYSQL,
        'database_version': '5.7',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'vm_spec': vm_spec,
        'disk_spec': disk_spec,
        'high_availability': False
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

    mock_db_spec_attrs = self.createSpecDict()
    self.mock_db_spec = mock.Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    self.mock_db_spec.configure_mock(**mock_db_spec_attrs)

  def testNoHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Create()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertNotIn('--failover-replica-name', command_string)
      self.assertNotIn('replica-pkb-db-instance-123', command_string)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self):
    """A context manager that patches a few critical objects with mocks."""
    retval = ('', '', 0)
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

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      vm = gcp_managed_relational_db.GCPManagedRelationalDb(self.mock_db_spec)
      vm._Delete()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud sql instances delete pkb-db-instance-123'))

  def testHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createSpecDict()
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
      db = self.createManagedDbFromSpec(self.createSpecDict())

      self.assertIn(
          'pkb-db-instance-123',
          db._ParseEndpoint(json.loads(test_output)))


if __name__ == '__main__':
  unittest.main()
