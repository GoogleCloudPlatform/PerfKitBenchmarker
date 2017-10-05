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
"""Tests for perfkitbenchmarker.providers.aws.aws_managed_relational_db"""

import contextlib
import unittest
import os
import json

from mock import patch, Mock

from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.managed_relational_db import MYSQL
from perfkitbenchmarker.providers.aws import aws_managed_relational_db
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None
_AWS_PREFIX = 'aws --output json'


class AwsManagedRelationalDbSpecTestCase(unittest.TestCase):
  pass


class AwsManagedRelationalDbFlagsTestCase(unittest.TestCase):
  pass


class AwsManagedRelationalDbTestCase(unittest.TestCase):

  def createSpecDict(self):
    disk_spec = aws_disk.AwsDiskSpec(
        _COMPONENT, disk_size=5, disk_type=aws_disk.IO1, iops=1000)

    vm_spec = virtual_machine.BaseVmSpec(
        'NAME', **{'machine_type': 'db.t1.micro',
                   'zone': 'us-west-2b'})

    return {
        'database': MYSQL,
        'database_version': '5.7.11',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'database_username': 'fakeusername',
        'vm_spec': vm_spec,
        'disk_spec': disk_spec,
        'high_availability': False
    }

  def createManagedDbFromSpec(self, spec_dict):
    mock_db_spec = Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    db_class = aws_managed_relational_db.AwsManagedRelationalDb(mock_db_spec)
    db_class.security_group_name = 'fake-security_group_name'

    network_patch = patch.object(
        db_class, '_GetNetwork',
        new_callable=self.createNetworkMock())
    network_patch.start()
    self.addCleanup(network_patch.stop)

    get_new_zones_patch = patch.object(
        db_class, '_GetNewZones')
    get_new_zones_mock = get_new_zones_patch.start()
    get_new_zones_mock.return_value = ['us-west-2a',
                                       'us-west-2c']
    self.addCleanup(get_new_zones_patch.stop)

    create_subnet_patch = patch.object(
        db_class, '_CreateSubnetInAdditionalZone')
    create_subnet_patch.start()
    self.addCleanup(create_subnet_patch.stop)

    is_ready_patch = patch.object(
        db_class, '_IsReady')
    is_ready_mock = is_ready_patch.start()
    is_ready_mock.return_value = True
    self.addCleanup(is_ready_patch.stop)

    db_class._CreateDependencies()

    return db_class

  def createNetworkMock(self):
    return Mock(spec=aws_network.AwsNetwork)

  def setUp(self):
    flag_values = {'run_uri': '123', 'project': None}

    p = patch(aws_managed_relational_db.__name__ + '.FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with patch(vm_util.__name__ + '.IssueCommand',
               return_value=retval) as issue_command, \
            patch('__builtin__.open'), \
            patch(vm_util.__name__ + '.NamedTemporaryFile'):
      yield issue_command

  def testCreate(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertTrue(
          command_string.startswith('%s rds create-db-instance' % _AWS_PREFIX))
      self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                    command_string)
      self.assertIn('--db-instance-class=db.t1.micro', command_string)
      self.assertIn('--engine=mysql', command_string)
      self.assertIn('--master-user-password=fakepassword', command_string)

  def testNoHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertNotIn('--multi-az', command_string)

  def testHighAvailability(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createSpecDict()
      spec['high_availability'] = True
      db = self.createManagedDbFromSpec(spec)
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--multi-az', command_string)

  def testDiskWithIops(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--allocated-storage=5', command_string)
      self.assertIn('--storage-type=%s' % aws_disk.IO1, command_string)
      self.assertIn('--iops=1000', command_string)

  def testDiskWithoutIops(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createSpecDict()
      spec['disk_spec'] = aws_disk.AwsDiskSpec(
          _COMPONENT, disk_size=5, disk_type=aws_disk.GP2)
      db = self.createManagedDbFromSpec(spec)
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--allocated-storage=5', command_string)
      self.assertIn('--storage-type=%s' % aws_disk.GP2, command_string)
      self.assertNotIn('--iops', command_string)

  def testUnspecifiedDatabaseVersion(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--engine-version=5.7.11', command_string)

  def testSpecifiedDatabaseVersion(self):
    with self._PatchCriticalObjects() as issue_command:
      spec = self.createSpecDict()
      spec['database_version'] = '5.6.29'
      db = self.createManagedDbFromSpec(spec)
      db._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('--engine-version=5.6.29', command_string)

  @unittest.skip('Need to refactor this class so that IsReady is not '
                 'always patched')
  def testIsNotReady(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'aws-describe-db-instances-creating.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects(stdout=test_output):
      db = self.createManagedDbFromSpec(self.createSpecDict())

      self.assertEqual(False, db._IsReady())

  def testIsReady(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'aws-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects(stdout=test_output):
      db = self.createManagedDbFromSpec(self.createSpecDict())

      self.assertEqual(True, db._IsReady())

  def testParseEndpoint(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'aws-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec(self.createSpecDict())

      self.assertEqual(
          'pkb-db-instance-a4499926.cqxeajwjbqne.us-west-2.rds.amazonaws.com',
          db._ParseEndpoint(json.loads(test_output)))

  def testParsePort(self):
    path = os.path.join(
        os.path.dirname(__file__), '../../data',
        'aws-describe-db-instances-available.json')
    with open(path) as fp:
      test_output = fp.read()

    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec(self.createSpecDict())

      self.assertEqual(3306, db._ParsePort(json.loads(test_output)))

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(self.createSpecDict())
      db._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('aws --output json rds delete-db-instance', command_string)
      self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                    command_string)
      self.assertIn('--skip-final-snapshot', command_string)


if __name__ == '__main__':
  unittest.main()
