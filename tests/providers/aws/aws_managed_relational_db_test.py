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
from perfkitbenchmarker.providers.aws.aws_managed_relational_db import (
    AwsManagedRelationalDb)

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None
_AWS_PREFIX = 'aws --output json'


def readTestDataFile(filename):
  path = os.path.join(
      os.path.dirname(__file__), '../../data',
      filename)
  with open(path) as fp:
    return fp.read()


class AwsManagedRelationalDbSpecTestCase(unittest.TestCase):
  """Class that tests the creation of an AwsManagedRelationalDbSpec"""
  pass


class AwsManagedRelationalDbFlagsTestCase(unittest.TestCase):
  """Class that tests the flags defined in AwsManagedRelationalDb"""
  pass


class AwsManagedRelationalDbTestCase(unittest.TestCase):

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

  def createMockSpec(self, additional_spec_items={}):
    default_server_disk_spec = aws_disk.AwsDiskSpec(
        _COMPONENT,
        disk_size=5,
        disk_type=aws_disk.IO1,
        iops=1000)

    default_server_vm_spec = virtual_machine.BaseVmSpec(
        'NAME', **{'machine_type': 'db.t1.micro',
                   'zone': 'us-west-2b'})
    spec_dict = {
        'engine': MYSQL,
        'engine_version': '5.7.11',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'database_password': 'fakepassword',
        'database_username': 'fakeusername',
        'high_availability': False,
        'vm_spec': default_server_vm_spec,
        'disk_spec': default_server_disk_spec,
    }
    spec_dict.update(additional_spec_items)

    mock_db_spec = Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    mock_db_spec.configure_mock(**spec_dict)
    return mock_db_spec

  def createManagedDbFromSpec(self, additional_spec_items={}):
    mock_spec = self.createMockSpec(additional_spec_items)
    aws_db = AwsManagedRelationalDb(mock_spec)

    # Set necessary instance attributes that are not part of the spec
    aws_db.security_group_name = 'fake_security_group'
    aws_db.db_subnet_group_name = 'fake_db_subnet'
    aws_db.security_group_id = 'fake_security_group_id'

    return aws_db

  def create(self, additional_spec_items={}):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec(additional_spec_items)
      db._Create()
      return ' '.join(issue_command.call_args[0][0])

  def testCreate(self):
    command_string = self.create()

    self.assertTrue(
        command_string.startswith('%s rds create-db-instance' % _AWS_PREFIX))
    self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                  command_string)
    self.assertIn('--db-instance-class=db.t1.micro', command_string)
    self.assertIn('--engine=mysql', command_string)
    self.assertIn('--master-user-password=fakepassword', command_string)

  def testNoHighAvailability(self):
    spec_dict = {
        'multi_az': False,
    }
    command_string = self.create(spec_dict)

    self.assertNotIn('--multi-az', command_string)

  def testHighAvailability(self):
    command_string = self.create()

    self.assertNotIn('--multi-az', command_string)

  def testDiskWithIops(self):
    command_string = self.create()

    self.assertIn('--allocated-storage=5', command_string)
    self.assertIn('--storage-type=%s' % aws_disk.IO1, command_string)
    self.assertIn('--iops=1000', command_string)

  def testDiskWithoutIops(self):
    spec_dict = {
        'disk_spec': aws_disk.AwsDiskSpec(
            _COMPONENT, disk_size=5, disk_type=aws_disk.GP2)
    }
    command_string = self.create(spec_dict)

    self.assertIn('--allocated-storage=5', command_string)
    self.assertIn('--storage-type=%s' % aws_disk.GP2, command_string)
    self.assertNotIn('--iops', command_string)

  def testUnspecifiedDatabaseVersion(self):
    command_string = self.create()

    self.assertIn('--engine-version=5.7.11', command_string)

  def testSpecifiedDatabaseVersion(self):
    spec_dict = {
        'engine_version': '5.6.29',
    }
    command_string = self.create(spec_dict)

    self.assertIn('--engine-version=5.6.29', command_string)

  def testIsNotReady(self):
    test_data = readTestDataFile('aws-describe-db-instances-creating.json')
    with self._PatchCriticalObjects(stdout=test_data):
      db = self.createManagedDbFromSpec()

      self.assertEqual(False, db._IsReady(timeout=0))

  def testIsReady(self):
    test_data = readTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects(stdout=test_data):
      db = self.createManagedDbFromSpec()

      self.assertEqual(True, db._IsReady())

  def testParseEndpoint(self):
    test_data = readTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec()

      self.assertEqual(
          'pkb-db-instance-a4499926.cqxeajwjbqne.us-west-2.rds.amazonaws.com',
          db._ParseEndpoint(json.loads(test_data)))

  def testParsePort(self):
    test_data = readTestDataFile('aws-describe-db-instances-available.json')
    with self._PatchCriticalObjects():
      db = self.createManagedDbFromSpec()

      self.assertEqual(3306, db._ParsePort(json.loads(test_data)))

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      db = self.createManagedDbFromSpec()
      db._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertIn('aws --output json rds delete-db-instance', command_string)
      self.assertIn('--db-instance-identifier=pkb-db-instance-123',
                    command_string)
      self.assertIn('--skip-final-snapshot', command_string)


if __name__ == '__main__':
  unittest.main()
