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
import mock
import re
import unittest

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.managed_relational_db import MYSQL, POSTGRES
from perfkitbenchmarker.providers.aws import aws_managed_relational_db
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import util
from tests import mock_flags

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

  def setUp(self):
    flag_values = {'run_uri': '123', 'project': None}

    p = mock.patch(aws_managed_relational_db.__name__ + '.FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)

    vm_spec = virtual_machine.BaseVmSpec(
        'NAME',
        **{'machine_type': 'db.t1.micro'}
    )
    disk_spec = aws_disk.AwsDiskSpec(
        _COMPONENT,
        disk_size=5,
        disk_type='test_disk_type',
        iops=1000)

    mock_db_spec_attrs = {
        'database': MYSQL,
        'database_version': '5.6',
        'run_uri': '123',
        'database_name': 'fakedbname',
        'db_password': 'fakepassword',
        'db_username': 'fakeusername',
        'vm_spec': vm_spec,
        'disk_spec': disk_spec
    }
    self.mock_db_spec = mock.Mock(
        spec=benchmark_config_spec._ManagedRelationalDbSpec)
    self.mock_db_spec.configure_mock(**mock_db_spec_attrs)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self):
    """A context manager that patches a few critical objects with mocks."""
    retval = ('', '', 0)
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=retval) as issue_command, \
            mock.patch('__builtin__.open'), \
            mock.patch(vm_util.__name__ + '.NamedTemporaryFile'):
      yield issue_command

  def testCreate(self):
    with self._PatchCriticalObjects() as issue_command:
      vm = aws_managed_relational_db.AwsManagedRelationalDb(self.mock_db_spec)
      vm._Create()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertTrue(command_string.startswith('%s rds create-db-instance' %
                                                _AWS_PREFIX))
      self.assertIn('--db-instance-identifier pkb-db-instance-123', command_string)
      self.assertIn('--db-instance-class db.t1.micro', command_string)
      self.assertIn('--engine mysql', command_string)
      self.assertIn('--master-user-password fakepassword', command_string)
      self.assertIn('--allocated-storage 5', command_string)


  @unittest.skip('TODO')
  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      vm = aws_managed_relational_db.AwsManagedRelationalDb(self.mock_db_spec)
      vm._Delete()
      self.assertEquals(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertTrue(command_string.startswith('aws rds delete-db-instance'))
      self.assertIn('--db-instance-identifier pkb-db-instance-123', command_string)
      self.assertIn('--skip-final-snapshot', command_string)

  # testUsername
  # testDatabseName
  # testMysql
  # testPostgres
  # class StorageTest:
  #   testStandard
  #   testGp2
  #   testIops
  # testReboot
  # testRegion
  # testMulti-az





if __name__ == '__main__':
  unittest.main()
