# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.rackspace.util"""

import unittest
import mock
import os

from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.openstack import utils


_OPENSTACK_CLI_PATH = 'path/openstack'


class OpenStackResource(resource.BaseResource):

  def __init__(self, **kwargs):
    for k, v in kwargs.iteritems():
      setattr(self, k, v)

  def _Create(self):
    raise NotImplementedError()

  def _Delete(self):
    raise NotImplementedError()


class OpenStackCLICommandTestCase(unittest.TestCase):

  def setUp(self):
    super(OpenStackCLICommandTestCase, self).setUp()
    p = mock.patch(utils.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)
    self.mock_flags.openstack_cli_path = _OPENSTACK_CLI_PATH

  def testCommonFlagsWithoutOptionalFlags(self):
    rack_resource = OpenStackResource()
    cmd = utils.OpenStackCLICommand(rack_resource, 'image', 'list')
    self.assertEqual(cmd._GetCommand(), [
        'path/openstack', 'image', 'list', '--format', 'json'])

  def testCommonFlagsWithOptionalFlags(self):
    rack_resource = OpenStackResource()
    cmd = utils.OpenStackCLICommand(rack_resource, 'image', 'list')
    cmd.flags['public'] = True
    self.assertEqual(cmd._GetCommand(), [
        'path/openstack', 'image', 'list', '--format', 'json', '--public'])


class NovaTableTestCase(unittest.TestCase):

  def testEmptyParseFloatingIPTable(self):
    s = ('+----+----+-----------+----------+------+\n'
         '| Id | IP | Server Id | Fixed IP | Pool |\n'
         '+----+----+-----------+----------+------+\n'
         '+----+----+-----------+----------+------+\n')
    floating_ip_dict_list = utils.ParseFloatingIPTable(s)
    self.assertEqual(len(floating_ip_dict_list), 0)

  def testSingleWordColumnsParseFloatingIPTable(self):
    s = self._load_table_output(
        'nova-floating-ip-list-single-word-populated.txt')
    floating_ip_dict_list = utils.ParseFloatingIPTable(s)

    self.assertEqual(len(floating_ip_dict_list), 3)
    for floating_ip_dict in floating_ip_dict_list:
      for k in ('id', 'ip', 'instance_id', 'fixed_ip', 'pool',):
        self.assertTrue(k in floating_ip_dict)

    self.assertEqual(floating_ip_dict_list[0]['id'],
                     '15d1f214-a025-4f19-b0f5-d9ae1f409674')
    self.assertEqual(floating_ip_dict_list[0]['ip'], '10.50.49.207')
    self.assertIsNone(floating_ip_dict_list[0]['instance_id'])
    self.assertIsNone(floating_ip_dict_list[0]['fixed_ip'])
    self.assertEqual(floating_ip_dict_list[0]['pool'], 'public')

    self.assertEqual(floating_ip_dict_list[2]['id'],
                     '74519d10-25b7-4a43-921c-f5b31d87818d')
    self.assertEqual(floating_ip_dict_list[2]['ip'], '10.50.49.208')
    self.assertEqual(floating_ip_dict_list[2]['instance_id'],
                     'ba574fdd-5feb-4d5e-b314-185cc540cc1d')
    self.assertEqual(floating_ip_dict_list[2]['fixed_ip'], '172.16.1.209')
    self.assertEqual(floating_ip_dict_list[2]['pool'], 'public')

  def testMultiWordsColumnsParseFloatingIPTable(self):
    s = self._load_table_output(
        'nova-floating-ip-list-multi-words-populated.txt')
    floating_ip_dict_list = utils.ParseFloatingIPTable(s)

    self.assertEqual(len(floating_ip_dict_list), 3)
    for floating_ip_dict in floating_ip_dict_list:
      for k in ('id', 'ip', 'instance_id', 'fixed_ip', 'pool',):
        self.assertTrue(k in floating_ip_dict)

        self.assertEqual(floating_ip_dict_list[0]['id'],
                         '15d1f214-a025-4f19-b0f5-d9ae1f409674')

    self.assertEqual(floating_ip_dict_list[0]['ip'], '10.50.49.207')
    self.assertIsNone(floating_ip_dict_list[0]['instance_id'])
    self.assertIsNone(floating_ip_dict_list[0]['fixed_ip'])
    self.assertEqual(floating_ip_dict_list[0]['pool'], 'a public')

    self.assertEqual(floating_ip_dict_list[1]['pool'], 'p1')

    self.assertEqual(floating_ip_dict_list[2]['id'],
                     '74519d10-25b7-4a43-921c-f5b31d87818d')

    self.assertEqual(floating_ip_dict_list[2]['ip'], '10.50.49.208')
    self.assertEqual(floating_ip_dict_list[2]['instance_id'],
                     'ba574fdd-5feb-4d5e-b314-185cc540cc1d')
    self.assertEqual(floating_ip_dict_list[2]['fixed_ip'], '172.16.1.209')
    self.assertEqual(floating_ip_dict_list[2]['pool'], 'my public network')

  def _load_table_output(self, filename):
    path = os.path.join(os.path.dirname(__file__), '..', '..',
                        'data', 'openstack', filename)
    with open(path) as f:
      return f.read()

if __name__ == '__main__':
  unittest.main()
