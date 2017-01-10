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

  def _Exists(self):
    return True


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


if __name__ == '__main__':
  unittest.main()
