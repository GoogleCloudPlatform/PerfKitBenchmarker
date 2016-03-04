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
from perfkitbenchmarker.providers.rackspace import util


_RACK_PATH = 'path/rack'


class RackspaceResource(resource.BaseResource):

  def __init__(self, **kwargs):
    for k, v in kwargs.iteritems():
      setattr(self, k, v)

  def _Create(self):
    raise NotImplementedError()

  def _Delete(self):
    raise NotImplementedError()


class RackCLICommandTestCase(unittest.TestCase):

  def setUp(self):
    super(RackCLICommandTestCase, self).setUp()
    p = mock.patch(util.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.addCleanup(p.stop)
    self.mock_flags.rack_path = _RACK_PATH

  def testCommonFlagsWithoutOptionalFlags(self):
    rack_resource = RackspaceResource(profile=None)
    cmd = util.RackCLICommand(rack_resource, 'servers', 'image', 'list')
    self.assertEqual(cmd._GetCommand(), [
        'path/rack', 'servers', 'image', 'list', '--output', 'json'])

  def testCommonFlagsWithOptionalFlags(self):
    rack_resource = RackspaceResource(profile='US', region='DFW')
    cmd = util.RackCLICommand(rack_resource, 'servers', 'keypair', 'list')
    cmd.flags['all-pages'] = True
    self.assertEqual(cmd._GetCommand(), [
        'path/rack', 'servers', 'keypair', 'list', '--all-pages',
        '--output', 'json', '--profile', 'US', '--region', 'DFW'])


if __name__ == '__main__':
  unittest.main()
