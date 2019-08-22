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
"""Tests for perfkitbenchmarker.providers.rackspace.util."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import mock

from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.openstack import utils
import six


_OPENSTACK_CLI_PATH = 'path/openstack'


class OpenStackResource(resource.BaseResource):

  def __init__(self, **kwargs):
    for k, v in six.iteritems(kwargs):
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

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIssueCommandRaiseOnFailureDefault(self, mock_cmd):
    rack_resource = OpenStackResource()
    cmd = utils.OpenStackCLICommand(rack_resource)
    cmd.Issue()
    mock_cmd.assert_called_with(['path/openstack', '--format', 'json'],
                                raise_on_failure=False)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIssueCommandRaiseOnFailureTrue(self, mock_cmd):
    rack_resource = OpenStackResource()
    cmd = utils.OpenStackCLICommand(rack_resource)
    cmd.Issue(raise_on_failure=True)
    mock_cmd.assert_called_with(['path/openstack', '--format', 'json'],
                                raise_on_failure=True)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testIssueCommandRaiseOnFailureFalse(self, mock_cmd):
    rack_resource = OpenStackResource()
    cmd = utils.OpenStackCLICommand(rack_resource)
    cmd.Issue(raise_on_failure=False)
    mock_cmd.assert_called_with(['path/openstack', '--format', 'json'],
                                raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
