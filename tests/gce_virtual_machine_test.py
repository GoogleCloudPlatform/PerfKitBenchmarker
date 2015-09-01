# Copyright 2015 Google Inc. All rights reserved.
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

"""Tests for perfkitbenchmarker.gcp.gce_virtual_machine"""

import unittest
import mock

from perfkitbenchmarker import pkb  # noqa. Imported to create needed flags.
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.gcp import gce_virtual_machine


class GCEPreemptibleVMFlagTestCase(unittest.TestCase):
  def testPreemptibleVMFlag(self):
    with mock.patch(vm_util.__name__ + '.IssueCommand') as issue_command, \
            mock.patch('__builtin__.open'), \
            mock.patch(vm_util.__name__ + '.NamedTemporaryFile'), \
            mock.patch(gce_virtual_machine.__name__ + '.FLAGS') as gvm_flags:
      gvm_flags.gce_preemptible_vms = True
      gvm_flags.gcloud_scopes = None
      vm_spec = virtual_machine.BaseVirtualMachineSpec('proj',
                                                       'zone',
                                                       'n1-standard-1',
                                                       'image')
      vm = gce_virtual_machine.GceVirtualMachine(vm_spec)
      vm._Create()
      self.assertEquals(issue_command.call_count, 1)
      self.assertIn('--preemptible', issue_command.call_args[0][0])

if __name__ == '__main__':
  unittest.main()
