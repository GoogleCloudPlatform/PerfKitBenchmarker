# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.linux_packages.gce_hpc_tools."""

import unittest
from absl import flags
from absl.testing import parameterized
import mock

from perfkitbenchmarker.linux_packages import gce_hpc_tools
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_GITHASH = 'abcdef'


def _YumInstall():
  vm = mock.Mock(RemoteCommand=mock.Mock(return_value=(_GITHASH, '')))
  vm.metadata = {}
  gce_hpc_tools.YumInstall(vm)
  return vm


class GcpHpcToolsTest(pkb_common_test_case.PkbCommonTestCase):

  @parameterized.named_parameters(
      ('has_reboot', 'reboot', True),
      ('no_reboot', 'tcpmem', False),
  )
  def testRebootFlag(self, hpc_tools_turning_flag, wait_for_reboot_called):
    FLAGS.gce_hpc_tools_tuning = [hpc_tools_turning_flag]
    vm = _YumInstall()
    if wait_for_reboot_called:
      vm.WaitForBootCompletion.assert_called_once()
    else:
      vm.WaitForBootCompletion.assert_not_called()

  def testMetadataRecorded(self):
    vm = _YumInstall()
    hpc_tools_tuning_str = ('hpcprofile,limits,nofirewalld,nomitigation,'
                            'noselinux,nosmt,reboot,tcpmem')
    expected_metadata = {
        'hpc_tools': True,
        'hpc_tools_tag': 'head',
        'hpc_tools_tuning': hpc_tools_tuning_str,
        'hpc_tools_version': _GITHASH,
    }
    self.assertEqual(expected_metadata, vm.metadata)

  @parameterized.named_parameters(
      ('has_tag', 'foo', 'foo'),
      ('tag_not_set', None, 'head'),
  )
  def testSetGitCommitTag(self, git_commit_tag, metadata_tag):
    FLAGS.gce_hpc_tools_tag = git_commit_tag
    vm = _YumInstall()
    self.assertEqual(metadata_tag, vm.metadata['hpc_tools_tag'])

  def testBashCommandCalled(self):
    vm = _YumInstall()
    base_command = 'cd /tmp/pkb/hpc-tools; sudo bash mpi-tuning.sh'
    command_flags = ('--hpcprofile --limits --nofirewalld --nomitigation '
                     '--noselinux --nosmt --reboot --tcpmem')
    vm.RemoteCommand.assert_called_with(
        f'{base_command} {command_flags}', ignore_failure=True)


if __name__ == '__main__':
  unittest.main()
