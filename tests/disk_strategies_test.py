# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import configs
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import lustre_service
from tests import mock_command
from tests import pkb_common_test_case


class SetUpLustreDiskStrategyTest(pkb_common_test_case.PkbCommonTestCase):

  def _CreateLustreSetup(
      self,
  ) -> tuple[
      linux_virtual_machine.BaseLinuxVirtualMachine,
      disk_strategies.SetUpLustreDiskStrategy,
      mock_command.MockRemoteCommand,
  ]:
    vm = mock.create_autospec(
        linux_virtual_machine.BaseLinuxVirtualMachine,
        instance=True,
    )
    vm.scratch_disks = []
    mock_cmd = mock_command.MockRemoteCommand({}, vm)
    disk_spec = disk.BaseLustreDiskSpec(
        'test_lustre',
        mount_point='/lustre_dir',
        disk_type=disk.LUSTRE,
    )
    service = mock.create_autospec(
        lustre_service.BaseLustreService,
        instance=True,
        spec_set=True,
    )
    service.CreateLustreDisk.return_value = disk.LustreDisk(disk_spec)
    service.GetMountPoint.return_value = '10.0.0.1@tcp:/lustre'
    strategy = disk_strategies.SetUpLustreDiskStrategy(vm, disk_spec, service)
    return vm, strategy, mock_cmd

  def testDefaultLustreStripeArgsSkipsSetstripe(self):
    vm, strategy, mock_cmd = self._CreateLustreSetup()

    strategy.SetUpDiskOnLinux()

    self.assertLen(vm.scratch_disks, 1)
    self.assertEqual(mock_cmd.all_commands, '')

  @parameterized.named_parameters(
      dict(testcase_name='empty', stripe_args=''),
      dict(testcase_name='whitespace', stripe_args='   '),
  )
  def testBlankLustreStripeArgsSkipsSetstripe(self, stripe_args: str):
    vm, strategy, mock_cmd = self._CreateLustreSetup()

    with flagsaver.flagsaver(lustre_stripe_args=stripe_args):
      strategy.SetUpDiskOnLinux()

    self.assertLen(vm.scratch_disks, 1)
    self.assertEqual(mock_cmd.all_commands, '')

  @flagsaver.flagsaver(lustre_stripe_args='-E 1M -c 1 -E -1 -c -1')
  def testConfiguredLustreStripeArgsRunsSetstripe(self):
    _, strategy, mock_cmd = self._CreateLustreSetup()

    strategy.SetUpDiskOnLinux()

    self.assertEqual(
        mock_cmd.all_commands,
        'sudo lfs setstripe -E 1M -c 1 -E -1 -c -1 /lustre_dir\n',
    )

  def testDefaultBenchmarkConfigIo500LustreFlags(self):
    io500_lustre_config = configs.GetDefaultAndUserConfig()['io500_lustre']
    flags_dict = io500_lustre_config['flags']

    self.assertEqual(flags_dict['lustre_stripe_args'], '-E 1M -c 1 -E -1 -c -1')
    self.assertTrue(flags_dict['io500_wait_for_cleanup'])
    self.assertEqual(flags_dict['lustre_max_dirty_mb'], 2000)


if __name__ == '__main__':
  unittest.main()
