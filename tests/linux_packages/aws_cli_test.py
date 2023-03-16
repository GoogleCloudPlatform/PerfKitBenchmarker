# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.linux_packages.awscli."""

import unittest
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import awscli


class AwsCliTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()

  def test_install_returns_with_late_version(self):
    awscli._VERSION = '1.10.44'
    self.vm.RemoteCommand.return_value = ('aws-cli/1.19.99 Linux/1.2', 'unused')
    awscli.Install(self.vm)
    self.vm.RemoteCommand.assert_called_once_with(
        'aws --version', ignore_failure=True
    )

  def test_full_install_returns_with_no_version(self):
    awscli._VERSION = '1.10.44'
    self.vm.RemoteCommand.return_value = ('what is aws?', 'unused')
    awscli.Install(self.vm)
    self.assertEqual(
        [
            mock.call('aws --version', ignore_failure=True),
            mock.call(
                "sudo pip3 install 'awscli==1.10.44' --ignore-installed "
                '--force-reinstall'
            ),
        ],
        self.vm.RemoteCommand.call_args_list,
    )
    self.vm.Install.assert_called_once_with('pip3')

  def test_yum_install_found(self):
    self.vm.RemoteCommand.return_value = ('fine', 'unused')
    awscli.YumInstall(self.vm)
    self.vm.RemoteCommand.assert_called_once_with('yum list installed awscli')

  def test_yum_install_not_found(self):
    self.vm.RemoteCommand.side_effect = (
        errors.VirtualMachine.RemoteCommandError('not found')
    )
    # First raise is expected, second isn't but can't swap from raise to not.
    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      awscli.YumInstall(self.vm)
    self.assertEqual(
        [
            mock.call('yum list installed awscli'),
            mock.call('aws --version', ignore_failure=True),
        ],
        self.vm.RemoteCommand.call_args_list,
    )

  def test_zypper_install_found(self):
    self.vm.RemoteCommand.return_value = ('zypper found aws-cli', 'unused')
    awscli.ZypperInstall(self.vm)
    self.vm.RemoteCommand.assert_called_once_with('zypper search -i aws-cli')

  def test_zypper_install_not_found(self):
    self.vm.RemoteCommand.return_value = ('fine but not cli', 'unused')
    awscli.ZypperInstall(self.vm)
    self.assertGreater(self.vm.RemoteCommand.call_count, 1)


if __name__ == '__main__':
  unittest.main()
