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
from perfkitbenchmarker.linux_packages import awscli


class AwsCliTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()

  def test_install_found(self):
    self.vm.TryRemoteCommand.return_value = True
    awscli.Install(self.vm)
    self.vm.TryRemoteCommand.assert_called_once_with('aws --version')

  def test_install_not_found(self):
    self.vm.TryRemoteCommand.return_value = False
    awscli.Install(self.vm)
    self.vm.TryRemoteCommand.assert_called_once_with('aws --version')
    self.vm.Install.assert_called_once_with('pip')
    self.vm.RemoteCommand.assert_called_once_with(
        'sudo pip3 install awscli --ignore-installed --force-reinstall'
    )


if __name__ == '__main__':
  unittest.main()
