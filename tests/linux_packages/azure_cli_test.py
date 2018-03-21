# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.linux_packages.azure_cli."""

import unittest
import mock
from perfkitbenchmarker.linux_packages import azure_cli


class AzureCliTest(unittest.TestCase):

  def setUp(self):
    self.vm = mock.Mock()

  def assertCallArgsEqual(self, call_args_singles, mock_method):
    """Compare the list of single arguments to all mocked calls in mock_method.

    Mock calls can be tested like this:
      (('x',),) == call('x')
    As all the mocked method calls have one single argument (ie 'x') they need
    to be converted into the tuple of positional arguments tuple that mock
    expects.

    Args:
      call_args_singles: List of single arguments sent to the mock_method,
        ie ['x', 'y'] is for when mock_method was called twice: once with
        x and then with y.
      mock_method: Method that was mocked and called with call_args_singles.
    """
    # convert from ['a', 'b'] into [(('a',),), (('b',),)]
    expected = [((arg,),) for arg in call_args_singles]
    self.assertEqual(expected, mock_method.call_args_list)

  def assertInstallPackageCommandsEqual(self, expected_packages):
    # tests the calls to vm.InstallPackages(str)
    self.assertCallArgsEqual(expected_packages, self.vm.InstallPackages)

  def assertRemoteCommandsEqual(self, expected_cmds):
    # tests the calls to vm.RemoteCommand(str)
    self.assertCallArgsEqual(expected_cmds, self.vm.RemoteCommand)

  def assertVmInstallCommandsEqual(self, expected_cmds):
    # tests the calls to vm.Install(str)
    self.assertCallArgsEqual(expected_cmds, self.vm.Install)

  def assertOnlyKnownMethodsCalled(self, *known_methods):
    # this test will fail if vm.foo() was called and "foo" was not in the
    # known methods
    found_methods = set()
    for mock_call in self.vm.mock_calls:
      found_methods.add(mock_call[0])
    self.assertEqual(set(known_methods), found_methods)

  def testShowHowMockingWorks(self):
    # show how assertCallArgsEqual works by calling a method twice with two
    # different strings
    cmds = ['echo', 'bash']
    for cmd in cmds:
      self.vm.foo(cmd)
    self.assertCallArgsEqual(cmds, self.vm.foo)

  def testYumInstall(self):
    azure_cli.YumInstall(self.vm)
    self.assertRemoteCommandsEqual([
        'echo "[azure-cli]\n'
        'name=Azure CLI\n'
        'baseurl=https://packages.microsoft.com/yumrepos/azure-cli\n'
        'enabled=1\n'
        'gpgcheck=1\n'
        'gpgkey=https://packages.microsoft.com/keys/microsoft.asc\n"'
        ' | sudo tee /etc/yum.repos.d/azure-cli.repo',
        'sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc'
    ])
    self.assertInstallPackageCommandsEqual(['azure-cli'])
    self.assertOnlyKnownMethodsCalled('RemoteCommand', 'InstallPackages')

  def testAptInstall(self):
    self.vm.RemoteCommand.return_value = ('wheezy', '')
    azure_cli.AptInstall(self.vm)
    self.assertRemoteCommandsEqual([
        'lsb_release -cs', 'echo "deb [arch=amd64] '
        'https://packages.microsoft.com/repos/azure-cli/ wheezy main" | sudo '
        'tee /etc/apt/sources.list.d/azure-cli.list',
        'sudo apt-key adv --keyserver packages.microsoft.com --recv-keys '
        '52E16F86FEE04B979B07E28DB02C46DF417A0893', 'sudo apt-get update'
    ])
    self.assertInstallPackageCommandsEqual(['apt-transport-https', 'azure-cli'])
    self.assertVmInstallCommandsEqual(['python', 'lsb_release'])
    self.assertOnlyKnownMethodsCalled('RemoteCommand', 'Install',
                                      'InstallPackages')


if __name__ == '__main__':
  unittest.main()
