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
"""Tests for perfkitbenchmarker.linux_packages.epel_release."""

import unittest
from absl.testing import parameterized
import mock

from perfkitbenchmarker import os_types
from perfkitbenchmarker.linux_packages import epel_release
from tests import pkb_common_test_case

_REPOLIST_NO_EPEL = """
repo id                             repo name                             status
base/7/x86_64                       CentOS-7 - Base                       10070
updates/7/x86_64                    CentOS-7 - Updates                      900
repolist: 11382
"""

_REPOLIST_WITH_EPEL = """
repo id               repo name                                           status
base/7/x86_64         CentOS-7 - Base                                     10070
epel/x86_64           Extra Packages for Enterprise Linux 7 - x86_64      13421
updates/7/x86_64      CentOS-7 - Updates                                    900
repolist: 24803
"""

_REPOLIST_WITH_EPEL_REMOTE = """
repo id               repo name                                           status
*epel/x86_64          Extra Packages for Enterprise Linux 7 - x86_64      13421
"""

EMPTY_RES = ''


def Vm(os_type, responses):
  vm_spec = pkb_common_test_case.CreateTestVmSpec()
  vm = pkb_common_test_case.TestLinuxVirtualMachine(vm_spec=vm_spec)
  # pylint: disable=invalid-name
  vm.OS_TYPE = os_type
  vm.RemoteCommand = mock.Mock()
  vm.InstallPackages = mock.Mock()
  vm.RemoteCommand.side_effect = [(text, '') for text in responses]
  # pylint: enable=invalid-name
  return vm


class EpelReleaseTest(pkb_common_test_case.PkbCommonTestCase):

  def testHappyPathCentos8(self):
    responses = [
        _REPOLIST_NO_EPEL,  # initial call for repo list
        EMPTY_RES,  # centos8 PowerTools
        EMPTY_RES,  # config manager
        _REPOLIST_WITH_EPEL,  # call to confirm epel repo present
    ]
    vm = Vm(os_types.CENTOS8, responses)
    epel_release.YumInstall(vm)
    self.assertEqual(
        vm.InstallPackages.call_args_list,
        [mock.call(epel_release._EPEL_URL.format(8)),
         mock.call('yum-utils')])

  def testRepoAllReadyInstalled(self):
    vm = Vm(os_types.CENTOS7, [_REPOLIST_WITH_EPEL])
    epel_release.YumInstall(vm)
    vm.RemoteCommand.assert_called_once()

  @parameterized.named_parameters(
      ('NoEpelRepo', _REPOLIST_NO_EPEL,
       ('base/7/x86_64', 'updates/7/x86_64'), False),
      ('HasEpelRepo', _REPOLIST_WITH_EPEL,
       ('epel/x86_64', 'base/7/x86_64', 'updates/7/x86_64'), True),
      ('HasRemoteEpelRepo', _REPOLIST_WITH_EPEL_REMOTE, ('epel/x86_64',), True),
  )
  def testRepoList(self, repo_response, repo_ids, repo_enabled):
    vm = Vm(os_types.CENTOS7, [repo_response, repo_response])
    self.assertEqual(epel_release.Repolist(vm), frozenset(repo_ids))
    self.assertEqual(epel_release.IsEpelRepoInstalled(vm), repo_enabled)


if __name__ == '__main__':
  unittest.main()
