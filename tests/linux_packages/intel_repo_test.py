"""Tests for Intel Repo package."""

import unittest
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import intel_repo
from tests import pkb_common_test_case


class IntelRepoTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testAptInstall(self) -> None:
    vm = mock.Mock()
    intel_repo.AptInstall(vm)
    vm.PushDataFile.call_args_list[0].assert_called_with(
        'intel_repo_key.txt', '/tmp/pkb/intel_repo_key.txt')
    vm.PushDataFile.call_args_list[1].assert_called_with(
        'intel_repo_list.txt', '/tmp/pkb/intel.list')
    vm.InstallPackages.assert_called_with('libgomp1')

  def testYumInstall(self) -> None:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = ('', '', 0)
    intel_repo.YumInstall(vm)
    vm.PushDataFile.assert_called_with('intel_repo_key.txt',
                                       '/tmp/pkb/intel_repo_key.txt')
    vm.InstallPackages.assert_called_with('yum-utils')
    vm.RemoteCommandWithReturnCode.assert_called_with(
        'diff /tmp/pkb/intel_repo_key.txt /tmp/pkb/mpi.yumkey')

  def testYumInstallBadKey(self) -> None:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = ('', '', 1)
    with self.assertRaises(errors.Setup.InvalidConfigurationError):
      intel_repo.YumInstall(vm)

  @flagsaver.flagsaver(intelmpi_version='2021.2.1')
  def testAptInstall2021(self) -> None:
    vm = mock.Mock()
    intel_repo.AptInstall(vm)
    vm.PushDataFile.call_args_list[0].assert_called_with(
        'intel_repo_key.txt', '/tmp/pkb/intel_repo_key.txt')
    self.assertLen(vm.PushDataFile.call_args_list, 1)
    expected_command = (
        'sudo apt-key add /tmp/pkb/intel_repo_key.txt;'
        'rm /tmp/pkb/intel_repo_key.txt;'
        'echo "deb https://apt.repos.intel.com/oneapi all main" '
        '| sudo tee /etc/apt/sources.list.d/oneAPI.list;'
        'sudo apt-get update')
    vm.RemoteCommand.assert_called_with(expected_command)


if __name__ == '__main__':
  unittest.main()
