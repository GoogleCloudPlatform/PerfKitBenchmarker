# Lint as: python3
"""Tests for Intel Repo package."""

import unittest
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import intel_repo
from tests import pkb_common_test_case


class IntelRepoTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testAptPrepare(self) -> None:
    vm = mock.Mock()
    intel_repo.AptPrepare(vm)
    vm.PushDataFile.call_args_list[0].assert_called_with(
        'intel_repo_key.txt', '/tmp/pkb/intel_repo_key.txt')
    vm.PushDataFile.call_args_list[1].assert_called_with(
        'intel_repo_list.txt', '/tmp/pkb/intel.list')
    vm.InstallPackages.assert_called_with('libgomp1')

  def testYumPrepare(self) -> None:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = ('', '', 0)
    intel_repo.YumPrepare(vm)
    vm.PushDataFile.assert_called_with('intel_repo_key.txt',
                                       '/tmp/pkb/intel_repo_key.txt')
    vm.InstallPackages.assert_called_with('yum-utils')
    vm.RemoteCommandWithReturnCode.assert_called_with(
        'diff /tmp/pkb/intel_repo_key.txt /tmp/pkb/mpi.yumkey')

  def testYumPrepareBadKey(self) -> None:
    vm = mock.Mock()
    vm.RemoteCommandWithReturnCode.return_value = ('', '', 1)
    with self.assertRaises(errors.Setup.InvalidConfigurationError):
      intel_repo.YumPrepare(vm)


if __name__ == '__main__':
  unittest.main()
