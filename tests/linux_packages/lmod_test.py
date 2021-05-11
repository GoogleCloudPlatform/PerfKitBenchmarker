"""Tests for lmod."""

import unittest
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import lmod
from tests import pkb_common_test_case


def MockVm():
  return mock.Mock()


class LmodTest(pkb_common_test_case.PkbCommonTestCase):

  def testYumInstall(self):
    vm = MockVm()

    lmod.YumInstall(vm)

    vm.InstallPackages.assert_called_with('Lmod')
    vm.RemoteCommand.assert_not_called()

  def testAptInstall(self):
    vm = MockVm()
    vm.TryRemoteCommand.side_effect = [
        True,  # lua directory exists
        True,  # lua posix.so file exists
    ]

    lmod.AptInstall(vm)

    vm.InstallPackages.assert_called_with('lmod')
    lua_dir = '/usr/lib/x86_64-linux-gnu/lua/5.2'
    vm.RemoteCommand.assert_has_calls([
        mock.call(f'sudo ln -s {lua_dir}/posix_c.so {lua_dir}/posix.so'),
    ])

  def testAptInstallFails(self):
    vm = MockVm()
    vm.TryRemoteCommand.side_effect = [
        False,  # lua directory does not exist
    ]

    with self.assertRaises(errors.Setup.InvalidSetupError):
      lmod.AptInstall(vm)


if __name__ == '__main__':
  unittest.main()
