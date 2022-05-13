"""Test for linux_packages/cmake.py."""

import unittest
from absl.testing import flagsaver
import mock
from perfkitbenchmarker.linux_packages import cmake
from tests import pkb_common_test_case

_CMAKE_VERSION = 'mock123'


def MockVm(os_type):
  vm = mock.Mock()
  vm.OS_TYPE = os_type
  # response to 'cmake --version'
  vm.RemoteCommand.return_value = _CMAKE_VERSION, ''
  return vm


class CmakeTests(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(cmake_kitware=True)
  def testAptInstallViaKitware(self):
    vm = MockVm('ubuntu1804')
    cmake.AptInstall(vm)
    expected_cmds = [
        'curl --silent https://apt.kitware.com/keys/kitware-archive-latest.asc '
        '| gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg '
        '>/dev/null', 'sudo apt-add-repository '
        '"deb https://apt.kitware.com/ubuntu/ bionic main"'
    ]
    vm.RemoteCommand.assert_has_calls([mock.call(cmd) for cmd in expected_cmds])

  @flagsaver.flagsaver(cmake_kitware=True)
  def testYumInstallViaKitware(self):
    vm = MockVm('centos7')
    with self.assertRaises(ValueError):
      cmake.YumInstall(vm)

  def testYumNonKitware(self):
    vm = MockVm('centos7')
    # no exceptions thrown
    cmake.YumInstall(vm)


if __name__ == '__main__':
  unittest.main()
