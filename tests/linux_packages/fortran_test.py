"""Tests for Fortran package."""

import unittest
import mock
from perfkitbenchmarker.linux_packages import fortran
from tests import pkb_common_test_case


def MockVm():
  vm = mock.Mock()
  # For the logging call
  vm.RemoteCommand.return_value = 'fortran_version', ''
  return vm


class FortranRepoTestCase(pkb_common_test_case.PkbCommonTestCase):

  def assertRemoteCommandsCalled(self, expected_calls, vm):
    # call_args_list.assert.... hard to read
    for command, call_arg_entry in zip(expected_calls,
                                       vm.RemoteCommand.call_args_list):
      self.assertEqual(command, call_arg_entry[0][0])
    self.assertLen(vm.RemoteCommand.call_args_list, len(expected_calls))

  def testAptInstall(self) -> None:
    # test when no --fortran_version is passed in
    vm = MockVm()
    fortran.AptInstall(vm)
    vm.InstallPackages.assert_called_with('gfortran')

  def testYumInstall(self) -> None:
    # test when no --fortran_version is passed in
    vm = MockVm()
    fortran.YumInstall(vm)
    vm.InstallPackages.assert_called_with('gcc-gfortran libgfortran')

  def testAptInstallVersion(self) -> None:
    vm = MockVm()
    fortran._AptInstallVersion(vm, 9)
    vm.Install.assert_called_with('ubuntu_toolchain')
    vm.InstallPackages.assert_called_with('gfortran-9')
    expected_commands = [
        'sudo update-alternatives --install /usr/bin/gfortran gfortran '
        '/usr/bin/gfortran-9 100'
    ]
    self.assertRemoteCommandsCalled(expected_commands, vm)

  def testYumInstallVersion(self) -> None:
    vm = MockVm()
    fortran._YumInstallVersion(vm, 9)
    vm.InstallPackages.assert_has_calls([
        mock.call('centos-release-scl-rh'),
        mock.call('devtoolset-9-gcc-gfortran')
    ])
    scl_bin = '/opt/rh/devtoolset-9/root/usr/bin'
    expected_commands = [
        'echo "source scl_source enable devtoolset-9" >> .bashrc',
        f'sudo rm {scl_bin}/sudo',
        'sudo alternatives --install /usr/bin/gfortran-9 fortran '
        f'{scl_bin}/gfortran 100'
    ]
    self.assertRemoteCommandsCalled(expected_commands, vm)


if __name__ == '__main__':
  unittest.main()
