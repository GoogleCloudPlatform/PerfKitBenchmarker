"""Tests for Intel MPI benchmark."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import os_types
from perfkitbenchmarker.linux_packages import imb
from perfkitbenchmarker.linux_packages import intelmpi
# Required for --mpi_vendor flag.
from perfkitbenchmarker.linux_packages import mpi  # pylint: disable=unused-import

from tests import pkb_common_test_case


def MockVm():
  return mock.Mock(
      internal_ip='1.2.3.4', NumCpusForBenchmark=8, BASE_OS_TYPE=os_types.RHEL
  )


class IntelMpiLibTestCase(pkb_common_test_case.PkbCommonTestCase):
  MPIVARS_FILE = (
      '/opt/intel/compilers_and_libraries/linux/mpi/intel64/bin/mpivars.sh'
  )

  COMPILE_2019 = (
      'cd mpi-benchmarks; '
      '. /opt/intel/mkl/bin/mklvars.sh intel64; '
      '. /opt/intel/compilers_and_libraries/'
      'linux/bin/compilervars.sh intel64; '
      'CC=mpicc CXX=mpicxx make'
  )
  COMPILE_2021 = (
      'cd mpi-benchmarks; '
      '. /opt/intel/oneapi/setvars.sh; '
      'CC=mpicc CXX=mpicxx make'
  )

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(mpi_vendor='intel'))

  def MockVmWithReturnValues(self):
    # for use when calling intelmpi.py commands to find mpivars, MPI version
    vm = MockVm()
    vm_returns = [
        self.MPIVARS_FILE,
        (
            'Intel(R) MPI Library for Linux* OS, '
            'Version 2018 Update 4 Build 20180823 (id: 18555)'
        ),
    ]
    vm.RemoteCommand.side_effect = [(txt, '') for txt in vm_returns]
    return vm

  def testInstallCompileSource(self) -> None:
    vm = MockVm()
    imb.Install(vm)
    # TODO(user) taken out due to not installing MKL
    # vm.InstallPackages.assert_called_with('intel-mkl-2020.1-102')
    # just confirm that the git clone and patch were done
    cmd = ';'.join([cmd[0][0] for cmd in vm.RemoteCommand.call_args_list])
    self.assertRegex(
        cmd,
        'git clone -n https://github.com/intel/mpi-benchmarks.git',
        'Missing git clone command',
    )
    self.assertRegex(
        cmd,
        'patch -d mpi-benchmarks -p3 < ~/intelmpi.patch',
        'Missing patch command',
    )

  def testMpirunMpiVersion(self):
    vm = self.MockVmWithReturnValues()

    mpi_version = intelmpi.MpirunMpiVersion(vm)

    self.assertEqual('2018.4', mpi_version)
    vm.RemoteCommand.assert_called_with(f'. {self.MPIVARS_FILE}; mpirun -V')

  def testMpirunMpiVersionError(self):
    vm = MockVm()
    vm.RemoteCommand.return_value = 'Non parsable text', ''

    with self.assertRaises(ValueError):
      intelmpi.MpirunMpiVersion(vm)

  @parameterized.parameters((2, ' -ppn 1'), (4, ''))
  def testPpn(self, total_processes, expected_suffix):
    vm = self.MockVmWithReturnValues()
    hosts = ['10.0.0.1', '10.0.0.2']

    mpirun = imb.MpiRunCommand(vm, hosts, total_processes, 0, [], [], False)

    # '-ppn 1' is only seen when running single threaded tests
    expected_mpirun = (
        f'mpirun -n {total_processes} -hosts 10.0.0.1,10.0.0.2{expected_suffix}'
    )
    self.assertEqual(f'. {self.MPIVARS_FILE}; {expected_mpirun}', mpirun)

  @parameterized.parameters(
      ('2019.6', COMPILE_2019, []),
      (
          '2021.2',
          COMPILE_2021,
          ['intel-oneapi-compiler-dpcpp-cpp', 'intel-oneapi-mpi-devel'],
      ),
  )
  def testInstall2021(
      self, intelmpi_version, expected_compile_cmd, installed_packages
  ):
    vm = MockVm()
    with flagsaver.flagsaver(intelmpi_version=intelmpi_version):
      imb.Install(vm)
    vm.RemoteCommand.assert_any_call(expected_compile_cmd)
    vm.InstallPackages.assert_has_calls(
        [mock.call(pkb) for pkb in installed_packages]
    )


class OpenMpiLibTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(mpi_vendor='openmpi'))

  def testInstallCompileSource(self) -> None:
    vm = MockVm()
    imb.Install(vm)
    cmd = ';'.join([cmd[0][0] for cmd in vm.RemoteCommand.call_args_list])
    self.assertRegex(
        cmd,
        'git clone -n https://github.com/intel/mpi-benchmarks.git',
        'Missing git clone command',
    )
    self.assertRegex(
        cmd,
        'patch -d mpi-benchmarks -p3 < ~/intelmpi.patch',
        'Missing patch command',
    )

  @flagsaver.flagsaver(imb_compile_from_source=False)
  def testInstallWithoutImbCompileFromSourceThrows(self) -> None:
    vm = MockVm()
    with self.assertRaises(ValueError) as e:
      imb.Install(vm)
    self.assertEqual(
        str(e.exception),
        '--mpi_vendor=openmpi requires --imb_compile_from_source',
    )

  def testMpiRunCommandEnvVarsExported(self):
    vm = MockVm()
    total_proc = 2
    ppn = 1
    hosts = ['10.0.0.1', '10.0.0.2']
    environment = [
        'OMPI_MCA_btl=self,tcp',
        'OMPI_MCA_rmaps_base_mapping_policy=node:PE=1',
    ]

    mpirun = imb.MpiRunCommand(
        vm, hosts, total_proc, ppn, environment, [], False
    )

    expected_mpirun = (
        'OMPI_MCA_btl=self,tcp OMPI_MCA_rmaps_base_mapping_policy=node:PE=1 '
        'mpirun -x OMPI_MCA_btl -x OMPI_MCA_rmaps_base_mapping_policy '
        '-report-bindings -display-map -n 2 -npernode 1 --use-hwthread-cpus '
        '-host 10.0.0.1:slots=2,10.0.0.2:slots=2'
    )
    self.assertEqual(expected_mpirun, mpirun)

  def testMpiRunCommandNoEnvVarsIsFormattedCorrectly(self):
    vm = MockVm()
    total_proc = 2
    ppn = 1
    hosts = ['10.0.0.1', '10.0.0.2']
    environment = []

    mpirun = imb.MpiRunCommand(
        vm, hosts, total_proc, ppn, environment, [], False
    )

    expected_mpirun = (
        'mpirun -report-bindings -display-map -n 2 -npernode 1 '
        '--use-hwthread-cpus -host 10.0.0.1:slots=2,10.0.0.2:slots=2'
    )
    self.assertEqual(expected_mpirun, mpirun)

  def testMpiRunCommandNoPpnSpecified(self):
    vm = MockVm()
    total_proc = 8
    ppn = 0
    hosts = ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']
    environment = []

    mpirun = imb.MpiRunCommand(
        vm, hosts, total_proc, ppn, environment, [], False
    )
    expected_mpirun = (
        'mpirun -report-bindings -display-map -n 8 -npernode 2 '
        '--use-hwthread-cpus -host '
        '10.0.0.1:slots=8,10.0.0.2:slots=8,10.0.0.3:slots=8,10.0.0.4:slots=8'
    )
    self.assertEqual(expected_mpirun, mpirun)


if __name__ == '__main__':
  unittest.main()
