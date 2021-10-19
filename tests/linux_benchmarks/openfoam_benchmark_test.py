# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for openfoam_benchmark."""


import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import openfoam_benchmark
from perfkitbenchmarker.linux_packages import openmpi
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class OpenfoamBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                            test_util.SamplesTestMixin):

  def setUp(self):
    super(OpenfoamBenchmarkTest, self).setUp()
    self.mock_vm = mock.Mock()
    self.mock_benchmark_spec = mock.Mock(vms=[self.mock_vm])
    self.enter_context(
        mock.patch.object(openmpi, 'GetMpiVersion', return_value='1.10.2'))
    self.enter_context(
        mock.patch.object(
            openfoam_benchmark, '_GetOpenfoamVersion', return_value='7'))

  @mock.patch.object(openfoam_benchmark, '_ParseRunCommands',
                     return_value=['mpirun $(getApplication)'])
  @flagsaver.flagsaver(openfoam_dimensions=['80_32_32'])
  def testRunCaseReturnsCorrectlyParsedSamples(self,
                                               mock_parseruncommands):
    # Run with mocked output data
    self.mock_vm.RemoteCommand.return_value = None, '\n'.join(
        ['real 131.64', 'user 327.05', 'sys 137.04'])
    self.mock_vm.NumCpusForBenchmark.return_value = 8
    samples = openfoam_benchmark.Run(self.mock_benchmark_spec)

    # Verify command is what we expected to run
    run_cmd = [
        'cd $HOME/OpenFOAM/run/motorBike',
        'time -p mpirun $(getApplication)'
    ]
    self.mock_vm.RemoteCommand.assert_called_with(' && '.join(run_cmd))

    # Verify sample equality
    expected_metadata = {
        'case_name': 'motorbike',
        'command': '$(getApplication)',
        'decomp_method': 'scotch',
        'dimensions': '80_32_32',
        'full_command': 'mpirun $(getApplication)',
        'max_global_cells': 200000000,
        'mpi_mapping': 'core:SPAN',
        'openfoam_version': '7',
        'openmpi_version': '1.10.2',
        'total_cpus_available': 8,
        'total_cpus_used': 4,
    }
    unit = 'seconds'
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_real', 131, unit, expected_metadata), samples[0])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_user', 327, unit, expected_metadata), samples[1])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_sys', 137, unit, expected_metadata), samples[2])

  def testYumInstallRaisesNotImplementedError(self):
    static_vm_spec = static_virtual_machine.StaticVmSpec('test_static_vm_spec')
    self.mock_vm = static_virtual_machine.Rhel7BasedStaticVirtualMachine(
        static_vm_spec)
    self.mock_vm.install_packages = True
    with self.assertRaises(NotImplementedError):
      self.mock_vm.Install('openfoam')

  @flagsaver.flagsaver(
      openfoam_dimensions=['120_48_48'],
      openfoam_num_threads_per_vm=8,
      openfoam_mpi_mapping='hwthread',
      openfoam_decomp_method='simple',
      openfoam_max_global_cells=1e9)
  @mock.patch.object(openfoam_benchmark, '_UseMpi')
  @mock.patch.object(openfoam_benchmark, '_SetDictEntry')
  @mock.patch.object(openfoam_benchmark, '_RunCase')
  def testRunWithMoreFlags(self, mock_runcase, mock_setdict, mock_usempi):
    self.mock_vm.NumCpusForBenchmark.return_value = 16
    test_sample = sample.Sample('mock', 0, 'mock')
    mock_runcase.return_value = [test_sample]

    samples = openfoam_benchmark.Run(self.mock_benchmark_spec)

    mock_runcase.assert_called_with(self.mock_vm, '120_48_48')

    setdict_calls = [
        ('method', 'simple', 'system/decomposeParDict'),
        ('numberOfSubdomains', 8, 'system/decomposeParDict'),
        ('hierarchicalCoeffs.n', '(8 1 1)', 'system/decomposeParDict'),
        ('castellatedMeshControls.maxGlobalCells', float(1e9),
         'system/snappyHexMeshDict'),
    ]
    mock_setdict.assert_has_calls([
        mock.call(self.mock_vm, key, value, dict_file_name)
        for key, value, dict_file_name in setdict_calls
    ])

    mock_usempi.assert_called_with(self.mock_vm, 8, 'hwthread')

    self.assertLen(samples, 1)
    expected_metadata = {
        'case_name': 'motorbike',
        'decomp_method': 'simple',
        'max_global_cells': 1000000000.0,
        'mpi_mapping': 'hwthread',
        'openfoam_version': '7',
        'openmpi_version': '1.10.2',
        'total_cpus_available': 16,
        'total_cpus_used': 8,
    }
    self.assertEqual(expected_metadata, samples[0].metadata)


if __name__ == '__main__':
  unittest.main()
