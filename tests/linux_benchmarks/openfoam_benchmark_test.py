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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
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

  @mock.patch.object(openmpi, 'GetMpiVersion', return_value='1.10.2')
  @mock.patch.object(openfoam_benchmark, '_GetOpenfoamVersion',
                     return_value='7')
  @mock.patch.object(openfoam_benchmark, '_ParseRunCommands',
                     return_value=['mpirun $(getApplication)'])
  @flagsaver.flagsaver(openfoam_dimensions=['80_32_32'])
  def testRunCaseReturnsCorrectlyParsedSamples(self,
                                               mock_getopenfoamversion,
                                               mock_getmpiversion,
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
    self.mock_vm = linux_virtual_machine.Rhel7Mixin()
    self.mock_vm.install_packages = True
    with self.assertRaises(NotImplementedError):
      self.mock_vm.Install('openfoam')

if __name__ == '__main__':
  unittest.main()
