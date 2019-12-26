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

import os
import subprocess
import tempfile
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import openfoam_benchmark
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


TEST_BLOCKMESH_DICT_PATH = os.path.join(
    os.path.dirname(__file__), '../data/openfoam_blockmesh_dict.txt')


class OpenfoamBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                            test_util.SamplesTestMixin):

  def setUp(self):
    super(OpenfoamBenchmarkTest, self).setUp()
    self.mock_vm = mock.Mock()
    self.mock_benchmark_spec = mock.Mock(vms=[self.mock_vm])

  @mock.patch.object(openfoam_benchmark, '_GetOpenmpiVersion',
                     return_value='1.10.2')
  @mock.patch.object(openfoam_benchmark, '_GetOpenfoamVersion',
                     return_value='7')
  @flagsaver.flagsaver(openfoam_dimensions=['80_32_32'])
  def testRunReturnsCorrectlyParsedSamples(self,
                                           mock_getopenfoamversion,
                                           mock_getmpiversion):
    # Run with mocked output data
    self.mock_vm.RemoteCommand.return_value = None, '\n'.join(
        ['real    4m1.419s', 'user    23m11.198s', 'sys     0m25.274s'])
    self.mock_vm.NumCpusForBenchmark.return_value = 8
    samples = openfoam_benchmark.Run(self.mock_benchmark_spec)

    # Verify command is what we expected to run
    run_cmd = [
        'cd $HOME/OpenFOAM/run/motorBike',
        './Allclean',
        'time ./Allrun'
    ]
    self.mock_vm.RemoteCommand.assert_called_with(' && '.join(run_cmd))

    # Verify sample equality
    expected_metadata = {
        'case_name': 'motorbike',
        'decomp_method': 'scotch',
        'dimensions': '80 32 32',
        'mpi_mapping': 'core:SPAN',
        'openfoam_version': '7',
        'openmpi_version': '1.10.2',
        'total_cpus_available': 8,
        'total_cpus_used': 4,
    }
    unit = 'seconds'
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_real', 241, unit, expected_metadata), samples[0])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_user', 1391, unit, expected_metadata), samples[1])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_sys', 25, unit, expected_metadata), samples[2])

  def testYumInstallRaisesNotImplementedError(self):
    self.mock_vm = linux_virtual_machine.RhelMixin()
    self.mock_vm.install_packages = True
    with self.assertRaises(NotImplementedError):
      self.mock_vm.Install('openfoam')

  def _mockRemoteCommand(self, command):
    subprocess.call(command, shell=True)

  def testSetDimensionsCorrectlyReplacesText(self):
    """Test of _SetDimensions().

    This test creates a copy of the data/openfoam_blockmesh_dict.txt file,
    replaces the dimensions line in the copy, and checks that the copy has
    the correctly formatted line that we expect.
    """
    with open(TEST_BLOCKMESH_DICT_PATH, 'rb') as f:
      lines = f.read()
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(lines)
    temp_file.close()
    self.mock_vm.RemoteCommand = self._mockRemoteCommand
    mock.patch.object(openfoam_benchmark, '_GetPath',
                      return_value=temp_file.name).start()
    openfoam_benchmark._SetDimensions(self.mock_vm, '10 10 10')
    with open(temp_file.name, 'rb') as temp_file:
      new_lines = temp_file.read()
      expected = b'hex (0 1 2 3 4 5 6 7) (10 10 10) simpleGrading (1 1 1)'
      self.assertIn(expected, new_lines)

if __name__ == '__main__':
  unittest.main()
