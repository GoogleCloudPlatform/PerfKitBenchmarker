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

import mock
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import openfoam_benchmark
from tests import pkb_common_test_case


class OpenfoamBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                            test_util.SamplesTestMixin):

  def setUp(self):
    super(OpenfoamBenchmarkTest, self).setUp()
    self.mock_vm = mock.Mock()
    self.benchmark_spec = mock.Mock(vms=[self.mock_vm])

  @mock.patch.object(openfoam_benchmark, '_GetOpenmpiVersion',
                     return_value='1.10.2')
  @mock.patch.object(openfoam_benchmark, '_GetOpenfoamVersion',
                     return_value='7')
  def testRunReturnsCorrectlyParsedSamples(self,
                                           mock_getopenfoamversion,
                                           mock_getmpiversion):
    # Run with mocked output data
    self.mock_vm.RemoteCommand.return_value = None, '\n'.join(
        ['real    4m1.419s', 'user    23m11.198s', 'sys     0m25.274s'])
    samples = openfoam_benchmark.Run(self.benchmark_spec)

    # Verify command is what we expected to run
    run_cmd = [
        'source /opt/openfoam7/etc/bashrc',
        'cd $HOME/Openfoam/${USER}-7/run/motorBike',
        './Allclean',
        'time ./Allrun'
    ]
    self.mock_vm.RemoteCommand.assert_called_with(' && '.join(run_cmd))

    # Verify sample equality
    expected_metadata = {
        'case': 'motorbike',
        'openfoam_version': '7',
        'openmpi_version': '1.10.2'
    }
    unit = 'seconds'
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_real', 241.0, unit, expected_metadata), samples[0])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_user', 1391.0, unit, expected_metadata), samples[1])
    self.assertSamplesEqualUpToTimestamp(
        sample.Sample('time_sys', 25.0, unit, expected_metadata), samples[2])

if __name__ == '__main__':
  unittest.main()
