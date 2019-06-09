# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for Coremark benchmark."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import coremark_benchmark
from tests import pkb_common_test_case


_SAMPLE_OUTPUT_FILE = os.path.join(
    os.path.dirname(__file__), '../data/coremark_sample_output.txt')


class CoremarkBenchmarkTest(pkb_common_test_case.PkbCommonTestCase):

  def testParseOutput(self):
    """Tests parsing valid output from running Coremark."""
    with open(_SAMPLE_OUTPUT_FILE) as f:
      output = f.read()
    samples = coremark_benchmark._ParseOutputForSamples(output)
    self.assertEqual('Coremark Score', samples[0].metric)
    self.assertAlmostEqual(29504.182218, samples[0].value)
    self.assertEqual('CoreMark 1.0 : 29504.182218 / GCC7.3.0 -O2 -g -O2 '
                     '-DMULTITHREAD=2 -DUSE_PTHREAD -DPERFORMANCE_RUN=1 '
                     '-DPERFORMANCE_RUN=1  -lrt -lpthread / Heap / 2:PThreads',
                     samples[0].metadata['summary'])
    self.assertEqual(666, samples[0].metadata['size'])
    self.assertEqual(67787, samples[0].metadata['total_ticks'])
    self.assertAlmostEqual(67.787000, samples[0].metadata['total_time_sec'])
    self.assertEqual(2000000, samples[0].metadata['iterations'])
    self.assertEqual(coremark_benchmark.ITERATIONS_PER_CPU,
                     samples[0].metadata['iterations_per_cpu'])
    self.assertEqual('PTHREAD', samples[0].metadata['parallelism_method'])

  def testParseInvalidOutput(self):
    """Tests failing when Coremark does not report valid output."""
    with open(_SAMPLE_OUTPUT_FILE) as f:
      output = f.read()
    output = output.replace('Correct operation validated', 'Invalid run')
    with self.assertRaises(errors.Benchmarks.RunError):
      coremark_benchmark._ParseOutputForSamples(output)


if __name__ == '__main__':
  unittest.main()
