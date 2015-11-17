# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
#
# Contributed by: Zi Shen Lim.

"""Tests for scimark2_benchmark."""

import unittest

from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import scimark2_benchmark

TEST_OUTPUT_C = """;;; C small
**                                                              **
** SciMark2 Numeric Benchmark, see http://math.nist.gov/scimark **
** for details. (Results can be submitted to pozo@nist.gov)     **
**                                                              **
Using       2.00 seconds min time per kenel.
Composite Score:         1596.04
FFT             Mflops:  1568.64    (N=1024)
SOR             Mflops:  1039.98    (100 x 100)
MonteCarlo:     Mflops:   497.64
Sparse matmult  Mflops:  1974.39    (N=1000, nz=5000)
LU              Mflops:  2899.56    (M=100, N=100)
"""

TEST_OUTPUT_JAVA = """;;; Java small
SciMark 2.0a

Composite Score: 1716.3662351463677
FFT (1024): 1000.1380057152871
SOR (100x100):   1353.1987180103354
Monte Carlo : 727.7138820888014
Sparse matmult (N=1000, nz=5000): 1495.40225150659
LU (100x100): 4005.3783184108247

java.vendor: Oracle Corporation
java.version: 1.7.0_75
os.arch: amd64
os.name: Linux
os.version: 3.16.0-25-generic
"""

EXPECTED_C_METADATA = {
    'benchmark_language': 'C',
    'benchmark_size': 'small',
}

EXPECTED_JAVA_METADATA = {
    'benchmark_language': 'Java',
    'benchmark_size': 'small',
    'java.vendor': 'Oracle Corporation',
    'os.version': '3.16.0-25-generic',
    'os.arch': 'amd64',
    'os.name': 'Linux',
    'java.version': '1.7.0_75',
}

EXPECTED_RESULT_C = [
    sample.Sample(metric='Composite Score', value=1596.04,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
    sample.Sample(metric='FFT (N=1024)', value=1568.64,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
    sample.Sample(metric='SOR (100 x 100)', value=1039.98,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
    sample.Sample(metric='MonteCarlo', value=497.64,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
    sample.Sample(metric='Sparse matmult (N=1000, nz=5000)', value=1974.39,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
    sample.Sample(metric='LU (M=100, N=100)', value=2899.56,
                  unit='Mflops', metadata=EXPECTED_C_METADATA),
]

EXPECTED_RESULT_JAVA = [
    sample.Sample(metric='Composite Score', value=1716.3662351463677,
                  unit='Mflops', metadata=EXPECTED_JAVA_METADATA),
    sample.Sample(metric='FFT (1024)', value=1000.1380057152871,
                  unit='Mflops', metadata=EXPECTED_JAVA_METADATA),
    sample.Sample(metric='SOR (100x100)', value=1353.1987180103354,
                  unit='Mflops', metadata=EXPECTED_JAVA_METADATA),
    sample.Sample(metric='Monte Carlo', value=727.7138820888014,
                  unit='Mflops', metadata=EXPECTED_JAVA_METADATA),
    sample.Sample(metric='Sparse matmult (N=1000, nz=5000)',
                  value=1495.40225150659, unit='Mflops',
                  metadata=EXPECTED_JAVA_METADATA),
    sample.Sample(metric='LU (100x100)', value=4005.3783184108247,
                  unit='Mflops', metadata=EXPECTED_JAVA_METADATA),
]


class Scimark2BenchmarkTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def testParseResultsC(self):
    samples = scimark2_benchmark.ParseResults(TEST_OUTPUT_C)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_C)

  def testParseResultsJava(self):
    samples = scimark2_benchmark.ParseResults(TEST_OUTPUT_JAVA)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_JAVA)

  def testParseResultsCombined(self):
    samples = scimark2_benchmark.ParseResults(TEST_OUTPUT_C + TEST_OUTPUT_JAVA)
    self.assertSampleListsEqualUpToTimestamp(
        samples,
        EXPECTED_RESULT_C + EXPECTED_RESULT_JAVA)
