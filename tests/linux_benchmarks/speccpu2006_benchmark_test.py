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

"""Tests for speccpu2006_benchmark."""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import speccpu2006_benchmark

TEST_OUTPUT_SPECINT = """
=============================================
400.perlbench    9770        417       23.4 *
401.bzip2        9650        565       17.1 *
403.gcc          8050        364       22.1 *
429.mcf          9120        364       25.1 *
445.gobmk       10490        499       21.0 *
456.hmmer        9330        491       19.0 *
458.sjeng       12100        588       20.6 *
462.libquantum  20720        468       44.2 *
464.h264ref     22130        700       31.6 *
471.omnetpp      6250        349       17.9 *
473.astar        7020        482       14.6 *
483.xalancbmk    6900        248       27.8 *
 Est. SPECint(R)_base2006              22.7
"""

GOOD_METADATA = {'runspec_config': 'linux64-x64-gcc47.cfg',
                 'num_cpus': 256,
                 'runspec_define': '',
                 'runspec_iterations': '3',
                 'runspec_enable_32bit': 'False'}

EXPECTED_RESULT_SPECINT = [
    sample.Sample(metric='400.perlbench', value=23.4, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='401.bzip2', value=17.1, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='403.gcc', value=22.1, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='429.mcf', value=25.1, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='445.gobmk', value=21.0, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='456.hmmer', value=19.0, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='458.sjeng', value=20.6, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='462.libquantum', value=44.2, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='464.h264ref', value=31.6, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='471.omnetpp', value=17.9, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='473.astar', value=14.6, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='483.xalancbmk', value=27.8, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='SPECint(R)_base2006', value=22.7, unit='',
                  metadata=GOOD_METADATA),
]

TEST_OUTPUT_SPECFP = """
=============================================
410.bwaves      13590        717      19.0  *
416.gamess      19580        923      21.2  *
433.milc         9180        480      19.1  *
434.zeusmp       9100        600      15.2  *
435.gromacs      7140        605      11.8  *
436.cactusADM   11950       1289       9.27 *
437.leslie3d     9400        859      10.9  *
444.namd         8020        504      15.9  *
447.dealII      11440        409      28.0  *
450.soplex       8340        272      30.6  *
453.povray       5320        231      23.0  *
454.calculix     8250        993       8.31 *
459.GemsFDTD    10610        775      13.7  *
465.tonto        9840        565      17.4  *
470.lbm         13740        365      37.7  *
481.wrf         11170        788      14.2  *
482.sphinx3     19490        668      29.2  *
 Est. SPECfp(R)_base2006              17.5
"""

EXPECTED_RESULT_SPECFP = [

    sample.Sample(metric='410.bwaves', value=19.0, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='416.gamess', value=21.2, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='433.milc', value=19.1, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='434.zeusmp', value=15.2, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='435.gromacs', value=11.8, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='436.cactusADM', value=9.27, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='437.leslie3d', value=10.9, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='444.namd', value=15.9, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='447.dealII', value=28.0, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='450.soplex', value=30.6, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='453.povray', value=23.0, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='454.calculix', value=8.31, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='459.GemsFDTD', value=13.7, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='465.tonto', value=17.4, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='470.lbm', value=37.7, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='481.wrf', value=14.2, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='482.sphinx3', value=29.2, unit='',
                  metadata=GOOD_METADATA),
    sample.Sample(metric='SPECfp(R)_base2006', value=17.5, unit='',
                  metadata=GOOD_METADATA),
]

# Invalid result, multiple NR failures and no aggregate score.
TEST_OUTPUT_BAD1 = """
==============================================================================
400.perlbench                               NR
401.bzip2                                   NR
403.gcc                                     NR
429.mcf                                     NR
445.gobmk           1  662             15.8 *
456.hmmer           1  631             14.8 *
458.sjeng           1  776             15.6 *
462.libquantum      1  553             37.5 *
464.h264ref         1  778             28.4 *
471.omnetpp         1  536             11.7 *
473.astar           1  657             10.7 *
483.xalancbmk                               NR
 Est. SPECint(R)_rate_base2006           --
 Est. SPECint_rate2006                                              Not Run
"""

EXPECTED_BAD1_METADATA = GOOD_METADATA.copy()
EXPECTED_BAD1_METADATA.update({
    'partial': 'true',
    'missing_results': ('400.perlbench,401.bzip2,403.gcc,429.mcf,'
                        '483.xalancbmk,SPECint(R)_rate_base2006')})

# Invalid result, multiple NR failures, but aggregate score present.
EXPECTED_RESULT_BAD1 = [
    sample.Sample(metric='445.gobmk', value=15.8, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='456.hmmer', value=14.8, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='458.sjeng', value=15.6, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='462.libquantum', value=37.5, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='464.h264ref', value=28.4, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='471.omnetpp', value=11.7, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
    sample.Sample(metric='473.astar', value=10.7, unit='',
                  metadata=EXPECTED_BAD1_METADATA),
]

TEST_OUTPUT_BAD2 = """
==============================================================================
400.perlbench                               NR
401.bzip2                                   NR
403.gcc                                     NR
429.mcf                                     NR
445.gobmk           1  662             15.8 *
456.hmmer           1  631             14.8 *
458.sjeng           1  776             15.6 *
462.libquantum      1  553             37.5 *
464.h264ref         1  778             28.4 *
471.omnetpp         1  536             11.7 *
473.astar           1  657             10.7 *
483.xalancbmk                               NR
 Est. SPECint(R)_rate_base2006         42.0
 Est. SPECint_rate2006                                              Not Run
"""

EXPECTED_BAD2_METADATA = GOOD_METADATA.copy()
EXPECTED_BAD2_METADATA.update({
    'partial': 'true',
    'missing_results': '400.perlbench,401.bzip2,403.gcc,429.mcf,483.xalancbmk',
    'num_cpus': 256})

EXPECTED_RESULT_BAD2 = [
    sample.Sample(metric='445.gobmk', value=15.8, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='456.hmmer', value=14.8, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='458.sjeng', value=15.6, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='462.libquantum', value=37.5, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='464.h264ref', value=28.4, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='471.omnetpp', value=11.7, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='473.astar', value=10.7, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
    sample.Sample(metric='SPECint(R)_rate_base2006', value=42.0, unit='',
                  metadata=EXPECTED_BAD2_METADATA),
]

TEST_OUTPUT_EST = """
==============================================================================
400.perlbench       1        359       27.3 *
401.bzip2           1        597       16.2 *
403.gcc             1        368       21.9 *
429.mcf             1        374       24.4 *
445.gobmk           1        541       19.4 *
456.hmmer           1        425       22.0 *
458.sjeng           1        571       21.2 *
462.libquantum      1        517       40.1 *
464.h264ref         1        607       36.4 *
471.omnetpp         1        513       12.2 *
473.astar           1        491       14.3 *
483.xalancbmk       1        318       21.7 *
 Est. SPECint(R)_rate_base2006           --
 """

EXPECTED_EST_METADATA = GOOD_METADATA.copy()
EXPECTED_EST_METADATA.update({
    'partial': 'true',
    'missing_results': 'SPECint(R)_rate_base2006',
    'num_cpus': 256})

EXPECTED_RESULT_EST = [
    sample.Sample(metric='400.perlbench', value=27.3, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='401.bzip2', value=16.2, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='403.gcc', value=21.9, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='429.mcf', value=24.4, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='445.gobmk', value=19.4, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='456.hmmer', value=22.0, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='458.sjeng', value=21.2, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='462.libquantum', value=40.1, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='464.h264ref', value=36.4, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='471.omnetpp', value=12.2, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='473.astar', value=14.3, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='483.xalancbmk', value=21.7, unit='',
                  metadata=EXPECTED_EST_METADATA),
    sample.Sample(metric='estimated_SPECint(R)_rate_base2006',
                  value=21.846042257681507, unit='',
                  metadata=EXPECTED_EST_METADATA),
]


class DummyVM(object):

  def __init__(self):
    self.num_cpus = 256


class Speccpu2006BenchmarkTestCase(unittest.TestCase,
                                   test_util.SamplesTestMixin):

  def testParseResultsC(self):
    self.maxDiff = None

    vm = DummyVM()

    samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_SPECINT, vm,
                                                  False, False)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_SPECINT)

    samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_SPECFP, vm,
                                                  False, False)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_SPECFP)

    # By default, incomplete results result in error.
    with self.assertRaises(errors.Benchmarks.RunError):
      samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_BAD1, vm,
                                                    False, False)

    with self.assertRaises(errors.Benchmarks.RunError):
      samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_BAD2, vm,
                                                    False, False)

    # Now use keep_partial_results
    samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_BAD1, vm,
                                                  True, False)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_BAD1)

    samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_BAD2, vm,
                                                  True, False)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_BAD2)

    # Estimate scores
    samples = speccpu2006_benchmark._ExtractScore(TEST_OUTPUT_EST, vm,
                                                  True, True)
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_EST)
