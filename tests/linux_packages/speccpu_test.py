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

"""Tests for speccpu."""

import unittest
import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import speccpu2017_benchmark  # noqa
from perfkitbenchmarker.linux_packages import speccpu

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

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
                 'runspec_define': '',
                 'runspec_iterations': '3',
                 'runspec_enable_32bit': 'False',
                 'runspec_metric': 'rate',
                 'spec_runmode': 'base',
                 'spec17_fdo': False,
                 'spec17_copies': None,
                 'spec17_threads': None,
                 'spec17_subset': ['intspeed', 'fpspeed', 'intrate', 'fprate']}

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
    'missing_results': '400.perlbench,401.bzip2,403.gcc,429.mcf,483.xalancbmk'})

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
    'missing_results': 'SPECint(R)_rate_base2006'})

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


SPEED_OUTPUT_SPECINT = """
==============================================================================
400.perlbench    9770        332       29.5 *
401.bzip2        9650        557       17.3 *
403.gcc          8050        342       23.5 *
429.mcf          9120        479       19.0 *
445.gobmk       10490        517       20.3 *
456.hmmer        9330        393       23.8 *
458.sjeng       12100        539       22.5 *
462.libquantum  20720        525       39.5 *
464.h264ref     22130        573       38.6 *
471.omnetpp      6250        489       12.8 *
473.astar        7020        504       13.9 *
483.xalancbmk    6900        309       22.3 *
 Est. SPECint(R)_base2006              22.3
 Est. SPECint2006                                                   Not Run
"""
SPEED_METADATA = GOOD_METADATA.copy()
SPEED_METADATA['runspec_metric'] = 'speed'
EXPECTED_SPEED_RESULT_SPECINT = [
    sample.Sample(metric='400.perlbench:speed', value=29.5, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='401.bzip2:speed', value=17.3, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='403.gcc:speed', value=23.5, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='429.mcf:speed', value=19.0, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='445.gobmk:speed', value=20.3, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='456.hmmer:speed', value=23.8, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='458.sjeng:speed', value=22.5, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='462.libquantum:speed', value=39.5, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='464.h264ref:speed', value=38.6, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='471.omnetpp:speed', value=12.8, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='473.astar:speed', value=13.9, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='483.xalancbmk:speed', value=22.3, unit='',
                  metadata=SPEED_METADATA),
    sample.Sample(metric='SPECint(R)_base2006:speed', value=22.3, unit='',
                  metadata=SPEED_METADATA),
]

TEST_OUTPUT_ALL = """
================================================================================
410.bwaves_r      13590        717      19.0  *    12345        710      19.1  *
507.cactuBSSN_r   19580        923      21.2  *    12346        711      29.0  *
508.namd_r         9180        480      19.1  *    12347        712      39.0  *
 Est. SPECrate2017_fp_base             4.90
 Est. SPECrate2017_fp_peak                                         12.3
"""
ALL_METADATA = GOOD_METADATA.copy()
ALL_METADATA['spec_runmode'] = 'all'
ALL_METADATA['runspec_metric'] = None
EXPECTED_ALL_RESULT_SPECINT = [
    sample.Sample(metric='410.bwaves_r', value=19.0, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='410.bwaves_r:peak', value=19.1, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='507.cactuBSSN_r', value=21.2, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='507.cactuBSSN_r:peak', value=29.0, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='508.namd_r', value=19.1, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='508.namd_r:peak', value=39.0, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='SPECrate2017_fp_base', value=4.90, unit='',
                  metadata=ALL_METADATA),
    sample.Sample(metric='SPECrate2017_fp_peak', value=12.3, unit='',
                  metadata=ALL_METADATA),
]


TEST_OUTPUT_PEAK = """
================================================================================
410.bwaves_r                                NR    8        710      19.1  *
507.cactuBSSN_r                             NR    8        711      29.0  *
508.namd_r                                  NR    8        712      39.0  *
 Est. SPECrate2017_fp_base             Not Run
 Est. SPECrate2017_fp_peak                                         12.3
"""
PEAK_METADATA = GOOD_METADATA.copy()
PEAK_METADATA['spec_runmode'] = 'peak'
PEAK_METADATA['runspec_metric'] = None
EXPECTED_PEAK_RESULT_SPECINT = [
    sample.Sample(metric='410.bwaves_r:peak', value=19.1, unit='',
                  metadata=PEAK_METADATA),
    sample.Sample(metric='507.cactuBSSN_r:peak', value=29.0, unit='',
                  metadata=PEAK_METADATA),
    sample.Sample(metric='508.namd_r:peak', value=39.0, unit='',
                  metadata=PEAK_METADATA),
    sample.Sample(metric='SPECrate2017_fp_peak', value=12.3, unit='',
                  metadata=PEAK_METADATA),
]


TEST_OUTPUT_PARTIAL_PEAK = """
================================================================================
410.bwaves_r                                NR                           NR
507.cactuBSSN_r                             NR    8        711      29.0  *
508.namd_r                                  NR                           NR
 Est. SPECrate2017_fp_base             Not Run
 Est. SPECrate2017_fp_peak                                         12.3
"""
PARTIAL_PEAK_METADATA = GOOD_METADATA.copy()
PARTIAL_PEAK_METADATA['spec_runmode'] = 'peak'
PARTIAL_PEAK_METADATA['runspec_metric'] = None
PARTIAL_PEAK_METADATA.update({
    'partial': 'true',
    'missing_results': ('410.bwaves_r,508.namd_r')})
EXPECTED_PARTIAL_PEAK_RESULT_SPECINT = [
    sample.Sample(metric='507.cactuBSSN_r:peak', value=29.0, unit='',
                  metadata=PARTIAL_PEAK_METADATA),
    sample.Sample(metric='SPECrate2017_fp_peak', value=12.3, unit='',
                  metadata=PARTIAL_PEAK_METADATA),
]


class Speccpu2006BenchmarkTestCase(unittest.TestCase,
                                   test_util.SamplesTestMixin):

  def testParseResultsC(self):
    vm = mock.Mock(vm=linux_virtual_machine.DebianMixin)
    spec_test_config = speccpu.SpecInstallConfigurations()
    spec_test_config.benchmark_name = 'speccpu2006'
    spec_test_config.log_format = r'Est. (SPEC.*_base2006)\s*(\S*)'
    spec_test_config.runspec_config = r'linux64-x64-gcc47.cfg'
    speccpu.FLAGS.spec_runmode = 'base'
    vm.speccpu_vm_state = spec_test_config

    samples = speccpu._ExtractScore(TEST_OUTPUT_SPECINT, vm,
                                    False, 'rate')
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_SPECINT)

    samples = speccpu._ExtractScore(TEST_OUTPUT_SPECFP, vm,
                                    False, 'rate')
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_SPECFP)

    # By default, incomplete results result in error.
    with self.assertRaises(errors.Benchmarks.RunError):
      samples = speccpu._ExtractScore(TEST_OUTPUT_BAD1, vm,
                                      False, 'rate')

    with self.assertRaises(errors.Benchmarks.RunError):
      samples = speccpu._ExtractScore(TEST_OUTPUT_BAD2, vm,
                                      False, 'rate')

    # Now use keep_partial_results
    samples = speccpu._ExtractScore(TEST_OUTPUT_BAD1, vm,
                                    True, 'rate')
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_BAD1)

    samples = speccpu._ExtractScore(TEST_OUTPUT_BAD2, vm,
                                    True, 'rate')
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_BAD2)

    # Estimate scores
    speccpu.FLAGS.runspec_estimate_spec = True
    samples = speccpu._ExtractScore(TEST_OUTPUT_EST, vm,
                                    True, 'rate')
    self.assertSampleListsEqualUpToTimestamp(samples, EXPECTED_RESULT_EST)

  def testParseSpeedResults(self):
    speccpu.FLAGS.spec_runmode = 'base'
    vm = mock.Mock(vm=linux_virtual_machine.DebianMixin)
    spec_test_config = speccpu.SpecInstallConfigurations()
    spec_test_config.benchmark_name = 'speccpu2006'
    spec_test_config.log_format = r'Est. (SPEC.*_base2006)\s*(\S*)'
    spec_test_config.runspec_config = r'linux64-x64-gcc47.cfg'
    vm.speccpu_vm_state = spec_test_config
    samples = speccpu._ExtractScore(SPEED_OUTPUT_SPECINT, vm,
                                    False, 'speed')
    self.assertSampleListsEqualUpToTimestamp(
        samples, EXPECTED_SPEED_RESULT_SPECINT)

  def testParseAllResults(self):
    speccpu.FLAGS.spec_runmode = 'all'
    vm = mock.Mock(vm=linux_virtual_machine.DebianMixin)
    spec_test_config = speccpu.SpecInstallConfigurations()
    spec_test_config.benchmark_name = 'speccpu2017'
    spec_test_config.log_format = r'Est. (SPEC.*2017_.*_base)\s*(\S*)'
    spec_test_config.runspec_config = r'linux64-x64-gcc47.cfg'
    vm.speccpu_vm_state = spec_test_config
    samples = speccpu._ExtractScore(TEST_OUTPUT_ALL, vm,
                                    False, None)
    self.assertSampleListsEqualUpToTimestamp(
        samples, EXPECTED_ALL_RESULT_SPECINT)

  def testParsePeakResults(self):
    speccpu.FLAGS.spec_runmode = 'peak'
    vm = mock.Mock(vm=linux_virtual_machine.DebianMixin)
    spec_test_config = speccpu.SpecInstallConfigurations()
    spec_test_config.benchmark_name = 'speccpu2017'
    spec_test_config.log_format = r'Est. (SPEC.*2017_.*_base)\s*(\S*)'
    spec_test_config.runspec_config = r'linux64-x64-gcc47.cfg'
    vm.speccpu_vm_state = spec_test_config
    samples = speccpu._ExtractScore(TEST_OUTPUT_PEAK, vm,
                                    False, None)
    self.assertSampleListsEqualUpToTimestamp(
        samples, EXPECTED_PEAK_RESULT_SPECINT)

  def testParsePartialPeakResults(self):
    speccpu.FLAGS.spec_runmode = 'peak'
    vm = mock.Mock(vm=linux_virtual_machine.DebianMixin)
    spec_test_config = speccpu.SpecInstallConfigurations()
    spec_test_config.benchmark_name = 'speccpu2017'
    spec_test_config.log_format = r'Est. (SPEC.*2017_.*_base)\s*(\S*)'
    spec_test_config.runspec_config = r'linux64-x64-gcc47.cfg'
    vm.speccpu_vm_state = spec_test_config
    samples = speccpu._ExtractScore(TEST_OUTPUT_PARTIAL_PEAK, vm,
                                    True, None)
    self.assertSampleListsEqualUpToTimestamp(
        samples, EXPECTED_PARTIAL_PEAK_RESULT_SPECINT)


if __name__ == '__main__':
  unittest.main()
