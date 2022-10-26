# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.linux_packages.hammerdb.py."""

# Used for flags
# pylint: disable=unused-import

import os
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import sample

from perfkitbenchmarker.linux_packages.hammerdb import ParseTpcCResults
from perfkitbenchmarker.linux_packages.hammerdb import ParseTpcCTimeProfileResultsFromFile
from perfkitbenchmarker.linux_packages.hammerdb import ParseTpcCTPMResultsFromFile
from perfkitbenchmarker.linux_packages.hammerdb import ParseTpcHResults

# Used for flags
# pylint: disable=unused-import

FLAGS = flags.FLAGS


def _ReadFileToString(filename):
  """Helper function to read a file into a string."""
  with open(filename) as f:
    return f.read()


TEST_DATA_DIR = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'hammerdb')
TPCC_TIMEPROFILE_LOG = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'hammerdbcli_tpcc_timeprofile.log'))

TPM_PER_SECOND_4_3_LOG = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'hammerdbcli_tpcc_tpm_per_second_4_3.log'))

TPCC_TIMEPROFILE_4_3_LOG = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'hammerdbcli_tpcc_timeprofile_4_3.log'))

TPCC_LOG = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'hammerdbcli_tpcc.log'))

TPCH_LOG = _ReadFileToString(
    os.path.join(TEST_DATA_DIR, 'hammerdbcli_tpch.log'))


# Enable tests when flags are migrated.
'''
class HammerdbBenchmarkTest(googletest.TestCase):

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testHammerdbBenchmarkRun(self):
    benchmark_spec = mock.MagicMock()
    FLAGS.hammerdbcli_script = 'tpc_c'
    FLAGS.managed_db_engine = 'mysql'
    FLAGS.hammerdbcli_tpcc_time_profile = False
    FLAGS.hammerdbcli_num_vu = 4
    FLAGS.hammerdbcli_version = '4.0'
    vm = mock.Mock()
    vm.RemoteCommand = mock.Mock(return_value=(TPCC_LOG, ''))
    benchmark_spec.vms = [vm]
    output = hammerdbcli_benchmark.Run(benchmark_spec)
    expected = [
        sample.Sample(
            metric='TPM',
            value=24772.0,
            unit='TPM',
            metadata={
                'hammerdbcli_version': '4.0',
                'hammerdbcli_script': 'tpc_c',
                'hammerdbcli_vu': 4,
                'hammerdbcli_build_tpcc_num_vu': 4,
                'hammerdbcli_num_warehouse': 5,
                'hammerdbcli_all_warehouse': False,
                'hammerdbcli_tpcc_time_profile': False,
                'hammerdbcli_rampup': 5,
                'hammerdbcli_duration': 10,
                'hammerdbcli_num_run': 1,
                'hammerdbcli_tpcc_log_transactions': False,
                'run_iteration': 1
            },
            timestamp=0),
        sample.Sample(
            metric='NOPM',
            value=8153.0,
            unit='NOPM',
            metadata={
                'hammerdbcli_version': '4.0',
                'hammerdbcli_script': 'tpc_c',
                'hammerdbcli_vu': 4,
                'hammerdbcli_build_tpcc_num_vu': 4,
                'hammerdbcli_num_warehouse': 5,
                'hammerdbcli_all_warehouse': False,
                'hammerdbcli_tpcc_time_profile': False,
                'hammerdbcli_rampup': 5,
                'hammerdbcli_duration': 10,
                'hammerdbcli_num_run': 1,
                'hammerdbcli_tpcc_log_transactions': False,
                'run_iteration': 1
            },
            timestamp=0)
    ]
    self.assertCountEqual(output, expected)


class HammerdbcliTest(googletest.TestCase):

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTpcCTPMResultsFromFile(self):
    """Tests output from TPCC time profile."""
    FLAGS.hammerdbcli_tpcc_time_profile = True
    FLAGS.hammerdbcli_tpcc_rampup = 0
    vm = mock.Mock()
    vm.RemoteCommand = mock.Mock(return_value=(TPM_PER_SECOND_4_3_LOG, ''))
    output = ParseTpcCTPMResultsFromFile(vm)
    self.assertEqual(output, [
        sample.Sample(
            metric='TPM_time_series',
            value=0.0,
            unit='TPM',
            metadata={
                'values': [
                    0.0, 324540.0, 324300.0, 316260.0, 331020.0, 308700.0,
                    305280.0, 303300.0, 328500.0, 312180.0, 453480.0, 322620.0,
                    317760.0, 312060.0, 320820.0, 321420.0, 326520.0, 338940.0,
                    315780.0, 325560.0, 320400.0, 325860.0, 340320.0, 326160.0,
                    328140.0, 331200.0, 323460.0, 334140.0, 312660.0, 325980.0,
                    325440.0, 330360.0, 335460.0, 314160.0, 325440.0, 326760.0,
                    319320.0, 328740.0, 320700.0, 323340.0, 331200.0, 329700.0,
                    320640.0, 349560.0, 331980.0, 322320.0, 337680.0, 319680.0,
                    319680.0, 322920.0, 326760.0, 330600.0, 317460.0, 324300.0,
                    319500.0, 332340.0, 340980.0, 388500.0, 345120.0, 322980.0,
                    366180.0, 318660.0, 324540.0, 329820.0, 349020.0, 324360.0,
                    327180.0, 319800.0, 320160.0, 339960.0, 316680.0, 325140.0,
                    327480.0, 323640.0, 343920.0, 323460.0, 320880.0, 323400.0,
                    327540.0, 327420.0, 327540.0, 327240.0, 316020.0, 332460.0,
                    328320.0, 320880.0, 318840.0, 320160.0, 336360.0, 305880.0,
                    331800.0, 348780.0, 333000.0, 330960.0, 325440.0, 324240.0,
                    326160.0, 333540.0, 325020.0
                ],
                'timestamps': [
                    1656657820000.0, 1656657821000.0, 1656657822000.0,
                    1656657823000.0, 1656657824000.0, 1656657825000.0,
                    1656657826000.0, 1656657827000.0, 1656657828000.0,
                    1656657829000.0, 1656657830000.0, 1656657831000.0,
                    1656657832000.0, 1656657833000.0, 1656657834000.0,
                    1656657835000.0, 1656657836000.0, 1656657837000.0,
                    1656657838000.0, 1656657839000.0, 1656657840000.0,
                    1656657841000.0, 1656657842000.0, 1656657843000.0,
                    1656657844000.0, 1656657845000.0, 1656657846000.0,
                    1656657847000.0, 1656657848000.0, 1656657849000.0,
                    1656657850000.0, 1656657851000.0, 1656657852000.0,
                    1656657853000.0, 1656657854000.0, 1656657855000.0,
                    1656657856000.0, 1656657857000.0, 1656657858000.0,
                    1656657859000.0, 1656657860000.0, 1656657861000.0,
                    1656657862000.0, 1656657863000.0, 1656657864000.0,
                    1656657865000.0, 1656657866000.0, 1656657867000.0,
                    1656657868000.0, 1656657869000.0, 1656657870000.0,
                    1656657871000.0, 1656657872000.0, 1656657873000.0,
                    1656657874000.0, 1656657875000.0, 1656657876000.0,
                    1656657877000.0, 1656657878000.0, 1656657879000.0,
                    1656657880000.0, 1656657881000.0, 1656657882000.0,
                    1656657883000.0, 1656657884000.0, 1656657885000.0,
                    1656657886000.0, 1656657887000.0, 1656657888000.0,
                    1656657889000.0, 1656657890000.0, 1656657891000.0,
                    1656657892000.0, 1656657893000.0, 1656657894000.0,
                    1656657895000.0, 1656657896000.0, 1656657897000.0,
                    1656657898000.0, 1656657899000.0, 1656657900000.0,
                    1656657901000.0, 1656657902000.0, 1656657903000.0,
                    1656657904000.0, 1656657905000.0, 1656657906000.0,
                    1656657907000.0, 1656657908000.0, 1656657909000.0,
                    1656657910000.0, 1656657911000.0, 1656657912000.0,
                    1656657914000.0, 1656657915000.0, 1656657916000.0,
                    1656657917000.0, 1656657918000.0, 1656657919000.0
                ],
                'interval': 1,
                'ramp_up_ends': 1656657820000.0,
                'ramp_down_starts': 1656658420000.0
            },
            timestamp=0)
    ])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTpcCTPMResultsFromFileWithRampUp(self):
    """Tests output from TPCC time profile."""
    FLAGS.hammerdbcli_tpcc_time_profile = True
    FLAGS.hammerdbcli_tpcc_rampup = 1
    vm = mock.Mock()
    vm.RemoteCommand = mock.Mock(return_value=(TPM_PER_SECOND_4_3_LOG, ''))
    output = ParseTpcCTPMResultsFromFile(vm)
    self.assertEqual(output, [
        sample.Sample(
            metric='TPM_time_series',
            value=0.0,
            unit='TPM',
            metadata={
                'values': [
                    0.0, 324540.0, 324300.0, 316260.0, 331020.0, 308700.0,
                    305280.0, 303300.0, 328500.0, 312180.0, 453480.0, 322620.0,
                    317760.0, 312060.0, 320820.0, 321420.0, 326520.0, 338940.0,
                    315780.0, 325560.0, 320400.0, 325860.0, 340320.0, 326160.0,
                    328140.0, 331200.0, 323460.0, 334140.0, 312660.0, 325980.0,
                    325440.0, 330360.0, 335460.0, 314160.0, 325440.0, 326760.0,
                    319320.0, 328740.0, 320700.0, 323340.0, 331200.0, 329700.0,
                    320640.0, 349560.0, 331980.0, 322320.0, 337680.0, 319680.0,
                    319680.0, 322920.0, 326760.0, 330600.0, 317460.0, 324300.0,
                    319500.0, 332340.0, 340980.0, 388500.0, 345120.0, 322980.0,
                    366180.0, 318660.0, 324540.0, 329820.0, 349020.0, 324360.0,
                    327180.0, 319800.0, 320160.0, 339960.0, 316680.0, 325140.0,
                    327480.0, 323640.0, 343920.0, 323460.0, 320880.0, 323400.0,
                    327540.0, 327420.0, 327540.0, 327240.0, 316020.0, 332460.0,
                    328320.0, 320880.0, 318840.0, 320160.0, 336360.0, 305880.0,
                    331800.0, 348780.0, 333000.0, 330960.0, 325440.0, 324240.0,
                    326160.0, 333540.0, 325020.0
                ],
                'timestamps': [
                    1656657820000.0, 1656657821000.0, 1656657822000.0,
                    1656657823000.0, 1656657824000.0, 1656657825000.0,
                    1656657826000.0, 1656657827000.0, 1656657828000.0,
                    1656657829000.0, 1656657830000.0, 1656657831000.0,
                    1656657832000.0, 1656657833000.0, 1656657834000.0,
                    1656657835000.0, 1656657836000.0, 1656657837000.0,
                    1656657838000.0, 1656657839000.0, 1656657840000.0,
                    1656657841000.0, 1656657842000.0, 1656657843000.0,
                    1656657844000.0, 1656657845000.0, 1656657846000.0,
                    1656657847000.0, 1656657848000.0, 1656657849000.0,
                    1656657850000.0, 1656657851000.0, 1656657852000.0,
                    1656657853000.0, 1656657854000.0, 1656657855000.0,
                    1656657856000.0, 1656657857000.0, 1656657858000.0,
                    1656657859000.0, 1656657860000.0, 1656657861000.0,
                    1656657862000.0, 1656657863000.0, 1656657864000.0,
                    1656657865000.0, 1656657866000.0, 1656657867000.0,
                    1656657868000.0, 1656657869000.0, 1656657870000.0,
                    1656657871000.0, 1656657872000.0, 1656657873000.0,
                    1656657874000.0, 1656657875000.0, 1656657876000.0,
                    1656657877000.0, 1656657878000.0, 1656657879000.0,
                    1656657880000.0, 1656657881000.0, 1656657882000.0,
                    1656657883000.0, 1656657884000.0, 1656657885000.0,
                    1656657886000.0, 1656657887000.0, 1656657888000.0,
                    1656657889000.0, 1656657890000.0, 1656657891000.0,
                    1656657892000.0, 1656657893000.0, 1656657894000.0,
                    1656657895000.0, 1656657896000.0, 1656657897000.0,
                    1656657898000.0, 1656657899000.0, 1656657900000.0,
                    1656657901000.0, 1656657902000.0, 1656657903000.0,
                    1656657904000.0, 1656657905000.0, 1656657906000.0,
                    1656657907000.0, 1656657908000.0, 1656657909000.0,
                    1656657910000.0, 1656657911000.0, 1656657912000.0,
                    1656657914000.0, 1656657915000.0, 1656657916000.0,
                    1656657917000.0, 1656657918000.0, 1656657919000.0
                ],
                'interval': 1,
                'ramp_up_ends': 1656657880000.0,
                'ramp_down_starts': 1656658480000.0
            },
            timestamp=0)
    ])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTPCCWithTimeProfile(self):
    """Tests output from TPCC time profile."""
    FLAGS.hammerdbcli_tpcc_time_profile = True
    output = ParseTpcCResults(TPCC_TIMEPROFILE_LOG, None)
    expected_result = [
        ('TPM', 17145, 'TPM'), ('NOPM', 5614, 'NOPM'),
        ('neword_MIN', 7.252800000000001, 'milliseconds'),
        ('neword_P50', 13.401272222222222, 'milliseconds'),
        ('neword_P95', 18.249016666666666, 'milliseconds'),
        ('neword_P99', 21.498166666666666, 'milliseconds'),
        ('neword_MAX', 25.013733333333334, 'milliseconds'),
        ('delivery_MIN', 86.24158426966292, 'milliseconds'),
        ('delivery_P50', 96.0373820224719, 'milliseconds'),
        ('delivery_P95', 109.40153932584269, 'milliseconds'),
        ('delivery_P99', 110.51451685393258, 'milliseconds'),
        ('delivery_MAX', 110.51451685393258, 'milliseconds'),
        ('payment_MIN', 8.258455555555557, 'milliseconds'),
        ('payment_P50', 11.342472222222222, 'milliseconds'),
        ('payment_P95', 15.634505555555556, 'milliseconds'),
        ('payment_P99', 17.918, 'milliseconds'),
        ('payment_MAX', 21.4622, 'milliseconds'),
        ('ostat_MIN', 2.295011111111111, 'milliseconds'),
        ('ostat_P50', 3.039427777777778, 'milliseconds'),
        ('ostat_P95', 4.759111111111111, 'milliseconds'),
        ('ostat_P99', 4.949144444444444, 'milliseconds'),
        ('ostat_MAX', 4.949144444444444, 'milliseconds'),
        ('slev_MIN', 2.014932584269663, 'milliseconds'),
        ('slev_P50', 2.4485280898876405, 'milliseconds'),
        ('slev_P95', 3.8453932584269666, 'milliseconds'),
        ('slev_P99', 4.0170112359550565, 'milliseconds'),
        ('slev_MAX', 4.0170112359550565, 'milliseconds'),
        ('gettimestamp_MIN', 0.0032444444444444443, 'milliseconds'),
        ('gettimestamp_P50', 0.004, 'milliseconds'),
        ('gettimestamp_P95', 0.005155555555555556, 'milliseconds'),
        ('gettimestamp_P99', 0.009266666666666668, 'milliseconds'),
        ('gettimestamp_MAX', 0.01857777777777778, 'milliseconds')
    ]

    self.assertCountEqual(
        output, [sample.Sample(i[0], i[1], i[2]) for i in expected_result])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTpcCTimeProfileResultsFromFile(self):
    """Tests output from TPCC time profile."""
    FLAGS.hammerdbcli_tpcc_time_profile = True
    vm = mock.Mock()
    vm.RemoteCommand = mock.Mock(return_value=(TPCC_TIMEPROFILE_4_3_LOG, ''))
    output = ParseTpcCTimeProfileResultsFromFile(vm)
    expected_result = [
        sample.Sample(
            metric='neword_CALLS',
            value=322104.0,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='neword_MIN',
            value=2.269,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='neword_MAX',
            value=36.366,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='neword_P99',
            value=14.197,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='neword_P95',
            value=12.399,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='neword_P50',
            value=9.087,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_CALLS',
            value=321305.0,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_MIN',
            value=3.038,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_MAX',
            value=26.534,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_P99',
            value=10.944,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_P95',
            value=9.492,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='payment_P50',
            value=6.818,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_CALLS',
            value=31967.0,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_MIN',
            value=31.27,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_MAX',
            value=83.076,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_P99',
            value=68.64,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_P95',
            value=65.194,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='delivery_P50',
            value=57.664,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_CALLS',
            value=32440.0,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_MIN',
            value=1.349,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_MAX',
            value=16.625,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_P99',
            value=5.53,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_P95',
            value=4.04,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='slev_P50',
            value=2.305,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_CALLS',
            value=32044.0,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_MIN',
            value=1.095,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_MAX',
            value=17.16,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_P99',
            value=5.246,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_P95',
            value=3.752,
            unit='milliseconds',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='ostat_P50',
            value=2.033,
            unit='milliseconds',
            metadata={},
            timestamp=0)
    ]

    self.assertCountEqual(
        output, [sample.Sample(i[0], i[1], i[2]) for i in expected_result])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTPCCWithoutTimeProfile(self):
    """Tests parsing metadata from the ANSYS benchmark output."""
    FLAGS.hammerdbcli_tpcc_time_profile = False
    output = ParseTpcCResults(TPCC_LOG, None)
    self.assertCountEqual(output, [
        sample.Sample('TPM', 24772, 'TPM'),
        sample.Sample('NOPM', 8153, 'NOPM')
    ])

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseTPCH(self):
    """Tests parsing metadata from the ANSYS benchmark output."""
    output = ParseTpcHResults(TPCH_LOG)
    expected_result = [('Query_14', 0.795, 'seconds'),
                       ('Query_2', 0.188, 'seconds'),
                       ('Query_9', 5.554, 'seconds'),
                       ('Query_20', 1.381, 'seconds'),
                       ('Query_6', 3.631, 'seconds'),
                       ('Query_17', 0.527, 'seconds'),
                       ('Query_18', 2.804, 'seconds'),
                       ('Query_8', 0.932, 'seconds'),
                       ('Query_21', 2.218, 'seconds'),
                       ('Query_13', 4.656, 'seconds'),
                       ('Query_3', 2.556, 'seconds'),
                       ('Query_22', 0.152, 'seconds'),
                       ('Query_16', 0.478, 'seconds'),
                       ('Query_4', 1.538, 'seconds'),
                       ('Query_11', 0.429, 'seconds'),
                       ('Query_15', 4.128, 'seconds'),
                       ('Query_1', 17.681, 'seconds'),
                       ('Query_10', 1.661, 'seconds'),
                       ('Query_19', 0.19, 'seconds'),
                       ('Query_5', 1.613, 'seconds'),
                       ('Query_7', 2.194, 'seconds'),
                       ('Query_12', 2.803, 'seconds'),
                       ('query_times_geomean', 1.40067754041747, 'seconds')]
    self.assertCountEqual(
        output, [sample.Sample(i[0], i[1], i[2]) for i in expected_result])
'''

if __name__ == '__main__':
  unittest.main()
