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
"""Tests for S64da Benchmark."""

import os
import unittest
from unittest import mock

from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import s64da
from tests import pkb_common_test_case

TEST_DATA_DIR = 'tests/data'
OLAP_RESULTS = 'olap_result.txt'
OLTP_RESULTS = 'oltp_result.txt'


class S64DATest(pkb_common_test_case.PkbCommonTestCase):

  def S64daOutput(self, file: str) -> str:
    path = os.path.join(os.path.dirname(__file__), '..', 'data', file)
    with open(path) as reader:
      return reader.read()

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseOLAPResults(self):
    results = s64da.ParseOLAPResults(self.S64daOutput(OLAP_RESULTS))
    self.assertEqual([
        sample.Sample(
            metric='Query 2',
            value=1.0979999999999999,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 9', value=0.238, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 20', value=0.45, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 6', value=0.352, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 17', value=1.768, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 18', value=6.936, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 8',
            value=0.21400000000000002,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 21', value=2.16, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 13', value=0.78, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 3', value=0.51, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 22',
            value=0.44800000000000006,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 16',
            value=4.5920000000000005,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 4', value=0.144, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 11', value=0.606, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 15', value=0.7075, unit='s', metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 1', value=0.31, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 10', value=0.91, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 19', value=0.7825, unit='s', metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 5',
            value=1.3599999999999999,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 7', value=0.4875, unit='s', metadata={}, timestamp=0),
        sample.Sample(
            metric='Query 12', value=0.5675, unit='s', metadata={},
            timestamp=0),
        sample.Sample(
            metric='Query 14', value=0.3325, unit='s', metadata={},
            timestamp=0),
        sample.Sample(
            metric='query_times_geomean',
            value=0.6943741551291615,
            unit='s',
            metadata={},
            timestamp=0),
        sample.Sample(
            metric='TPCH_failure_rate',
            value=0.0,
            unit='%',
            metadata={},
            timestamp=0)
    ], results)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseOLTPResults(self):
    results = s64da.ParseOLTPResults(self.S64daOutput(OLTP_RESULTS), 0)
    self.assertEqual([
        sample.Sample(
            metric='TPM', value=15853.0, unit='TPM', metadata={}, timestamp=0),
        sample.Sample(
            metric='TPCC_failure_rate',
            value=0.4461056185194116,
            unit='%',
            metadata={},
            timestamp=0)
    ], results)

  @mock.patch('time.time', mock.MagicMock(return_value=0))
  def testParseOLTPWithRampUpResults(self):
    results = s64da.ParseOLTPResults(self.S64daOutput(OLTP_RESULTS), 1)
    self.assertEqual([
        sample.Sample(
            metric='TPM', value=15886.0, unit='TPM', metadata={}, timestamp=0),
        sample.Sample(
            metric='TPCC_failure_rate',
            value=0.4461056185194116,
            unit='%',
            metadata={},
            timestamp=0)
    ], results)


if __name__ == '__main__':
  unittest.main()
