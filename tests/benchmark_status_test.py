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
"""Tests for perfkitbenchmarker.benchmark_status."""

import os
import unittest

from perfkitbenchmarker import benchmark_status


_STATUS_TUPLES = [('iperf', 'iperf0', benchmark_status.SUCCEEDED),
                  ('iperf', 'iperf1', benchmark_status.FAILED),
                  ('cluster_boot', 'cluster_boot0', benchmark_status.SKIPPED)]
_STATUS_TABLE = os.linesep.join((
    '--------------------------------------',
    'Name          UID            Status   ',
    '--------------------------------------',
    'iperf         iperf0         SUCCEEDED',
    'iperf         iperf1         FAILED   ',
    'cluster_boot  cluster_boot0  SKIPPED  ',
    '--------------------------------------'))
_STATUS_SUMMARY = os.linesep.join((
    'Benchmark run statuses:',
    '--------------------------------------',
    'Name          UID            Status   ',
    '--------------------------------------',
    'iperf         iperf0         SUCCEEDED',
    'iperf         iperf1         FAILED   ',
    'cluster_boot  cluster_boot0  SKIPPED  ',
    '--------------------------------------',
    'Success rate: 33.33% (1/3)'))


class CreateSummaryTableTestCase(unittest.TestCase):

  def testCreateSummaryTable(self):
    result = benchmark_status._CreateSummaryTable(_STATUS_TUPLES)
    self.assertEqual(result, _STATUS_TABLE)


class CreateSummaryTestCase(unittest.TestCase):

  def testCreateSummary(self):
    result = benchmark_status.CreateSummary(_STATUS_TUPLES)
    self.assertEqual(result, _STATUS_SUMMARY)
