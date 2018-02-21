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


class MockSpec(object):
  """A mock BenchmarkSpec class.

  We need to use this rather than a mock.MagicMock object because
  the "name" attribute of MagicMocks is difficult to set.
  """

  def __init__(self, name, uid, status, failed_substatus=None):
    self.name = name
    self.uid = uid
    self.status = status
    self.failed_substatus = failed_substatus


_BENCHMARK_SPECS = [
    MockSpec('iperf', 'iperf0', benchmark_status.SUCCEEDED),
    MockSpec('iperf', 'iperf1', benchmark_status.FAILED),
    MockSpec('iperf', 'iperf2', benchmark_status.FAILED,
             benchmark_status.FailedSubstatus.QUOTA),
    MockSpec('cluster_boot', 'cluster_boot0', benchmark_status.SKIPPED)
]
_STATUS_TABLE = os.linesep.join((
    '--------------------------------------------------------',
    'Name          UID            Status     Failed Substatus',
    '--------------------------------------------------------',
    'iperf         iperf0         SUCCEEDED                  ',
    'iperf         iperf1         FAILED                     ',
    'iperf         iperf2         FAILED     QUOTA_EXCEEDED  ',
    'cluster_boot  cluster_boot0  SKIPPED                    ',
    '--------------------------------------------------------'))
_STATUS_SUMMARY = os.linesep.join((
    'Benchmark run statuses:',
    '--------------------------------------------------------',
    'Name          UID            Status     Failed Substatus',
    '--------------------------------------------------------',
    'iperf         iperf0         SUCCEEDED                  ',
    'iperf         iperf1         FAILED                     ',
    'iperf         iperf2         FAILED     QUOTA_EXCEEDED  ',
    'cluster_boot  cluster_boot0  SKIPPED                    ',
    '--------------------------------------------------------',
    'Success rate: 25.00% (1/4)'))


class CreateSummaryTableTestCase(unittest.TestCase):

  def testCreateSummaryTable(self):
    result = benchmark_status._CreateSummaryTable(_BENCHMARK_SPECS)
    self.assertEqual(result, _STATUS_TABLE)


class CreateSummaryTestCase(unittest.TestCase):

  def testCreateSummary(self):
    result = benchmark_status.CreateSummary(_BENCHMARK_SPECS)
    self.assertEqual(result, _STATUS_SUMMARY)
