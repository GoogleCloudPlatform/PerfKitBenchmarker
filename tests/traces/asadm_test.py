# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for otel trace utility."""
import os
import unittest

from absl import flags
from dateutil import parser
from perfkitbenchmarker import sample as pkb_sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.traces import asadm

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

TEST_VM = 'test_vm0'


class AsadmTestCase(unittest.TestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(AsadmTestCase, self).setUp()
    directory = os.path.join(os.path.dirname(__file__), '..', 'data')

    self.interval = 60
    self.collector = asadm._AsadmSummaryCollector(
        output_directory=directory, interval=self.interval
    )
    self.collector._role_mapping[TEST_VM] = 'asadm_output.txt'

  def testAsadmSummaryAnalyze(self):
    samples = []
    self.collector.Analyze('test_sender', None, samples)
    expected_timestamps = [
        pkb_sample.ConvertDateTimeToUnixMs(parser.parse('2023-10-18 21:35:30')),
        pkb_sample.ConvertDateTimeToUnixMs(parser.parse('2023-10-18 21:35:28')),
    ]
    expected_samples = [
        pkb_sample.Sample(
            metric=asadm.MEMORY_USED_METRIC,
            value=0.0,
            unit=asadm.DEFAULT_RESOURCE_SIZE_UNIT,
            metadata={
                'values': [53.644, 88.0],
                'interval': self.interval,
                'timestamps': expected_timestamps
            },
            timestamp=0
        ),
        pkb_sample.Sample(
            metric=asadm.MEMORY_USED_PERCENTAGES_METRIC,
            value=0.0,
            unit='%',
            metadata={
                'values': [4.82, 14.82],
                'interval': self.interval,
                'timestamps': expected_timestamps
            },
            timestamp=0
        ),
        pkb_sample.Sample(
            metric=asadm.DISK_USED_METRIC,
            value=0.0,
            unit=asadm.DEFAULT_RESOURCE_SIZE_UNIT,
            metadata={
                'values': [898.540, 1200.0],
                'interval': self.interval,
                'timestamps': expected_timestamps
            },
            timestamp=0
        ),
        pkb_sample.Sample(
            metric=asadm.DISK_USED_PERCENTAGES_METRIC,
            value=0.0,
            unit='%',
            metadata={
                'values': [3.66, 13.66],
                'interval': self.interval,
                'timestamps': expected_timestamps
            },
            timestamp=0
        ),
    ]
    self.assertSampleListsEqualUpToTimestamp(samples, expected_samples)


if __name__ == '__main__':
  unittest.main()
