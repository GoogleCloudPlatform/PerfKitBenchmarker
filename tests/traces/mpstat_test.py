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
"""Tests for mpstat utility."""
import datetime
import json
import os
import unittest

from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import sample
from perfkitbenchmarker.traces import mpstat

FAKE_DATETIME = datetime.datetime(2021, 5, 19)

MPSTAT_METADATA = {
    'event': 'mpstat',
    'sender': 'run',
}

_AGGREGATE_SAMPLES = [
    sample.Sample(
        metric='mpstat_avg_intr',
        value=274.26,
        unit='interrupts/sec',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': -1,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0),
    sample.Sample(
        metric='mpstat_avg_irq',
        value=0.0,
        unit='%',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': -1,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0),
    sample.Sample(
        metric='mpstat_avg_soft',
        value=0.01,
        unit='%',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': -1,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0),
    sample.Sample(
        metric='mpstat_avg_intr',
        value=264.5,
        unit='interrupts/sec',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': 0,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0),
    sample.Sample(
        metric='mpstat_avg_intr',
        value=14.375,
        unit='interrupts/sec',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': 1,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0)
]

# Besides verifying that per-interval samples are produced, the
# important thing in this test is making sure that the difference in
# timestamps of samples from different intervals is the interval time
# (60s)
_PER_INTERVAL_SAMPLES = [
    sample.Sample(
        metric='mpstat_avg_idle',
        value=49.98,
        unit='%',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': -1,
            'ordinal': 0,
            'nodename': 'instance-3'
        },
        timestamp=1621447265.0),
    sample.Sample(
        metric='mpstat_avg_idle',
        value=49.98,
        unit='%',
        metadata={
            'event': 'mpstat',
            'sender': 'run',
            'mpstat_cpu_id': -1,
            'ordinal': 1,
            'nodename': 'instance-3'
        },
        timestamp=1621447325.0)
]


@freezegun.freeze_time(FAKE_DATETIME)
class MpstatTestCase(parameterized.TestCase):

  def setUp(self):
    super(MpstatTestCase, self).setUp()
    # The example output was generated with the following commands on an
    # e2-standard-2:
    # stress -c 1 &
    # export S_TIME_FORMAT=ISO
    # mpstat -I ALL -u -P ALL 60 2 -o JSON
    path = os.path.join(
        os.path.dirname(__file__), '../data', 'mpstat_output.json')
    with open(path) as fp:
      self.contents = json.loads(fp.read())

  @parameterized.named_parameters(
      ('averages_only', False, None, 33, _AGGREGATE_SAMPLES),
      (
          'per_minute_samples',
          True,
          60,
          93,
          _AGGREGATE_SAMPLES + _PER_INTERVAL_SAMPLES,
      ))
  def testMpstatParse(self, per_interval_samples, interval,
                      expected_number_of_samples, expected_samples):
    actual_samples = mpstat._MpstatResults(
        MPSTAT_METADATA,
        self.contents,
        per_interval_samples=per_interval_samples,
        interval=interval)

    self.assertLen(actual_samples, expected_number_of_samples)

    for expected_sample in expected_samples:
      if expected_sample not in actual_samples:
        sample_not_found_message = (
            f'Expected sample:\n{expected_sample}\nnot found in actual samples:'
            f'\n{actual_samples}')
        raise Exception(sample_not_found_message)


if __name__ == '__main__':
  unittest.main()
