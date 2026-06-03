# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for agent_sandbox_metrics."""

import unittest

from perfkitbenchmarker.linux_packages import agent_sandbox_loadgen
from perfkitbenchmarker.linux_packages import agent_sandbox_metrics
from tests import pkb_common_test_case


def _record(
    name,
    requested_at,
    ready_at=None,
    error=None,
    warm_served=None,
    exec_started_at=None,
    exec_completed_at=None,
    released_at=None,
):
  rec = agent_sandbox_loadgen.ClaimRecord(name=name, requested_at=requested_at)
  rec.ready_at = ready_at
  rec.error = error
  rec.warm_served = warm_served
  rec.exec_started_at = exec_started_at
  rec.exec_completed_at = exec_completed_at
  rec.released_at = released_at
  return rec


class PercentileTest(pkb_common_test_case.PkbCommonTestCase):

  def testPercentileInterpolates(self):
    self.assertAlmostEqual(
        agent_sandbox_metrics.percentile([1, 2, 3, 4], 50), 2.5
    )

  def testPercentileEmptyIsZero(self):
    self.assertEqual(agent_sandbox_metrics.percentile([], 99), 0.0)


class BuildSamplesTest(pkb_common_test_case.PkbCommonTestCase):

  def testBuildSamplesEmitsLatencyAndCounts(self):
    records = [
        _record('claim-0', 0.0, ready_at=2.0, warm_served=True),
        _record('claim-1', 1.0, ready_at=5.0, warm_served=False),
        _record('claim-2', 2.0, error='TimeoutError'),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={'target_qps': 10}
    )
    by_metric = {s.metric: s for s in samples}

    self.assertIn('startup_time_p50', by_metric)
    self.assertEqual(by_metric['startup_time_p50'].unit, 'seconds')
    self.assertEqual(by_metric['success_count'].value, 2)
    self.assertEqual(by_metric['error_count'].value, 1)
    self.assertEqual(by_metric['peak_concurrency'].value, 2)
    self.assertAlmostEqual(by_metric['warm_served_fraction'].value, 0.5)
    self.assertEqual(by_metric['startup_time_p50'].metadata['target_qps'], 10)
    self.assertAlmostEqual(by_metric['startup_time_p50'].value, 3.0)
    self.assertAlmostEqual(by_metric['startup_time_max'].value, 4.0)
    self.assertAlmostEqual(by_metric['submit_qps'].value, 1.5)
    self.assertAlmostEqual(by_metric['completion_qps'].value, 0.4)
    self.assertIn('error_count_TimeoutError', by_metric)
    self.assertEqual(by_metric['error_count_TimeoutError'].value, 1)


class PeakFromRecordsTest(pkb_common_test_case.PkbCommonTestCase):

  def testPeakUsesReadyAtWhenNoReleasedAt(self):
    """Without released_at, peak interval closes at ready_at (point event)."""
    records = [
        _record('c0', 0.0, ready_at=1.0),
        _record('c1', 0.5, ready_at=1.5),
    ]
    # Both intervals close immediately at ready_at: no overlap after closing.
    peak = agent_sandbox_metrics._peak_from_records(records)
    self.assertGreaterEqual(peak, 1)

  def testPeakUsesReleasedAtWhenPresent(self):
    """With released_at, sandboxes stay alive until release.

    Two claims: c0 ready at t=1, released at t=10; c1 ready at t=2, released
    at t=11. Both alive from t=2..t=10, so peak should be 2.
    """
    records = [
        _record('c0', 0.0, ready_at=1.0, released_at=10.0),
        _record('c1', 0.5, ready_at=2.0, released_at=11.0),
    ]
    peak = agent_sandbox_metrics._peak_from_records(records)
    self.assertEqual(peak, 2)

  def testPeakSkipsRecordsWithNoReadyAt(self):
    """Claims that never became ready (error/timeout) are excluded."""
    records = [
        _record('c0', 0.0, ready_at=1.0, released_at=5.0),
        _record('c1', 0.0, error='Timeout'),
    ]
    peak = agent_sandbox_metrics._peak_from_records(records)
    self.assertEqual(peak, 1)

  def testPeakEmptyRecordsIsZero(self):
    self.assertEqual(agent_sandbox_metrics._peak_from_records([]), 0)


class BuildSamplesWorkloadMetricsTest(pkb_common_test_case.PkbCommonTestCase):

  def testExecDurationSamplesEmittedWhenPresent(self):
    """exec_duration_s_p50 .. _max emitted when records have exec timings."""
    records = [
        _record(
            'c0',
            0.0,
            ready_at=1.0,
            exec_started_at=1.0,
            exec_completed_at=6.0,
            released_at=6.1,
        ),
        _record(
            'c1',
            0.5,
            ready_at=1.5,
            exec_started_at=1.5,
            exec_completed_at=7.5,
            released_at=7.6,
        ),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_metric = {s.metric: s for s in samples}
    self.assertIn('exec_duration_s_p50', by_metric)
    self.assertIn('exec_duration_s_p90', by_metric)
    self.assertIn('exec_duration_s_p95', by_metric)
    self.assertIn('exec_duration_s_p99', by_metric)
    self.assertIn('exec_duration_s_max', by_metric)
    self.assertEqual(by_metric['exec_duration_s_p50'].unit, 'seconds')
    # c0: 5.0s, c1: 6.0s -> p50 interpolated = 5.5
    self.assertAlmostEqual(by_metric['exec_duration_s_p50'].value, 5.5)
    self.assertAlmostEqual(by_metric['exec_duration_s_max'].value, 6.0)

  def testTotalLifecycleSamplesEmittedWhenPresent(self):
    """total_lifecycle_s_p50 .. _max emitted when records have released_at."""
    records = [
        _record('c0', 0.0, ready_at=1.0, released_at=10.0),
        _record('c1', 1.0, ready_at=2.0, released_at=14.0),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_metric = {s.metric: s for s in samples}
    self.assertIn('total_lifecycle_s_p50', by_metric)
    self.assertIn('total_lifecycle_s_max', by_metric)
    # c0: 10.0s, c1: 13.0s -> p50 = 11.5
    self.assertAlmostEqual(by_metric['total_lifecycle_s_p50'].value, 11.5)
    self.assertAlmostEqual(by_metric['total_lifecycle_s_max'].value, 13.0)

  def testWorkloadMetricsAbsentWhenNoTimings(self):
    """When records have no exec/lifecycle timings, those metrics are absent."""
    records = [
        _record('c0', 0.0, ready_at=1.0),
        _record('c1', 1.0, ready_at=2.0),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_metric = {s.metric: s for s in samples}
    self.assertNotIn('exec_duration_s_p50', by_metric)
    self.assertNotIn('total_lifecycle_s_p50', by_metric)


if __name__ == '__main__':
  unittest.main()
