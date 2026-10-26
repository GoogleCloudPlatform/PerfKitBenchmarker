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
"""Tests for the agent_sandbox metrics."""

import unittest

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.kubernetes import agent_sandbox_metrics
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
  return agent_sandbox_metrics.ClaimRecord(
      name=name,
      requested_at=requested_at,
      ready_at=ready_at,
      error=error,
      warm_served=warm_served,
      exec_started_at=exec_started_at,
      exec_completed_at=exec_completed_at,
      released_at=released_at,
  )


class PercentileTest(pkb_common_test_case.PkbCommonTestCase):

  def testSummarizeMetric(self):
    samples = agent_sandbox_metrics._summarize_metric(
        'latency_', [1.0, 2.0, 3.0, 4.0], {}
    )
    by_name = {s.metric: s.value for s in samples}
    self.assertIn('latency_p50', by_name)
    self.assertIn('latency_p90', by_name)
    self.assertEqual(by_name['latency_max'], 4.0)

  def testSummarizeMetricEmptyReturnsZero(self):
    samples = agent_sandbox_metrics._summarize_metric('latency_', [], {})
    by_name = {s.metric: s.value for s in samples}
    self.assertEqual(by_name['latency_p50'], 0.0)
    self.assertEqual(by_name['latency_max'], 0.0)


class BuildSamplesTest(pkb_common_test_case.PkbCommonTestCase):

  def _two_success_records(self):
    return [
        _record('c0', requested_at=100.0, ready_at=101.0, warm_served=False),
        _record('c1', requested_at=100.0, ready_at=103.0, warm_served=True),
    ]

  def testStartupPercentileMetricsPresent(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={'target_qps': 10.0}
    )
    by_name = {s.metric: s for s in samples}
    for pct in (50, 90, 95, 99):
      self.assertIn(f'startup_time_p{pct}', by_name)
    self.assertIn('startup_time_max', by_name)

  def testStartupTimeValues(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    # startup times are 1.0 and 3.0; SummarizePercentiles p50 -> 2.0
    self.assertAlmostEqual(by_name['startup_time_p50'].value, 2.0)
    self.assertAlmostEqual(by_name['startup_time_max'].value, 3.0)

  def testStartupTimeUnits(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=1, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertEqual(by_name['startup_time_p50'].unit, 'seconds')
    self.assertEqual(by_name['startup_time_max'].unit, 'seconds')

  def testCountsAndConcurrency(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertEqual(by_name['peak_concurrency'].value, 2)
    self.assertEqual(by_name['success_count'].value, 2)
    self.assertEqual(by_name['error_count'].value, 0)

  def testErrorCount(self):
    records = [
        _record('c0', requested_at=100.0, ready_at=101.0, warm_served=False),
        _record('c1', requested_at=100.0, ready_at=101.0, warm_served=False),
        _record('c2', requested_at=100.0, ready_at=101.0, warm_served=False),
        _record('c3', requested_at=100.0, ready_at=None, error='Timeout'),
        _record('c4', requested_at=100.0, ready_at=None, error='Timeout'),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=1, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertEqual(by_name['error_count'].value, 2)
    self.assertEqual(by_name['success_count'].value, 3)
    self.assertIn('error_count_Timeout', by_name)
    self.assertEqual(by_name['error_count_Timeout'].value, 2)

  def testHighFailureRateRaisesRunError(self):
    records = [
        _record('c0', requested_at=100.0, ready_at=101.0, warm_served=False),
        _record('c1', requested_at=100.0, ready_at=None, error='Timeout'),
    ]
    with self.assertRaises(errors.Benchmarks.RunError):
      agent_sandbox_metrics.build_samples(
          records, peak_concurrency=1, metadata={}
      )

  def testAllErrorsRaisesRunError(self):
    records = [
        _record('c0', requested_at=100.0, ready_at=None, error='Failed'),
        _record('c1', requested_at=100.0, ready_at=None, error='Failed'),
    ]
    with self.assertRaises(errors.Benchmarks.RunError):
      agent_sandbox_metrics.build_samples(
          records, peak_concurrency=1, metadata={}
      )

  def testMetadataAttachedToAllSamples(self):
    records = self._two_success_records()
    meta = {'target_qps': 10.0, 'run_id': 'abc'}
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata=meta
    )
    for s in samples:
      self.assertEqual(s.metadata.get('target_qps'), 10.0)

  def testWarmServedFraction(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('warm_served_fraction', by_name)
    self.assertAlmostEqual(by_name['warm_served_fraction'].value, 0.5)
    self.assertEqual(by_name['warm_served_fraction'].unit, 'fraction')

  def testQpsMetricsPresent(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('submit_qps', by_name)
    self.assertIn('completion_qps', by_name)
    self.assertEqual(by_name['submit_qps'].unit, 'count/sec')
    self.assertEqual(by_name['completion_qps'].unit, 'count/sec')

  def testMultiPodCrossNodeMonotonicClockNormalization(self):
    records = [
        _record(
            'claim-pod1-0',
            requested_at=1000.0,
            ready_at=1001.0,
            warm_served=True,
        ),
        _record(
            'claim-pod1-1',
            requested_at=1010.0,
            ready_at=1012.0,
            warm_served=True,
        ),
        _record(
            'claim-pod2-0',
            requested_at=50000.0,
            ready_at=50001.5,
            warm_served=True,
        ),
        _record(
            'claim-pod2-1',
            requested_at=50010.0,
            ready_at=50013.0,
            warm_served=True,
        ),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=4, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertAlmostEqual(by_name['submit_qps'].value, 4.0 / 10.0)
    self.assertAlmostEqual(by_name['completion_qps'].value, 4.0 / 13.0)

  def testExecDurationAbsentWhenNotSet(self):
    # Records have no exec_started_at / exec_completed_at -> no exec metrics
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    for pct in (50, 90, 95, 99):
      self.assertNotIn(f'exec_duration_s_p{pct}', by_name)
    self.assertNotIn('exec_duration_s_max', by_name)

  def testTotalLifecycleAbsentWhenNotSet(self):
    # Records have no released_at -> no lifecycle metrics
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    for pct in (50, 90, 95, 99):
      self.assertNotIn(f'total_lifecycle_s_p{pct}', by_name)
    self.assertNotIn('total_lifecycle_s_max', by_name)

  def testExecDurationPresentWhenSet(self):
    records = [
        _record(
            'c0',
            requested_at=100.0,
            ready_at=101.0,
            warm_served=False,
            exec_started_at=102.0,
            exec_completed_at=104.0,
        ),
        _record(
            'c1',
            requested_at=100.0,
            ready_at=103.0,
            warm_served=True,
            exec_started_at=104.0,
            exec_completed_at=107.0,
        ),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('exec_duration_s_p50', by_name)
    self.assertIn('exec_duration_s_max', by_name)
    # durations: 2.0 and 3.0; SummarizePercentiles p50 -> 2.5, max = 3.0
    self.assertAlmostEqual(by_name['exec_duration_s_p50'].value, 2.5)
    self.assertAlmostEqual(by_name['exec_duration_s_max'].value, 3.0)
    self.assertEqual(by_name['exec_duration_s_p50'].unit, 'seconds')

  def testDeleteDurationPresentWhenSet(self):
    records = [
        _record(
            'c0',
            requested_at=100.0,
            ready_at=101.0,
            warm_served=True,
            exec_started_at=101.0,
            exec_completed_at=121.0,
            released_at=122.0,
        ),
        _record(
            'c1',
            requested_at=100.0,
            ready_at=102.0,
            warm_served=True,
            exec_started_at=102.0,
            exec_completed_at=122.0,
            released_at=125.0,
        ),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('delete_duration_s_p50', by_name)
    self.assertIn('delete_duration_s_max', by_name)
    self.assertAlmostEqual(by_name['delete_duration_s_p50'].value, 2.0)
    self.assertAlmostEqual(by_name['delete_duration_s_max'].value, 3.0)

  def testTotalLifecyclePresentWhenSet(self):
    records = [
        _record(
            'c0',
            requested_at=100.0,
            ready_at=101.0,
            warm_served=False,
            released_at=110.0,
        ),
        _record(
            'c1',
            requested_at=100.0,
            ready_at=103.0,
            warm_served=True,
            released_at=115.0,
        ),
    ]
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('total_lifecycle_s_p50', by_name)
    self.assertIn('total_lifecycle_s_max', by_name)
    # lifecycles are 10.0 and 15.0; SummarizePercentiles p50 -> 12.5
    self.assertAlmostEqual(by_name['total_lifecycle_s_p50'].value, 12.5)
    self.assertAlmostEqual(by_name['total_lifecycle_s_max'].value, 15.0)
    self.assertEqual(by_name['total_lifecycle_s_p50'].unit, 'seconds')

  def testWarmAndColdStartupSplitMetricsPresent(self):
    records = self._two_success_records()
    samples = agent_sandbox_metrics.build_samples(
        records, peak_concurrency=2, metadata={}
    )
    by_name = {s.metric: s for s in samples}
    self.assertIn('warm_startup_time_p50', by_name)
    self.assertIn('cold_startup_time_p50', by_name)
    self.assertAlmostEqual(by_name['cold_startup_time_p50'].value, 1.0)
    self.assertAlmostEqual(by_name['warm_startup_time_p50'].value, 3.0)

  def testParseControllerPrometheusMetrics(self):
    prom_text = (
        '# HELP workqueue_depth Current depth of workqueue\n# TYPE'
        ' workqueue_depth gauge\nworkqueue_depth{name="sandboxclaim"}'
        ' 4\nagent_sandbox_warmpool_size{warmpool_name="agent-sandbox",sandbox_status="ready"}'
        ' 150\nuntracked_metric 99\n'
    )
    samples = agent_sandbox_metrics.parse_controller_prometheus_metrics(
        prom_text, {'target_qps': 10.0}
    )
    self.assertLen(samples, 2)
    by_metric = {s.metric: s for s in samples}
    self.assertEqual(by_metric['controller_workqueue_depth'].value, 4.0)
    self.assertEqual(
        by_metric['controller_agent_sandbox_warmpool_size'].value, 150.0
    )
    self.assertEqual(
        by_metric['controller_workqueue_depth'].metadata['prom_labels'],
        'name="sandboxclaim"',
    )


if __name__ == '__main__':
  unittest.main()
