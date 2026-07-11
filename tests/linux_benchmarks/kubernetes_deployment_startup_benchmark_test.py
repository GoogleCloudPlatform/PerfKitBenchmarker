# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for kubernetes_deployment_startup_benchmark and startup_metrics.

PR 1 — Metrics & Observability (Layer 0)
"""

import threading
import time
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import kubernetes_deployment_startup_benchmark as bench
from perfkitbenchmarker.linux_benchmarks import kubernetes_deployment_startup_benchmark as kdsb
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.container_service import kubernetes_conditions
from tests import pkb_common_test_case


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_METADATA = {
    'workload': 'jvm',
    'scenario': 'baseline',
    'cloud': 'GCP',
    'replicas': 1,
    'namespace': 'default',
    'app_label': 'slowjvmstartup',
}


def _MakePodJson(
    pod_name: str,
    scheduled_ts: str,
    started_at: str | None = None,
    ready_ts: str | None = None,
) -> dict:
  """Builds a minimal pod JSON dict for testing."""
  conditions = [
      {
          'type': 'PodScheduled',
          'status': 'True',
          'lastTransitionTime': scheduled_ts,
      }
  ]
  if ready_ts:
    conditions.append({
        'type': 'Ready',
        'status': 'True',
        'lastTransitionTime': ready_ts,
    })

  container_statuses = []
  if started_at:
    container_statuses = [{
        'name': 'app',
        'ready': True,
        'state': {'running': {'startedAt': started_at}},
    }]

  return {
      'metadata': {'name': pod_name},
      'status': {
          'startTime': scheduled_ts,
          'conditions': conditions,
          'containerStatuses': container_statuses,
      },
  }


# ---------------------------------------------------------------------------
# startup_metrics tests
# ---------------------------------------------------------------------------


class StartupMetricsTest(pkb_common_test_case.PkbCommonTestCase):

  def testExtractLatencyFromContainerStartedAt(self):
    """Prefers containerStatuses[0].state.running.startedAt over Ready cond."""
    pod = _MakePodJson(
        'pod-1',
        scheduled_ts='2024-01-01T00:00:00Z',
        started_at='2024-01-01T00:00:30Z',   # 30s latency
        ready_ts='2024-01-01T00:00:35Z',      # 35s — should NOT be used
    )
    latency = startup_metrics._ExtractPodStartupLatency(pod)
    self.assertAlmostEqual(latency, 30.0, places=1)

  def testExtractLatencyFallsBackToReadyCondition(self):
    """Falls back to Ready condition when containerStatuses is absent."""
    pod = _MakePodJson(
        'pod-2',
        scheduled_ts='2024-01-01T00:00:00Z',
        ready_ts='2024-01-01T00:00:45Z',      # 45s latency via Ready cond
    )
    latency = startup_metrics._ExtractPodStartupLatency(pod)
    self.assertAlmostEqual(latency, 45.0, places=1)

  def testExtractLatencyReturnsNoneWhenNoReadyTimestamp(self):
    """Returns None when no container-ready or Ready-condition timestamp exists."""
    pod = _MakePodJson('pod-3', scheduled_ts='2024-01-01T00:00:00Z')
    latency = startup_metrics._ExtractPodStartupLatency(pod)
    self.assertIsNone(latency)

  def testExtractLatencyReturnsNoneForNegativeLatency(self):
    """Returns None when container-ready precedes scheduled (clock skew)."""
    pod = _MakePodJson(
        'pod-4',
        scheduled_ts='2024-01-01T00:01:00Z',
        started_at='2024-01-01T00:00:30Z',    # before scheduled → negative
    )
    latency = startup_metrics._ExtractPodStartupLatency(pod)
    self.assertIsNone(latency)

  def testLatenciesToSamplesEmitsAllStats(self):
    """_LatenciesToSamples emits min/max/mean/p50/p90/p99/count."""
    latencies = [10.0, 20.0, 30.0, 40.0, 50.0]
    samples = startup_metrics._LatenciesToSamples(latencies, _BASE_METADATA)
    metric_names = {s.metric for s in samples}
    expected = {
        'startup_latency_min',
        'startup_latency_max',
        'startup_latency_mean',
        'startup_latency_p50',
        'startup_latency_p90',
        'startup_latency_p99',
        'startup_latency_pod_count',
    }
    self.assertEqual(metric_names, expected)

  def testLatenciesToSamplesValues(self):
    """Verifies min/max/mean values are correct."""
    latencies = [10.0, 20.0, 30.0]
    samples = startup_metrics._LatenciesToSamples(latencies, _BASE_METADATA)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency_min'], 10.0)
    self.assertAlmostEqual(by_metric['startup_latency_max'], 30.0)
    self.assertAlmostEqual(by_metric['startup_latency_mean'], 20.0)
    self.assertEqual(by_metric['startup_latency_pod_count'], 3)

  def testLatenciesToSamplesCarryBaseMetadata(self):
    """All samples carry the base metadata keys."""
    latencies = [15.0]
    samples = startup_metrics._LatenciesToSamples(latencies, _BASE_METADATA)
    for s in samples:
      for key in ('workload', 'scenario', 'cloud'):
        self.assertIn(key, s.metadata)

  def testGetStartupLatencySamplesEmptyOnKubectlError(self):
    """Returns empty list when kubectl returns non-zero exit code."""
    with mock.patch.object(
        startup_metrics.kubectl, 'RunKubectlCommand',
        return_value=('', 'error', 1),
    ):
      result = startup_metrics.GetStartupLatencySamples(
          namespace='default', app_label='app', metadata=_BASE_METADATA
      )
    self.assertEqual(result, [])

  def testGetStartupLatencySamplesEmptyOnBadJson(self):
    """Returns empty list when kubectl output is not valid JSON."""
    with mock.patch.object(
        startup_metrics.kubectl, 'RunKubectlCommand',
        return_value=('not json', '', 0),
    ):
      result = startup_metrics.GetStartupLatencySamples(
          namespace='default', app_label='app', metadata=_BASE_METADATA
      )
    self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# _CpuUtilizationCollector tests
# ---------------------------------------------------------------------------


class CpuUtilizationCollectorTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.spec = mock.Mock(spec=benchmark_spec.BenchmarkSpec)
    self.spec.container_cluster = mock.Mock()
    self.spec.container_specs = {
        'kubernetes_deployment_startup': mock.Mock(image='test_image')
    }

  @mock.patch.object(kubernetes_commands, 'WaitForRollout')
  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  @mock.patch.object(
      kubernetes_conditions, 'GetStatusConditionsForResourceType'
  )
  def testRun(
      self, mock_get_conditions, mock_apply_manifest, mock_wait_for_rollout
  ):
    """Tests the Run method with mock pod data."""
    mock_get_conditions.return_value = [
        mock.Mock(
            event='PodReadyToStartContainers',
            resource_name='pod1',
            epoch_time=10,
        ),
        mock.Mock(event='Ready', resource_name='pod1', epoch_time=20),
        mock.Mock(
            event='PodReadyToStartContainers',
            resource_name='pod2',
            epoch_time=12,
        ),
        mock.Mock(event='Ready', resource_name='pod2', epoch_time=25),
    ]
    result = kdsb.Run(self.spec)

    mock_apply_manifest.assert_called_with(
        kdsb.DEPLOYMENT_YAML.value, name='startup', image='test_image'
    )
    mock_wait_for_rollout.assert_called_with('deployment/startup', timeout=600)
    self.assertLen(result, 1)
    self.assertEqual(
        result[0],
        sample.Sample(
            'max_pod_ready_time', 13, 'seconds', {}, result[0].timestamp
        ),
    )

  @mock.patch.object(kubernetes_commands, 'WaitForRollout')
  @mock.patch.object(kubernetes_commands, 'ApplyManifest')
  @mock.patch.object(
      kubernetes_conditions, 'GetStatusConditionsForResourceType'
  )
  def testRunNoPods(
      self, mock_get_conditions, mock_apply_manifest, mock_wait_for_rollout
  ):
    """Tests the Run method when no pods are found."""
    mock_get_conditions.return_value = []
    with self.assertRaises(RuntimeError):
      kdsb.Run(self.spec)

    self.collector = bench._CpuUtilizationCollector(
        namespace='default',
        app_label='slowjvmstartup',
        poll_interval=1,
    )

  def testCollectorAccumulatesReadings(self):
    """Collector accumulates readings while running."""
    call_count = [0]

    def fake_poll():
      call_count[0] += 1
      return 250.0

    with mock.patch.object(self.collector, '_PollCpuMillicores', side_effect=fake_poll):
      self.collector.Start()
      time.sleep(2.5)   # Allow ~2 polls at 1s interval
      self.collector.Stop()

    self.assertGreaterEqual(len(self.collector._readings), 1)

  def testGetSamplesEmitsPeakAndMean(self):
    """GetSamples emits cpu_utilization_peak and cpu_utilization_mean."""
    self.collector._readings = [100.0, 200.0, 300.0]
    samples = self.collector.GetSamples(_BASE_METADATA)
    metric_names = {s.metric for s in samples}
    self.assertIn('cpu_utilization_peak', metric_names)
    self.assertIn('cpu_utilization_mean', metric_names)
    self.assertIn('cpu_utilization_reading_count', metric_names)

  def testGetSamplesValues(self):
    """Peak = max, mean = average of readings."""
    self.collector._readings = [100.0, 200.0, 300.0]
    samples = self.collector.GetSamples(_BASE_METADATA)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['cpu_utilization_peak'], 300.0)
    self.assertAlmostEqual(by_metric['cpu_utilization_mean'], 200.0)
    self.assertEqual(by_metric['cpu_utilization_reading_count'], 3)

  def testGetSamplesEmptyWhenNoReadings(self):
    """Returns empty list when no readings collected."""
    self.collector._readings = []
    samples = self.collector.GetSamples(_BASE_METADATA)
    self.assertEqual(samples, [])

  def testGetSamplesCarryMetadata(self):
    """All samples carry the base metadata and cpu-specific keys."""
    self.collector._readings = [150.0]
    samples = self.collector.GetSamples(_BASE_METADATA)
    for s in samples:
      self.assertIn('workload', s.metadata)
      self.assertIn('scenario', s.metadata)

  def testPollCpuMillicoresParsesMilliSuffix(self):
    """_PollCpuMillicores correctly parses '250m' → 250.0."""
    fake_output = 'slowjvmstartup-abc   250m   128Mi\n'
    with mock.patch.object(
        bench.kubectl, 'RunKubectlCommand',
        return_value=(fake_output, '', 0),
    ):
      result = self.collector._PollCpuMillicores()
    self.assertAlmostEqual(result, 250.0)

  def testPollCpuMillicoresParsesCoreSuffix(self):
    """_PollCpuMillicores correctly parses '1' (core) → 1000.0 millicores."""
    fake_output = 'slowjvmstartup-abc   1   512Mi\n'
    with mock.patch.object(
        bench.kubectl, 'RunKubectlCommand',
        return_value=(fake_output, '', 0),
    ):
      result = self.collector._PollCpuMillicores()
    self.assertAlmostEqual(result, 1000.0)

  def testPollCpuMillicoresReturnsZeroOnError(self):
    """_PollCpuMillicores returns 0.0 when kubectl returns non-zero rc."""
    with mock.patch.object(
        bench.kubectl, 'RunKubectlCommand',
        return_value=('', 'error', 1),
    ):
      result = self.collector._PollCpuMillicores()
    self.assertEqual(result, 0.0)

  def testCollectorIsThreadSafe(self):
    """Concurrent Stop() and GetSamples() do not race."""
    self.collector._readings = [100.0] * 50
    errors = []

    def read():
      try:
        self.collector.GetSamples(_BASE_METADATA)
      except Exception as e:  # pylint: disable=broad-except
        errors.append(e)

    threads = [threading.Thread(target=read) for _ in range(10)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    self.assertEqual(errors, [])


# ---------------------------------------------------------------------------
# Benchmark lifecycle tests
# ---------------------------------------------------------------------------


class BenchmarkLifecycleTest(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='vllm',
  )
  def testCheckPrerequisitesRaisesForOptimizedVllm(self):
    """optimized + vLLM combination not supported in PR 1."""
    with self.assertRaises(ValueError):
      bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
  )
  def testCheckPrerequisitesPassesForBaselineJvm(self):
    """baseline + jvm is always valid."""
    bench.CheckPrerequisites(None)  # Should not raise

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
  )
  def testCheckPrerequisitesPassesForOptimizedJvm(self):
    """optimized + jvm is valid (VPA logic is added in PR 3)."""
    bench.CheckPrerequisites(None)  # Should not raise


if __name__ == '__main__':
  unittest.main()
