# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for kubernetes_deployment_startup_benchmark (PR 1)."""

import threading
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import (
    kubernetes_deployment_startup_benchmark as bench,
)
from tests import pkb_common_test_case


def _MakeCondition(resource_name, event, epoch_time):
  """Returns a mock KubernetesStatusCondition."""
  c = mock.MagicMock()
  c.resource_name = resource_name
  c.event = event
  c.epoch_time = epoch_time
  return c


def _MakeSpec(image='test_image'):
  """Returns a mock BenchmarkSpec."""
  bm = mock.MagicMock()
  bm.container_specs = {
      'kubernetes_deployment_startup': mock.MagicMock(image=image)
  }
  return bm


def _DefaultConditions():
  return [
      _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
      _MakeCondition('pod-0', 'Ready', 1030),
  ]


def _RunWithConditions(conditions, flag_kwargs=None):
  """Runs bench.Run() with mocked kubectl calls."""
  flag_kwargs = flag_kwargs or {'cloud': 'GCP'}
  with mock.patch.object(
      bench.kubernetes_commands, 'ApplyManifest'
  ), mock.patch.object(
      bench.kubernetes_commands, 'WaitForRollout'
  ), mock.patch.object(
      bench.kubernetes_conditions,
      'GetStatusConditionsForResourceType',
      return_value=conditions,
  ), mock.patch.object(
      bench, '_GetTotalCpuMillicores', return_value=None
  ), flagsaver.flagsaver(**flag_kwargs):
    return bench.Run(_MakeSpec())


# ---------------------------------------------------------------------------
# Existing metric: max_pod_ready_time (preserved from original)
# ---------------------------------------------------------------------------


class MaxPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for max_pod_ready_time metric (existing, preserved)."""

  def testEmitsMaxPodReadyTime(self):
    """max_pod_ready_time is always emitted."""
    samples = _RunWithConditions(_DefaultConditions())
    self.assertIn('max_pod_ready_time', {s.metric for s in samples})

  def testMaxPodReadyTimeValue(self):
    """max_pod_ready_time equals worst pod across all replicas."""
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'Ready', 1035),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 35)

  def testOriginalTwoPodsValue(self):
    """Preserves original test: 2 pods with times 10 and 13; max=13."""
    conditions = [
        _MakeCondition('pod1', 'PodReadyToStartContainers', 10),
        _MakeCondition('pod1', 'Ready', 20),
        _MakeCondition('pod2', 'PodReadyToStartContainers', 12),
        _MakeCondition('pod2', 'Ready', 25),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 13)

  def testApplyManifestAndWaitForRolloutCalled(self):
    """Verify ApplyManifest and WaitForRollout are called correctly."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply, mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ), flagsaver.flagsaver(cloud='GCP'):
      bench.Run(_MakeSpec())
    mock_apply.assert_called_with(
        bench.DEPLOYMENT_YAML.value, name='startup', image='test_image'
    )
    mock_wait.assert_called_with('deployment/startup', timeout=600)

  def testRaisesWhenNoPodsReady(self):
    """RuntimeError raised when no pod conditions found."""
    with self.assertRaises(RuntimeError):
      _RunWithConditions([])


# ---------------------------------------------------------------------------
# New metric: per_pod_ready_time (PR 1)
# ---------------------------------------------------------------------------


class PerPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for per_pod_ready_time metric (PR 1)."""

  def testEmitsOnePerPod(self):
    """One per_pod_ready_time sample per pod."""
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'Ready', 1025),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertLen(per_pod, 2)

  def testPerPodCarriesPodName(self):
    """per_pod_ready_time metadata contains pod_name."""
    conditions = [
        _MakeCondition('pod-abc', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-abc', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertEqual(per_pod[0].metadata['pod_name'], 'pod-abc')

  def testPerPodValue(self):
    """per_pod_ready_time value equals end_time - start_time."""
    conditions = [
        _MakeCondition('pod-x', 'PodReadyToStartContainers', 2000),
        _MakeCondition('pod-x', 'Ready', 2045),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertAlmostEqual(per_pod[0].value, 45)


# ---------------------------------------------------------------------------
# Sample metadata (PR 1)
# ---------------------------------------------------------------------------


class SampleMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for scenario/workload/cloud metadata on samples (PR 1)."""

  def testAllPodSamplesCarryMetadata(self):
    """scenario, workload, cloud present on all pod samples."""
    samples = _RunWithConditions(
        _DefaultConditions(),
        flag_kwargs={
            'cloud': 'GCP',
            'kubernetes_deployment_startup_scenario': 'baseline',
            'kubernetes_deployment_startup_workload': 'jvm',
        },
    )
    pod_samples = [
        s for s in samples
        if s.metric in ('max_pod_ready_time', 'per_pod_ready_time')
    ]
    for s in pod_samples:
      self.assertEqual(s.metadata['scenario'], 'baseline')
      self.assertEqual(s.metadata['workload'], 'jvm')
      self.assertEqual(s.metadata['cloud'], 'GCP')


# ---------------------------------------------------------------------------
# CPU utilization collector (PR 1)
# ---------------------------------------------------------------------------


class CpuUtilizationCollectorTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _CpuUtilizationCollector (PR 1)."""

  def _MakeCollector(self):
    samples = []
    stop = threading.Event()
    collector = bench._CpuUtilizationCollector(samples, stop)
    return collector, samples, stop

  def testEmitsPeakMeanCount(self):
    """ObserveCpuUtilization emits peak/mean/count samples."""
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    metrics = {s.metric for s in samples}
    self.assertIn('cpu_utilization_peak_millicores', metrics)
    self.assertIn('cpu_utilization_mean_millicores', metrics)
    self.assertIn('cpu_utilization_reading_count', metrics)

  def testPeakAndMeanValues(self):
    """Peak = max, mean = arithmetic mean of readings."""
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['cpu_utilization_peak_millicores'], 300.0)
    self.assertAlmostEqual(by_metric['cpu_utilization_mean_millicores'], 200.0)
    self.assertEqual(by_metric['cpu_utilization_reading_count'], 3)

  def testNoSamplesWhenNoReadings(self):
    """No CPU samples emitted when no readings collected."""
    collector, samples, stop = self._MakeCollector()
    collector._readings = []
    stop.set()
    collector.ObserveCpuUtilization()
    self.assertEqual(samples, [])

  def testObserveIgnoresIssueCommandError(self):
    """_Observe continues on IssueCommandError."""
    collector, _, stop = self._MakeCollector()
    call_count = [0]

    def flaky():
      call_count[0] += 1
      if call_count[0] < 3:
        raise errors.VmUtil.IssueCommandError('transient')
      stop.set()
      return []

    collector._Observe(flaky)
    self.assertEqual(call_count[0], 3)

  def testThreadSafety(self):
    """Concurrent appends to _readings are thread-safe."""
    collector, _, _ = self._MakeCollector()
    errs = []

    def append_readings():
      try:
        for _ in range(50):
          with collector._lock:
            collector._readings.append(1.0)
      except Exception as e:  # pylint: disable=broad-except
        errs.append(e)

    threads = [threading.Thread(target=append_readings) for _ in range(4)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertEqual(errs, [])
    self.assertEqual(len(collector._readings), 200)


# ---------------------------------------------------------------------------
# _GetTotalCpuMillicores (PR 1)
# ---------------------------------------------------------------------------


class GetTotalCpuMillicoresTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _GetTotalCpuMillicores helper (PR 1)."""

  def _MockKubectl(self, stdout, rc=0):
    return mock.patch.object(
        bench.kubectl, 'RunKubectlCommand',
        return_value=(stdout, '', rc),
    )

  def testParsesMiliSuffix(self):
    with self._MockKubectl('pod-abc   250m   128Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 250.0)

  def testParsesCoreSuffix(self):
    with self._MockKubectl('pod-abc   1   512Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 1000.0)

  def testSumsMultiplePods(self):
    with self._MockKubectl('pod-0   100m   64Mi\npod-1   150m   64Mi\n'):
      self.assertAlmostEqual(bench._GetTotalCpuMillicores(), 250.0)

  def testReturnsNoneOnError(self):
    with self._MockKubectl('', rc=1):
      self.assertIsNone(bench._GetTotalCpuMillicores())

  def testReturnsNoneOnEmpty(self):
    with self._MockKubectl(''):
      self.assertIsNone(bench._GetTotalCpuMillicores())


if __name__ == '__main__':
  unittest.main()
