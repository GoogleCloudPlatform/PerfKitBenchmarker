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
import threading
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import kubernetes_deployment_startup_benchmark as bench
from tests import pkb_common_test_case


def _make_condition(resource_name, event, epoch_time):
  """Returns a mock KubernetesStatusCondition."""
  c = mock.MagicMock()
  c.resource_name = resource_name
  c.event = event
  c.epoch_time = epoch_time
  return c


def _make_spec(image='test_image', config=None):
  """Returns a mock BenchmarkSpec."""
  bm = mock.MagicMock()
  bm.container_specs = {
      'kubernetes_deployment_startup': mock.MagicMock(image=image)
  }
  bm.config = config or {}
  return bm


def _default_conditions():
  return [
      _make_condition('pod-0', 'PodReadyToStartContainers', 1000),
      _make_condition('pod-0', 'PodRunning', 1025),
      _make_condition('pod-0', 'Ready', 1030),
  ]


def _run_with_conditions(conditions, flag_kwargs=None, config=None):
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
      bench.kubernetes_commands,
      'GetTotalCpuMillicores',
      return_value=250.0,
  ), flagsaver.flagsaver(**flag_kwargs):
    return bench.Run(_make_spec(config=config))


class MaxPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for max_pod_ready_time metric."""

  def testEmitsMaxPodReadyTime(self):
    """max_pod_ready_time is always emitted."""
    samples = _run_with_conditions(_default_conditions())
    self.assertIn('max_pod_ready_time', {s.metric for s in samples})

  def testMaxPodReadyTimeValue(self):
    """max_pod_ready_time equals worst pod across all replicas."""
    conditions = [
        _make_condition('pod-0', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-0', 'PodRunning', 1015),
        _make_condition('pod-0', 'Ready', 1020),
        _make_condition('pod-1', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-1', 'PodRunning', 1030),
        _make_condition('pod-1', 'Ready', 1035),
    ]
    samples = _run_with_conditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 35)

  def testOriginalTwoPodsValue(self):
    """Preserves original test: 2 pods with times 10 and 13; max=13."""
    conditions = [
        _make_condition('pod1', 'PodReadyToStartContainers', 10),
        _make_condition('pod1', 'PodRunning', 15),
        _make_condition('pod1', 'Ready', 20),
        _make_condition('pod2', 'PodReadyToStartContainers', 12),
        _make_condition('pod2', 'PodRunning', 20),
        _make_condition('pod2', 'Ready', 25),
    ]
    samples = _run_with_conditions(conditions)
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
        return_value=_default_conditions(),
    ), mock.patch.object(
        bench.kubernetes_commands, 'GetTotalCpuMillicores', return_value=250.0
    ), flagsaver.flagsaver(cloud='GCP'):
      bench.Prepare(_make_spec())
      bench.Run(_make_spec())
    mock_apply.assert_called_with(
        'container/kubernetes_deployment_startup/jenkins.yaml.j2',
        name='startup',
        image='test_image',
    )
    mock_wait.assert_called_with('deployment/startup', timeout=600)

  def testApplyManifestAndWaitForRolloutCalledVllm(self):
    """Verify ApplyManifest and WaitForRollout for vllm workload."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply, mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_default_conditions(),
    ), mock.patch.object(
        bench.kubernetes_commands, 'GetTotalCpuMillicores', return_value=250.0
    ), flagsaver.flagsaver(
        cloud='GCP',
        kubernetes_deployment_startup_workload='vllm',
        kubernetes_deployment_startup_vllm_gpu_memory_utilization=0.5,
        kubernetes_deployment_startup_vllm_memory_limit='8Gi',
    ):
      bench.Prepare(_make_spec())
      bench.Run(_make_spec())
    mock_apply.assert_called_with(
        'container/kubernetes_deployment_startup/vllm.yaml.j2',
        name='vllm-startup',
        image='test_image',
        gpu_memory_utilization=0.5,
        memory_limit='8Gi',
    )
    mock_wait.assert_called_with('deployment/vllm-startup', timeout=600)

  def testApplyManifestAndWaitForRolloutCalledOptimized(self):
    """Verify ApplyManifest and WaitForRollout for optimized scenario."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply, mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_commands, 'WaitForCrd'
    ) as mock_wait_crd, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_default_conditions(),
    ), mock.patch.object(
        bench.kubernetes_commands, 'GetTotalCpuMillicores', return_value=250.0
    ), flagsaver.flagsaver(
        cloud='GCP',
        kubernetes_deployment_startup_scenario='optimized',
        kubernetes_deployment_startup_workload='jvm',
        kubernetes_deployment_startup_boost_factor=3,
    ):
      bench.Prepare(_make_spec())
      bench.Run(_make_spec())
    mock_wait_crd.assert_called_with(
        'verticalpodautoscalers.autoscaling.k8s.io', 180
    )
    mock_apply.assert_any_call(
        'container/kubernetes_deployment_startup/jenkins_vpa.yaml.j2',
        name='startup',
        boost_factor=3,
        max_allowed_cpu='1',
        duration_seconds=120,
    )
    mock_apply.assert_any_call(
        'container/kubernetes_deployment_startup/jenkins.yaml.j2',
        name='startup',
        image='test_image',
    )
    mock_wait.assert_called_with('deployment/startup', timeout=600)

  def testRaisesWhenNoPodsReady(self):
    """RuntimeError raised when no pod conditions found."""
    with self.assertRaises(RuntimeError):
      _run_with_conditions([])


class PerPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for per_pod_ready_time metric."""

  def testEmitsOnePerPod(self):
    """One per_pod_ready_time sample per pod."""
    conditions = [
        _make_condition('pod-0', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-0', 'PodRunning', 1020),
        _make_condition('pod-0', 'Ready', 1025),
        _make_condition('pod-1', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-1', 'PodRunning', 1035),
        _make_condition('pod-1', 'Ready', 1040),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertLen(per_pod, 2)

  def testPerPodCarriesPodName(self):
    """per_pod_ready_time metadata contains pod_name."""
    conditions = [
        _make_condition('pod-abc', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-abc', 'PodRunning', 1025),
        _make_condition('pod-abc', 'Ready', 1030),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertEqual(per_pod[0].metadata['pod_name'], 'pod-abc')

  def testPerPodValue(self):
    """per_pod_ready_time value equals end_time - start_time."""
    conditions = [
        _make_condition('pod-x', 'PodReadyToStartContainers', 2000),
        _make_condition('pod-x', 'PodRunning', 2040),
        _make_condition('pod-x', 'Ready', 2045),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertAlmostEqual(per_pod[0].value, 45)


class StartupLatencyTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for startup_latency metric."""

  def testEmitsStartupLatency(self):
    """startup_latency is always emitted."""
    samples = _run_with_conditions(_default_conditions())
    self.assertIn('startup_latency', {s.metric for s in samples})

  def testStartupLatencyValue(self):
    """startup_latency equals worst pod across all replicas."""
    conditions = [
        _make_condition('pod-0', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-0', 'PodRunning', 1015),
        _make_condition('pod-0', 'Ready', 1020),
        _make_condition('pod-1', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-1', 'PodRunning', 1030),
        _make_condition('pod-1', 'Ready', 1040),
    ]
    samples = _run_with_conditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency'], 10)


class PerPodStartupLatencyTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for per_pod_startup_latency metric."""

  def testEmitsOnePerPod(self):
    """One per_pod_startup_latency sample per pod."""
    conditions = [
        _make_condition('pod-0', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-0', 'PodRunning', 1020),
        _make_condition('pod-0', 'Ready', 1025),
        _make_condition('pod-1', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-1', 'PodRunning', 1035),
        _make_condition('pod-1', 'Ready', 1045),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_startup_latency']
    self.assertLen(per_pod, 2)

  def testPerPodCarriesPodName(self):
    """per_pod_startup_latency metadata contains pod_name."""
    conditions = [
        _make_condition('pod-abc', 'PodReadyToStartContainers', 1000),
        _make_condition('pod-abc', 'PodRunning', 1025),
        _make_condition('pod-abc', 'Ready', 1030),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_startup_latency']
    self.assertEqual(per_pod[0].metadata['pod_name'], 'pod-abc')

  def testPerPodValue(self):
    """per_pod_startup_latency value equals end_time - start_time."""
    conditions = [
        _make_condition('pod-x', 'PodReadyToStartContainers', 2000),
        _make_condition('pod-x', 'PodRunning', 2040),
        _make_condition('pod-x', 'Ready', 2055),
    ]
    samples = _run_with_conditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_startup_latency']
    self.assertAlmostEqual(per_pod[0].value, 15)


class SampleMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for scenario/workload/cloud metadata on samples."""

  def testAllPodSamplesCarryMetadata(self):
    """scenario, workload, cloud present on all pod samples."""
    samples = _run_with_conditions(
        _default_conditions(),
        flag_kwargs={
            'cloud': 'GCP',
        },
        config={
            'scenario': 'baseline',
            'workload': 'jvm',
        },
    )
    pod_samples = [
        s for s in samples
        if s.metric in ('max_pod_ready_time', 'per_pod_ready_time',
                        'startup_latency', 'per_pod_startup_latency')
    ]
    for s in pod_samples:
      self.assertEqual(s.metadata['scenario'], 'baseline')
      self.assertEqual(s.metadata['workload'], 'jvm')
      self.assertEqual(s.metadata['cloud'], 'GCP')


class CpuUtilizationCollectorTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _CpuUtilizationCollector."""

  def _MakeCollector(self):
    samples = []
    stop = threading.Event()
    collector = bench._CpuUtilizationCollector(
        samples, stop, 'test-deployment', {'scenario': 'baseline'}
    )
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
    collector, _, stop = self._MakeCollector()
    collector._readings = []
    stop.set()
    with self.assertRaises(RuntimeError):
      collector.ObserveCpuUtilization()

  def testObserveIgnoresIssueCommandError(self):
    """_Observe continues on IssueCommandError."""
    collector, _, stop = self._MakeCollector()
    call_count = [0]

    def _flaky():
      call_count[0] += 1
      if call_count[0] < 3:
        raise errors.VmUtil.IssueCommandError('transient')
      stop.set()
      return []

    collector._Observe(_flaky)
    self.assertEqual(call_count[0], 3)

  def testThreadSafety(self):
    """Concurrent appends to _readings are thread-safe."""
    collector, _, _ = self._MakeCollector()
    errs = []

    def _append_readings():
      try:
        for _ in range(50):
          with collector._lock:
            collector._readings.append(1.0)
      except Exception as e:  # pylint: disable=broad-except
        errs.append(e)

    threads = [threading.Thread(target=_append_readings) for _ in range(4)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertEqual(errs, [])
    self.assertLen(collector._readings, 200)


if __name__ == '__main__':
  unittest.main()
