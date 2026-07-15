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
"""Tests for kubernetes_deployment_startup_benchmark (PR 1 + PR 2 + PR 3)."""

import os
import threading
import unittest
from unittest import mock

from absl.testing import flagsaver
import jinja2
import yaml
from perfkitbenchmarker import data
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


def _MakeSpec(image='slowjvmstartup'):
  """Returns a mock BenchmarkSpec."""
  bm = mock.MagicMock()
  bm.container_specs = {
      'kubernetes_deployment_startup': mock.MagicMock(image=image)
  }
  return bm


def _DefaultConditions():
  # Includes a PodRunning entry so startup_latency is always computable by
  # default -- tests that specifically exercise the "PodRunning missing"
  # failure path build their own conditions list without it.
  return [
      _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
      _MakeCondition('pod-0', 'PodRunning', 1015),
      _MakeCondition('pod-0', 'Ready', 1030),
  ]


def _RunWithConditions(conditions, flag_kwargs=None, cpu_millicores=100.0):
  """Runs bench.Run() with mocked kubectl calls.

  Args:
    conditions: Pod status conditions to return from
      GetStatusConditionsForResourceType.
    flag_kwargs: Flags to set via flagsaver.
    cpu_millicores: Value _GetTotalCpuMillicores should return on every
      poll. Defaults to a real value (not None) so cpu_utilization is
      computable by default -- tests exercising the "zero CPU readings"
      failure path override this to None explicitly.
  """
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
      bench, '_GetTotalCpuMillicores', return_value=cpu_millicores
  ), flagsaver.flagsaver(
      **flag_kwargs
  ):
    return bench.Run(_MakeSpec())


# ---------------------------------------------------------------------------
# PR 1: existing + new metrics
# ---------------------------------------------------------------------------


class MaxPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for max_pod_ready_time metric (existing, preserved)."""

  def testEmitsMaxPodReadyTime(self):
    samples = _RunWithConditions(_DefaultConditions())
    self.assertIn('max_pod_ready_time', {s.metric for s in samples})

  def testMaxPodReadyTimeValue(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1010),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'PodRunning', 1010),
        _MakeCondition('pod-1', 'Ready', 1035),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 35)

  def testRaisesWhenNoPodsReady(self):
    with self.assertRaises(RuntimeError):
      _RunWithConditions([])


class PerPodReadyTimeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for per_pod_ready_time metric (PR 1)."""

  def testEmitsOnePerPod(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1010),
        _MakeCondition('pod-0', 'Ready', 1025),
        _MakeCondition('pod-1', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-1', 'PodRunning', 1010),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertLen(per_pod, 2)

  def testPerPodCarriesPodName(self):
    conditions = [
        _MakeCondition('pod-abc', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-abc', 'PodRunning', 1010),
        _MakeCondition('pod-abc', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_ready_time']
    self.assertEqual(per_pod[0].metadata['pod_name'], 'pod-abc')


class StartupLatencyTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the startup_latency metric (PodRunning -> Ready)."""

  def testEmitsStartupLatency(self):
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1005),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    self.assertIn('startup_latency', {s.metric for s in samples})

  def testStartupLatencyValue(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1005),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency'], 25)

  def testStartupLatencyTakesMaxAcrossPods(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1000),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodRunning', 1000),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['startup_latency'], 40)

  def testEmitsOnePerPodStartupLatency(self):
    conditions = [
        _MakeCondition('pod-0', 'PodRunning', 1000),
        _MakeCondition('pod-0', 'Ready', 1020),
        _MakeCondition('pod-1', 'PodRunning', 1000),
        _MakeCondition('pod-1', 'Ready', 1040),
    ]
    samples = _RunWithConditions(conditions)
    per_pod = [s for s in samples if s.metric == 'per_pod_startup_latency']
    self.assertLen(per_pod, 2)

  def testDistinctFromMaxPodReadyTime(self):
    # PodReadyToStartContainers is earlier than PodRunning (scheduling +
    # image pull happen first), so startup_latency should be smaller than
    # max_pod_ready_time for the same pod.
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'PodRunning', 1015),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    samples = _RunWithConditions(conditions)
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['max_pod_ready_time'], 30)
    self.assertAlmostEqual(by_metric['startup_latency'], 15)
    self.assertLess(
        by_metric['startup_latency'], by_metric['max_pod_ready_time']
    )

  def testRaisesWhenPodRunningMissing(self):
    # Per review: if the cluster/runtime never reports containerStatuses
    # startedAt for any pod, startup_latency can't be computed at all --
    # this must fail loudly rather than silently succeeding with the
    # metric missing (a silent warning here is exactly what let the VPA
    # ordering bug ship an unboosted "optimized" run undetected).
    conditions = [
        _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
        _MakeCondition('pod-0', 'Ready', 1030),
    ]
    with self.assertRaises(RuntimeError):
      _RunWithConditions(conditions)


class SampleMetadataTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for sample metadata (PR 1)."""

  def testAllSamplesCarryScenarioWorkloadCloud(self):
    samples = _RunWithConditions(
        _DefaultConditions(),
        flag_kwargs={
            'cloud': 'GCP',
            'kubernetes_deployment_startup_scenario': 'baseline',
            'kubernetes_deployment_startup_workload': 'jvm',
        },
    )
    pod_samples = [
        s
        for s in samples
        if s.metric in ('max_pod_ready_time', 'per_pod_ready_time')
    ]
    for s in pod_samples:
      self.assertEqual(s.metadata['scenario'], 'baseline')
      self.assertEqual(s.metadata['workload'], 'jvm')
      self.assertEqual(s.metadata['cloud'], 'GCP')


# ---------------------------------------------------------------------------
# PR 1: CPU utilization collector
# ---------------------------------------------------------------------------


class CpuUtilizationCollectorTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _CpuUtilizationCollector (PR 1)."""

  def _MakeCollector(self):
    samples = []
    stop = threading.Event()
    collector = bench._CpuUtilizationCollector(samples, stop)
    return collector, samples, stop

  def testEmitsPeakMeanCount(self):
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    metrics = {s.metric for s in samples}
    self.assertIn('cpu_utilization_peak_millicores', metrics)
    self.assertIn('cpu_utilization_mean_millicores', metrics)
    self.assertIn('cpu_utilization_reading_count', metrics)

  def testPeakAndMeanValues(self):
    collector, samples, stop = self._MakeCollector()
    collector._readings = [100.0, 200.0, 300.0]
    stop.set()
    collector.ObserveCpuUtilization()
    by_metric = {s.metric: s.value for s in samples}
    self.assertAlmostEqual(by_metric['cpu_utilization_peak_millicores'], 300.0)
    self.assertAlmostEqual(by_metric['cpu_utilization_mean_millicores'], 200.0)
    self.assertEqual(by_metric['cpu_utilization_reading_count'], 3)

  def testRaisesWhenNoReadings(self):
    # Per review: zero CPU readings for the whole run must fail loudly
    # rather than silently shipping results with cpu_utilization missing.
    collector, samples, stop = self._MakeCollector()
    collector._readings = []
    stop.set()
    with self.assertRaises(RuntimeError):
      collector.ObserveCpuUtilization()
    self.assertEqual(samples, [])

  def testObserveIgnoresIssueCommandError(self):
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

  def testRunRaisesWhenCpuCollectionFailsEntirely(self):
    # End-to-end: bench.Run() runs the collector on a background thread,
    # so this also verifies the collector's RuntimeError is captured and
    # re-raised on the main thread rather than silently disappearing.
    with self.assertRaises(RuntimeError):
      _RunWithConditions(_DefaultConditions(), cpu_millicores=None)

  def testThreadSafety(self):
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
# PR 1: _GetTotalCpuMillicores
# ---------------------------------------------------------------------------


class GetTotalCpuMillicoresTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _GetTotalCpuMillicores helper (PR 1)."""

  def _MockKubectl(self, stdout, rc=0):
    return mock.patch.object(
        bench.kubectl,
        'RunKubectlCommand',
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


# ---------------------------------------------------------------------------
# PR 2: vLLM workload
# ---------------------------------------------------------------------------


class VllmWorkloadTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for vLLM workload support (PR 2)."""

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='vllm', cloud='GCP'
  )
  def testPrepareDeploysVllmManifest(self):
    bm = _MakeSpec(image='public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest')
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply:
      bench.Prepare(bm)
      call_args = mock_apply.call_args[0][0]
      self.assertIn('vllm', call_args)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='jvm', cloud='GCP'
  )
  def testPrepareDeploysJvmManifest(self):
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply:
      bench.Prepare(_MakeSpec())
      call_args = mock_apply.call_args[0][0]
      self.assertIn('slowjvmstartup', call_args)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='vllm', cloud='GCP'
  )
  def testRunWaitsOnVllmDeployment(self):
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=100.0
    ):
      bench.Run(_MakeSpec())
      mock_wait.assert_called_with('deployment/vllm-startup', timeout=600)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='jvm', cloud='GCP'
  )
  def testRunWaitsOnJvmDeployment(self):
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=100.0
    ):
      bench.Run(_MakeSpec())
      mock_wait.assert_called_with('deployment/startup', timeout=600)


# ---------------------------------------------------------------------------
# PR 3: VPA scenario + boost factor
# ---------------------------------------------------------------------------


class ScenarioTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for VPA scenario and CPU Startup Boost (PR 3)."""

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testCheckPrerequisitesPassesOptimizedGcp(self):
    bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='AWS',
  )
  def testCheckPrerequisitesRaisesOptimizedNonGcp(self):
    with self.assertRaises(ValueError):
      bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='vllm',
      cloud='GCP',
  )
  def testCheckPrerequisitesRaisesOptimizedVllm(self):
    with self.assertRaises(ValueError):
      bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      cloud='AWS',
  )
  def testCheckPrerequisitesPassesBaselineAnyCloud(self):
    bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testGetConfigEnablesVpa(self):
    config = bench.GetConfig({})
    self.assertTrue(config['container_cluster'].get('enable_vpa'))

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      cloud='GCP',
  )
  def testGetConfigNoVpaForBaseline(self):
    config = bench.GetConfig({})
    self.assertFalse(config['container_cluster'].get('enable_vpa', False))

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      kubernetes_deployment_startup_boost_factor=2,
      cloud='GCP',
  )
  def testPrepareAppliesVpaManifest(self):
    apply_calls = []
    with mock.patch.object(
        bench.kubernetes_commands,
        'ApplyManifest',
        side_effect=lambda *a, **kw: apply_calls.append(a[0]),
    ), mock.patch.object(
        bench.kubectl, 'RunKubectlCommand', return_value=('', '', 0)
    ):
      bench.Prepare(_MakeSpec())
    self.assertLen(apply_calls, 2)
    self.assertTrue(any('vpa' in c for c in apply_calls))

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      kubernetes_deployment_startup_boost_factor=2,
      cloud='GCP',
  )
  def testPrepareAppliesVpaBeforeDeployment(self):
    # Regression test: GKE CPU Startup Boost only mutates a pod's CPU
    # request at admission time, for pods created AFTER the VPA object
    # exists. If the Deployment (and its first pod) were applied before
    # the VPA, the boost would never apply to the pod this benchmark
    # measures -- which is exactly what happened in the config 4 run that
    # showed no improvement over baseline. The VPA must be applied first.
    apply_calls = []
    with mock.patch.object(
        bench.kubernetes_commands,
        'ApplyManifest',
        side_effect=lambda *a, **kw: apply_calls.append(a[0]),
    ), mock.patch.object(
        bench.kubectl, 'RunKubectlCommand', return_value=('', '', 0)
    ):
      bench.Prepare(_MakeSpec())
    self.assertLen(apply_calls, 2)
    vpa_index = next(i for i, c in enumerate(apply_calls) if 'vpa' in c)
    deployment_index = next(
        i for i, c in enumerate(apply_calls) if 'vpa' not in c
    )
    self.assertLess(
        vpa_index,
        deployment_index,
        'VPA manifest must be applied before the JVM deployment manifest,'
        ' otherwise CPU Startup Boost cannot affect the measured pod.',
    )

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testPrepareSkipsVpaForBaseline(self):
    apply_calls = []
    with mock.patch.object(
        bench.kubernetes_commands,
        'ApplyManifest',
        side_effect=lambda *a, **kw: apply_calls.append(a[0]),
    ):
      bench.Prepare(_MakeSpec())
    self.assertLen(apply_calls, 1)
    self.assertFalse(any('vpa' in c for c in apply_calls))

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      kubernetes_deployment_startup_boost_factor=3,
      cloud='GCP',
  )
  def testBoostFactorInMetadata(self):
    samples = _RunWithConditions(
        _DefaultConditions(),
        flag_kwargs={
            'cloud': 'GCP',
            'kubernetes_deployment_startup_scenario': 'optimized',
            'kubernetes_deployment_startup_workload': 'jvm',
            'kubernetes_deployment_startup_boost_factor': 3,
        },
    )
    pod_samples = [s for s in samples if 'boost_factor' in s.metadata]
    self.assertTrue(len(pod_samples) > 0)
    for s in pod_samples:
      self.assertEqual(s.metadata['boost_factor'], 3)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testBoostFactorIsOneForBaseline(self):
    samples = _RunWithConditions(
        _DefaultConditions(),
        flag_kwargs={
            'cloud': 'GCP',
            'kubernetes_deployment_startup_scenario': 'baseline',
            'kubernetes_deployment_startup_workload': 'jvm',
        },
    )
    pod_samples = [s for s in samples if 'boost_factor' in s.metadata]
    for s in pod_samples:
      self.assertEqual(s.metadata['boost_factor'], 1)


# ---------------------------------------------------------------------------
# PR 3: VPA CRD registration race fix
#
# Enabling VPA via --enable-vertical-pod-autoscaling triggers an
# asynchronous GKE addon install for the VPA CRDs, which can lag behind the
# cluster's own RUNNING status and kube-dns readiness. A production run
# (config 4) hit exactly this: `kubectl apply` for the VerticalPodAutoscaler
# manifest failed with "no matches for kind VerticalPodAutoscaler" just a
# few seconds after kube-dns reported ready, because the CRD wasn't
# registered yet. _WaitForVpaCrd polls for the CRD before Prepare() applies
# the VPA manifest, instead of assuming it's already there.
# ---------------------------------------------------------------------------


class WaitForVpaCrdTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _WaitForVpaCrd (VPA CRD registration race fix)."""

  def testSucceedsWhenCrdAlreadyRegistered(self):
    with mock.patch.object(
        bench.kubectl, 'RunKubectlCommand', return_value=('', '', 0)
    ) as mock_kubectl:
      bench._WaitForVpaCrd()
    # Must NOT pass raise_on_failure=False: kubectl.RunKubectlCommand's
    # suppress_failure wrapper rewrites a suppressed failure's return code
    # to 0, which would silently defeat any retcode-based readiness check
    # -- this is exactly the bug that shipped in the first version of this
    # fix (a failing "get crd" was reported as success). _WaitForVpaCrd
    # must rely on RunKubectlCommand raising naturally instead.
    mock_kubectl.assert_called_once_with(['get', 'crd', bench._VPA_CRD_NAME])

  def testRaisesRuntimeErrorWhenCrdNeverRegisters(self):
    # A short timeout keeps this test fast: vm_util.Retry checks the
    # deadline before sleeping, so with timeout << poll_interval it raises
    # on the very first failed poll instead of actually waiting.
    #
    # side_effect (a raised IssueCommandError), not return_value with an
    # rc=1 tuple: kubectl.RunKubectlCommand's suppress_failure wrapper
    # rewrites a suppressed failure's return code to 0, so a mock that
    # just returns rc=1 would not reproduce the real "get crd" failure
    # mode -- this distinction is exactly what let the CRD-wait fix ship
    # with a check that could never actually trigger.
    with mock.patch.object(
        bench, '_VPA_CRD_WAIT_TIMEOUT_SECS', 1
    ), mock.patch.object(
        bench.kubectl,
        'RunKubectlCommand',
        side_effect=errors.VmUtil.IssueCommandError(
            'Error from server (NotFound)'
        ),
    ):
      with self.assertRaises(RuntimeError):
        bench._WaitForVpaCrd()


if __name__ == '__main__':
  unittest.main()


# ---------------------------------------------------------------------------
# Regression test: manifest files must actually exist on disk.
#
# All tests above mock kubernetes_commands.ApplyManifest, so none of them
# ever resolve DEPLOYMENT_YAML/VLLM_YAML/VPA_YAML against the real `data/`
# directory. That blind spot let PR 2 and PR 3 ship referencing
# vllm.yaml.j2 and slowjvmstartup_vpa.yaml.j2 without ever committing them,
# which only surfaced as a runtime ResourceNotFound crash in production.
# These tests close that gap by calling data.ResourcePath() for real.
# ---------------------------------------------------------------------------


class ManifestResourceResolutionTest(pkb_common_test_case.PkbCommonTestCase):
  """Confirms every manifest flag default resolves to a real, valid file."""

  def testJvmManifestResolves(self):
    path = data.ResourcePath(bench.DEPLOYMENT_YAML.value)
    self.assertTrue(os.path.isfile(path))

  def testVllmManifestResolves(self):
    path = data.ResourcePath(bench.VLLM_YAML.value)
    self.assertTrue(os.path.isfile(path))

  def testVpaManifestResolves(self):
    path = data.ResourcePath(bench.VPA_YAML.value)
    self.assertTrue(os.path.isfile(path))

  def testJvmManifestRendersValidYaml(self):
    path = data.ResourcePath(bench.DEPLOYMENT_YAML.value)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.dirname(path))
    )
    rendered = env.get_template(os.path.basename(path)).render(
        name='startup', image='slowjvmstartup'
    )
    docs = list(yaml.safe_load_all(rendered))
    self.assertEqual(docs[0]['kind'], 'Deployment')

  def testVllmManifestRendersValidYaml(self):
    path = data.ResourcePath(bench.VLLM_YAML.value)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.dirname(path))
    )
    rendered = env.get_template(os.path.basename(path)).render(
        name='vllm-startup',
        image='public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest',
    )
    docs = list(yaml.safe_load_all(rendered))
    self.assertEqual(docs[0]['kind'], 'Deployment')
    self.assertEqual(docs[1]['kind'], 'Service')

  def testVpaManifestRendersValidYamlWithBoostFactor(self):
    path = data.ResourcePath(bench.VPA_YAML.value)
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.dirname(path))
    )
    rendered = env.get_template(os.path.basename(path)).render(
        name='startup', boost_factor=3
    )
    docs = list(yaml.safe_load_all(rendered))
    self.assertEqual(docs[0]['kind'], 'VerticalPodAutoscaler')
    self.assertEqual(docs[0]['spec']['startupBoost']['cpu']['factor'], 3)
