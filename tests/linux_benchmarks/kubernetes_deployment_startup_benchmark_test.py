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
"""Tests for PR 2 (vLLM workload) and PR 3 (VPA scenario) additions."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker.linux_benchmarks import kubernetes_deployment_startup_benchmark as bench
from tests import pkb_common_test_case


def _MakeCondition(resource_name, event, epoch_time):
  c = mock.MagicMock()
  c.resource_name = resource_name
  c.event = event
  c.epoch_time = epoch_time
  return c


def _MakeSpec():
  bm = mock.MagicMock()
  bm.container_specs = {
      'kubernetes_deployment_startup': mock.MagicMock(image='slowjvmstartup')
  }
  return bm


def _DefaultConditions():
  return [
      _MakeCondition('pod-0', 'PodReadyToStartContainers', 1000),
      _MakeCondition('pod-0', 'Ready', 1030),
  ]


# ---------------------------------------------------------------------------
# PR 2: vLLM workload
# ---------------------------------------------------------------------------


class VllmWorkloadTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for vLLM workload support (PR 2)."""

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='vllm',
      cloud='GCP',
  )
  def testPrepareDeploysVllmManifest(self):
    """Prepare() applies vllm.yaml.j2 when workload=vllm."""
    bm = _MakeSpec()
    bm.container_specs['kubernetes_deployment_startup'].image = (
        'public.ecr.aws/q9t5s3a7/vllm-cpu-release-repo:latest'
    )
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply:
      bench.Prepare(bm)
      mock_apply.assert_called_once()
      call_args = mock_apply.call_args[0][0]
      self.assertIn('vllm', call_args)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testPrepareDeploysJvmManifest(self):
    """Prepare() applies slowjvmstartup.yaml.j2 when workload=jvm."""
    bm = _MakeSpec()
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ) as mock_apply:
      bench.Prepare(bm)
      mock_apply.assert_called_once()
      call_args = mock_apply.call_args[0][0]
      self.assertIn('slowjvmstartup', call_args)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='vllm',
      cloud='GCP',
  )
  def testRunUsesVllmDeploymentName(self):
    """Run() waits for vllm-startup deployment when workload=vllm."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ):
      bench.Run(_MakeSpec())
      mock_wait.assert_called_with('deployment/vllm-startup', timeout=600)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testRunUsesJvmDeploymentName(self):
    """Run() waits for startup deployment when workload=jvm."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ) as mock_wait, mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ):
      bench.Run(_MakeSpec())
      mock_wait.assert_called_with('deployment/startup', timeout=600)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_workload='vllm',
      cloud='GCP',
  )
  def testWorkloadMetadataInSamples(self):
    """All samples carry workload=vllm in metadata."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ), mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ):
      samples = bench.Run(_MakeSpec())
    pod_samples = [s for s in samples if 'workload' in s.metadata]
    for s in pod_samples:
      self.assertEqual(s.metadata['workload'], 'vllm')


# ---------------------------------------------------------------------------
# PR 3: scenario + VPA boost
# ---------------------------------------------------------------------------


class ScenarioTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for VPA scenario and CPU Startup Boost (PR 3)."""

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testCheckPrerequisitesPassesForOptimizedGcp(self):
    """optimized + jvm + GCP is valid."""
    bench.CheckPrerequisites(None)  # Should not raise.

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='AWS',
  )
  def testCheckPrerequisitesRaisesForOptimizedNonGcp(self):
    """optimized scenario requires GCP."""
    with self.assertRaises(ValueError):
      bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='vllm',
      cloud='GCP',
  )
  def testCheckPrerequisitesRaisesForOptimizedVllm(self):
    """optimized + vllm is not supported."""
    with self.assertRaises(ValueError):
      bench.CheckPrerequisites(None)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
      cloud='AWS',
  )
  def testCheckPrerequisitesPassesForBaselineAws(self):
    """baseline on any cloud is always valid."""
    bench.CheckPrerequisites(None)  # Should not raise.

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      kubernetes_deployment_startup_boost_factor=2,
      cloud='GCP',
  )
  def testPrepareAppliesVpaForOptimized(self):
    """Prepare() applies VPA manifest for scenario=optimized."""
    bm = _MakeSpec()
    apply_calls = []
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest',
        side_effect=lambda *a, **kw: apply_calls.append(a[0])
    ):
      bench.Prepare(bm)

    # Should have applied both JVM manifest and VPA manifest.
    self.assertLen(apply_calls, 2)
    jvm_calls = [c for c in apply_calls if 'slowjvmstartup' in c]
    vpa_calls = [c for c in apply_calls if 'vpa' in c]
    self.assertLen(jvm_calls, 1)
    self.assertLen(vpa_calls, 1)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testPrepareSkipsVpaForBaseline(self):
    """Prepare() does NOT apply VPA manifest for scenario=baseline."""
    bm = _MakeSpec()
    apply_calls = []
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest',
        side_effect=lambda *a, **kw: apply_calls.append(a[0])
    ):
      bench.Prepare(bm)

    # Only JVM manifest, no VPA.
    self.assertLen(apply_calls, 1)
    self.assertNotIn('vpa', apply_calls[0])

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      kubernetes_deployment_startup_boost_factor=3,
      cloud='GCP',
  )
  def testBoostFactorInSampleMetadata(self):
    """boost_factor appears in sample metadata for optimized scenario."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ), mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ):
      samples = bench.Run(_MakeSpec())

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
    """boost_factor=1 in metadata for baseline scenario."""
    with mock.patch.object(
        bench.kubernetes_commands, 'ApplyManifest'
    ), mock.patch.object(
        bench.kubernetes_commands, 'WaitForRollout'
    ), mock.patch.object(
        bench.kubernetes_conditions,
        'GetStatusConditionsForResourceType',
        return_value=_DefaultConditions(),
    ), mock.patch.object(
        bench, '_GetTotalCpuMillicores', return_value=None
    ):
      samples = bench.Run(_MakeSpec())

    pod_samples = [s for s in samples if 'boost_factor' in s.metadata]
    for s in pod_samples:
      self.assertEqual(s.metadata['boost_factor'], 1)

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='optimized',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testGetConfigEnablesVpaForOptimized(self):
    """GetConfig() sets enable_vpa=True for scenario=optimized."""
    config = bench.GetConfig({})
    self.assertTrue(config['container_cluster'].get('enable_vpa'))

  @flagsaver.flagsaver(
      kubernetes_deployment_startup_scenario='baseline',
      kubernetes_deployment_startup_workload='jvm',
      cloud='GCP',
  )
  def testGetConfigNoVpaForBaseline(self):
    """GetConfig() does NOT set enable_vpa for scenario=baseline."""
    config = bench.GetConfig({})
    self.assertFalse(config['container_cluster'].get('enable_vpa', False))


if __name__ == '__main__':
  unittest.main()
