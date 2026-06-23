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
"""Tests for the Kubernetes agent sandbox spec and resource."""

import unittest
from unittest import mock

import yaml
from absl import flags
from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'


class K8sAgentSandboxSpecTest(pkb_common_test_case.PkbCommonTestCase):

  def _Decode(self, **overrides):
    config = {'type': 'Kubernetes'}
    config.update(overrides)
    return k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
        _COMPONENT, flag_values=FLAGS, **config
    )

  def testDefaults(self):
    spec = self._Decode()
    self.assertEqual(spec.type, 'Kubernetes')
    self.assertEqual(spec.namespace, 'default')
    self.assertIsInstance(spec.controller, k8s_agent_sandbox_spec.ControllerSpec)
    self.assertIsInstance(
        spec.sandbox_template, k8s_agent_sandbox_spec.SandboxTemplateSpec)
    self.assertIsInstance(
        spec.sandbox_warmpool, k8s_agent_sandbox_spec.SandboxWarmPoolSpec)
    self.assertEqual(spec.sandbox_template.runtime_class, 'runsc')
    self.assertEqual(spec.sandbox_warmpool.replicas, 0)
    self.assertFalse(spec.controller.leader_elect)

  def testNestedOverrides(self):
    spec = self._Decode(
        manifest_ref='abc123',
        controller={'claim_workers': 8, 'leader_elect': True},
        sandbox_template={'runtime_class': 'gvisor', 'cpu_limit': '4'},
        sandbox_warmpool={'replicas': 5},
    )
    self.assertEqual(spec.manifest_ref, 'abc123')
    self.assertEqual(spec.controller.claim_workers, 8)
    self.assertTrue(spec.controller.leader_elect)
    self.assertEqual(spec.sandbox_template.runtime_class, 'gvisor')
    self.assertEqual(spec.sandbox_template.cpu_limit, '4')
    self.assertEqual(spec.sandbox_warmpool.replicas, 5)

  def testFlagsOverrideConfig(self):
    FLAGS['agent_sandbox_manifest_ref'].parse('deadbeef')
    FLAGS['agent_sandbox_runtime_class'].parse('gvisor')
    FLAGS['agent_sandbox_warmpool_replicas'].parse(7)
    spec = self._Decode()
    self.assertEqual(spec.manifest_ref, 'deadbeef')
    self.assertEqual(spec.sandbox_template.runtime_class, 'gvisor')
    self.assertEqual(spec.sandbox_warmpool.replicas, 7)


class ConfigureControllerManifestTest(pkb_common_test_case.PkbCommonTestCase):

  def _ManifestYaml(self):
    manifest = {
        'kind': 'Deployment',
        'spec': {'template': {'spec': {'containers': [{
            'name': 'manager',
            'image': 'placeholder',
            'args': ['--leader-elect=true', '--existing-arg'],
            'resources': {},
        }]}}},
    }
    return yaml.dump(manifest, default_flow_style=False)

  def testImageAndTuningInjected(self):
    result_yaml = k8s_agent_sandbox._configure_controller_manifest(
        self._ManifestYaml(),
        controller_image='my/image:tag',
        tuning={'claim_workers': 8, 'kube_api_qps': 50, 'leader_elect': True},
    )
    out = yaml.safe_load(result_yaml)
    container = out['spec']['template']['spec']['containers'][0]
    self.assertEqual(container['image'], 'my/image:tag')
    self.assertIn('--sandbox-claim-concurrent-workers=8', container['args'])
    self.assertIn('--kube-api-qps=50', container['args'])

  def testResourceDefaultsApplied(self):
    result_yaml = k8s_agent_sandbox._configure_controller_manifest(
        self._ManifestYaml(), controller_image='img', tuning={})
    out = yaml.safe_load(result_yaml)
    res = out['spec']['template']['spec']['containers'][0]['resources']
    self.assertEqual(
        res['requests']['cpu'], k8s_agent_sandbox._DEFAULT_CPU_REQUEST)
    self.assertEqual(
        res['limits']['memory'], k8s_agent_sandbox._DEFAULT_MEMORY_LIMIT)

  def testResourceTuningOverridesDefaults(self):
    result_yaml = k8s_agent_sandbox._configure_controller_manifest(
        self._ManifestYaml(),
        controller_image='img',
        tuning={'cpu_request': '1', 'memory_limit': '2Gi'},
    )
    out = yaml.safe_load(result_yaml)
    res = out['spec']['template']['spec']['containers'][0]['resources']
    self.assertEqual(res['requests']['cpu'], '1')
    self.assertEqual(res['limits']['memory'], '2Gi')


class K8sAgentSandboxCreateTest(pkb_common_test_case.PkbCommonTestCase):

  def _Sandbox(self, **template_overrides):
    sandbox_spec = k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
        _COMPONENT, flag_values=FLAGS,
        type='Kubernetes', manifest_ref='ref123',
        sandbox_warmpool={'replicas': 3},
        sandbox_template=template_overrides or {'runtime_class': 'runsc'},
    )
    cluster = mock.Mock()
    nodepool = mock.Mock()
    nodepool.name = 'sandbox'
    cluster.nodepools = {'sandbox': nodepool}
    return k8s_agent_sandbox.K8sAgentSandbox(sandbox_spec, cluster)

  @mock.patch.object(k8s_agent_sandbox, 'install_crds_and_rbac')
  @mock.patch.object(k8s_agent_sandbox, 'install_gvisor')
  def testCreateOrchestration(self, mock_gvisor, mock_crds_rbac):
    sandbox = self._Sandbox()
    sandbox._Create()
    mock_gvisor.assert_called_once()
    mock_crds_rbac.assert_called_once_with('ref123')

  @mock.patch.object(k8s_agent_sandbox, 'install_warmpool')
  @mock.patch.object(k8s_agent_sandbox, 'apply_template')
  @mock.patch.object(k8s_agent_sandbox, 'install_controller')
  def testInstallWorkloadOrchestration(
      self, mock_controller, mock_template, mock_warmpool):
    sandbox = self._Sandbox()
    sandbox.InstallWorkload()
    mock_controller.assert_called_once()
    self.assertEqual(
        mock_controller.call_args.kwargs['controller_ref'], 'ref123')
    mock_template.assert_called_once()
    self.assertIs(mock_template.call_args.args[0], sandbox.spec.sandbox_template)
    mock_warmpool.assert_called_once()
    self.assertEqual(mock_warmpool.call_args.args[-1], 3)

  def testRefreshSpecFromFlagsAppliesCurrentFlags(self):
    sandbox = self._Sandbox()
    FLAGS['agent_sandbox_warmpool_replicas'].parse(42)
    FLAGS['agent_sandbox_runtime_class'].parse('gvisor')
    sandbox.RefreshSpecFromFlags()
    self.assertEqual(sandbox.spec.sandbox_warmpool.replicas, 42)
    self.assertEqual(sandbox.spec.sandbox_template.runtime_class, 'gvisor')

  def testDeleteIsNoOp(self):
    sandbox = self._Sandbox()
    self.assertIsNone(sandbox._Delete())


class AgentSandboxRunTest(pkb_common_test_case.PkbCommonTestCase):

  def testRunUsesJobFlowAndParsesJsonl(self):
    from perfkitbenchmarker.linux_benchmarks import agent_sandbox_benchmark
    from perfkitbenchmarker.linux_benchmarks import agent_sandbox_loadgen
    from perfkitbenchmarker.linux_benchmarks import agent_sandbox_metrics
    from perfkitbenchmarker.resources.container_service import kubectl as kubectl_mod
    from perfkitbenchmarker.resources.container_service import kubernetes_commands
    import dataclasses
    import json

    sandbox_spec = k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
        _COMPONENT, flag_values=FLAGS, type='Kubernetes',
        namespace='sandboxes', sandbox_warmpool={'replicas': 4})
    sandbox = k8s_agent_sandbox.K8sAgentSandbox(sandbox_spec, mock.Mock())
    bm_spec = mock.Mock()
    bm_spec.agent_sandbox = sandbox
    bm_spec.container_cluster.GetResourceMetadata.return_value = {}

    rec = agent_sandbox_loadgen.ClaimRecord(
        name='claim-0', requested_at=100.0, ready_at=102.0,
        warm_served=False, released_at=103.0)
    rec_line = json.dumps(dataclasses.asdict(rec))
    summary_line = json.dumps({'summary': {'peak_concurrency': 3}})
    fake_logs = f'some startup output\n---RESULTS---\n{rec_line}\n{summary_line}\n'

    sentinel = [mock.sentinel.sample]
    with mock.patch.object(
        kubectl_mod, 'RunKubectlCommand',
        return_value=('pod-abc-123', '', 0)) as mock_kubectl, \
        mock.patch.object(
            kubernetes_commands, 'ApplyManifest') as mock_apply, \
        mock.patch.object(
            kubernetes_commands, 'WaitForResourceForMultiConditions',
            return_value='condition=Complete') as mock_wait, \
        mock.patch.object(
            agent_sandbox_metrics, 'build_samples',
            return_value=sentinel) as mock_build:
      # First RunKubectlCommand call is 'delete job'; second is 'get pods';
      # third is 'logs'. Override side_effect for logs call.
      kubectl_calls = [
          ('', '', 0),          # delete job --ignore-not-found
          ('pod-abc-123', '', 0),  # get pods -l job-name=...
          (fake_logs, '', 0),   # logs pod-abc-123
      ]
      mock_kubectl.side_effect = kubectl_calls
      result = agent_sandbox_benchmark.Run(bm_spec)

    self.assertIs(result, sentinel)
    mock_wait.assert_called_once()
    wait_kwargs = mock_wait.call_args
    actual_conditions = wait_kwargs.kwargs.get(
        'conditions', wait_kwargs.args[1] if len(wait_kwargs.args) > 1 else [])
    self.assertIn('condition=Complete', actual_conditions)
    self.assertIn('condition=Failed', actual_conditions)
    mock_apply.assert_called()
    apply_calls = [str(c) for c in mock_apply.call_args_list]
    self.assertTrue(
        any('load_runner_job' in c for c in apply_calls),
        f'Expected ApplyManifest call for load_runner_job manifest; got: {apply_calls}')
    mock_build.assert_called_once()
    build_args = mock_build.call_args
    parsed_records, peak, meta = build_args.args
    self.assertEqual(len(parsed_records), 1)
    self.assertEqual(parsed_records[0].name, 'claim-0')
    self.assertEqual(peak, 3)
    self.assertEqual(meta['warmpool_replicas'], 4)


class AgentSandboxBenchmarkConfigTest(pkb_common_test_case.PkbCommonTestCase):

  def testConfigBuildsK8sAgentSandbox(self):
    from perfkitbenchmarker import configs
    from perfkitbenchmarker.linux_benchmarks import agent_sandbox_benchmark
    config = configs.LoadConfig(
        agent_sandbox_benchmark.BENCHMARK_CONFIG, {},
        agent_sandbox_benchmark.BENCHMARK_NAME)
    agent_sandbox_dict = config['agent_sandbox']
    sandbox_spec = agent_sandbox_spec.AgentSandboxConfigDecoder(
        option='agent_sandbox').Decode(
            agent_sandbox_dict, 'test', FLAGS)
    sandbox = agent_sandbox.GetAgentSandbox(sandbox_spec, mock.Mock())
    self.assertIsInstance(sandbox, k8s_agent_sandbox.K8sAgentSandbox)


class SandboxSchedulingTest(pkb_common_test_case.PkbCommonTestCase):

  def testTaintToTolerationWithValue(self):
    self.assertEqual(
        k8s_agent_sandbox._taint_to_toleration(
            'sandbox.gke.io/runtime=runsc:NoSchedule'),
        {
            'key': 'sandbox.gke.io/runtime',
            'operator': 'Equal',
            'value': 'runsc',
            'effect': 'NoSchedule',
        })

  def testTaintToTolerationNoValue(self):
    self.assertEqual(
        k8s_agent_sandbox._taint_to_toleration('dedicated:NoSchedule'),
        {'key': 'dedicated', 'operator': 'Exists', 'effect': 'NoSchedule'})

  def testTaintToTolerationMalformedRaises(self):
    with self.assertRaises(ValueError):
      k8s_agent_sandbox._taint_to_toleration('no-effect')

  def testSandboxSchedulingSelectorAndToleration(self):
    node_selector, tolerations = k8s_agent_sandbox._sandbox_scheduling('sandbox')
    self.assertEqual(node_selector, {'pkb_nodepool': 'sandbox'})
    self.assertEqual(tolerations, [{
        'key': 'sandbox.gke.io/runtime',
        'operator': 'Equal',
        'value': 'runsc',
        'effect': 'NoSchedule',
    }])

  def testRenderGvisorDaemonsetSchedulesOnPkbNodepool(self):
    node_selector, tolerations = k8s_agent_sandbox._sandbox_scheduling('sandbox')
    manifest = yaml.safe_load(
        k8s_agent_sandbox._render_gvisor_daemonset(node_selector, tolerations))
    pod_spec = manifest['spec']['template']['spec']
    self.assertEqual(pod_spec['nodeSelector'], {'pkb_nodepool': 'sandbox'})
    self.assertEqual(pod_spec['tolerations'], tolerations)

  def testRenderTemplateManifestSchedulingAndRuntimeClass(self):
    template_spec = mock.Mock()
    template_spec.runtime_class = 'runsc'
    template_spec.image = 'img:latest'
    template_spec.cpu_request = '500m'
    template_spec.cpu_limit = '2'
    template_spec.memory_request = '256Mi'
    template_spec.memory_limit = '1Gi'
    template_spec.labels = {'sandbox': 'python-sandbox-bench'}
    node_selector, tolerations = k8s_agent_sandbox._sandbox_scheduling('sandbox')
    manifest = yaml.safe_load(
        k8s_agent_sandbox._render_template_manifest(
            template_spec, node_selector, tolerations))
    pod_spec = manifest['spec']['podTemplate']['spec']
    self.assertEqual(pod_spec['nodeSelector'], {'pkb_nodepool': 'sandbox'})
    self.assertEqual(pod_spec['tolerations'], tolerations)
    # runtimeClassName stays as runtime identity, not scheduling.
    self.assertEqual(pod_spec['runtimeClassName'], 'runsc')
    # The old runtime label is no longer used as a node selector.
    self.assertNotIn('sandbox.gke.io/runtime', pod_spec['nodeSelector'])


if __name__ == '__main__':
  unittest.main()
