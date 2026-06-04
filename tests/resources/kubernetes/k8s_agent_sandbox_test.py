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
    FLAGS['agent_sandbox_controller_claim_workers'].parse(12)
    FLAGS['agent_sandbox_controller_leader_elect'].parse(True)
    spec = self._Decode()
    self.assertEqual(spec.manifest_ref, 'deadbeef')
    self.assertEqual(spec.sandbox_template.runtime_class, 'gvisor')
    self.assertEqual(spec.sandbox_warmpool.replicas, 7)
    self.assertEqual(spec.controller.claim_workers, 12)
    self.assertTrue(spec.controller.leader_elect)


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
    return k8s_agent_sandbox.K8sAgentSandbox(sandbox_spec, mock.Mock())

  @mock.patch.object(k8s_agent_sandbox, 'install_warmpool')
  @mock.patch.object(k8s_agent_sandbox, 'apply_template')
  @mock.patch.object(k8s_agent_sandbox, 'install_controller')
  @mock.patch.object(k8s_agent_sandbox, 'install_gvisor')
  def testCreateOrchestration(
      self, mock_gvisor, mock_controller, mock_template, mock_warmpool):
    sandbox = self._Sandbox()
    sandbox._Create()
    mock_gvisor.assert_called_once()
    mock_controller.assert_called_once()
    mock_template.assert_called_once()
    self.assertEqual(mock_template.call_args.args[0], 'agent-sandbox')
    self.assertIs(
        mock_template.call_args.args[1], sandbox.spec.sandbox_template)
    mock_warmpool.assert_called_once()
    self.assertEqual(
        mock_controller.call_args.kwargs['controller_ref'], 'ref123')
    self.assertEqual(mock_warmpool.call_args.args[-1], 3)

  def testDeleteIsNoOp(self):
    sandbox = self._Sandbox()
    self.assertIsNone(sandbox._Delete())


class AgentSandboxBenchmarkConfigTest(pkb_common_test_case.PkbCommonTestCase):

  def testConfigBuildsK8sAgentSandbox(self):
    from perfkitbenchmarker import configs
    from perfkitbenchmarker.linux_benchmarks import agent_sandbox_benchmark
    config = configs.LoadConfig(
        agent_sandbox_benchmark.BENCHMARK_CONFIG, {},
        agent_sandbox_benchmark.BENCHMARK_NAME)
    agent_sandbox_dict = config['container_cluster']['agent_sandbox']
    sandbox_spec = agent_sandbox_spec.AgentSandboxConfigDecoder(
        option='agent_sandbox').Decode(
            agent_sandbox_dict, 'test', FLAGS)
    sandbox = agent_sandbox.GetAgentSandbox(sandbox_spec, mock.Mock())
    self.assertIsInstance(sandbox, k8s_agent_sandbox.K8sAgentSandbox)


if __name__ == '__main__':
  unittest.main()
