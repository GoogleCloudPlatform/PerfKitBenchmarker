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


class K8sAgentSandboxDeleteTest(pkb_common_test_case.PkbCommonTestCase):

  def _Sandbox(self):
    sandbox_spec = k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
        _COMPONENT, flag_values=FLAGS,
        type='Kubernetes',
    )
    return k8s_agent_sandbox.K8sAgentSandbox(sandbox_spec, mock.Mock())

  def testCreateIsNoOp(self):
    sandbox = self._Sandbox()
    self.assertIsNone(sandbox._Create())

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
    agent_sandbox_dict = config['agent_sandbox']
    sandbox_spec = agent_sandbox_spec.AgentSandboxConfigDecoder(
        option='agent_sandbox').Decode(
            agent_sandbox_dict, 'test', FLAGS)
    sandbox = agent_sandbox.GetAgentSandbox(sandbox_spec, mock.Mock())
    self.assertIsInstance(sandbox, k8s_agent_sandbox.K8sAgentSandbox)


if __name__ == '__main__':
  unittest.main()
