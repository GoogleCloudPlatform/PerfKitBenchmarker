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

# pylint: disable=g-import-not-at-top
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'


def _CreateSandbox():
  sandbox_spec = k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
      _COMPONENT,
      flag_values=FLAGS,
      type='Kubernetes',
  )
  return k8s_agent_sandbox.K8sAgentSandbox(sandbox_spec, mock.Mock())


class K8sAgentSandboxSpecTest(pkb_common_test_case.PkbCommonTestCase):

  def _Decode(self, **overrides):
    config = {'type': 'Kubernetes'}
    config.update(overrides)
    return k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec(
        _COMPONENT, flag_values=FLAGS, **config
    )

  def testFlagsOverrideConfig(self):
    FLAGS['agent_sandbox_manifest_ref'].parse('deadbeef')
    FLAGS['agent_sandbox_runtime_class'].parse('gvisor')
    FLAGS['agent_sandbox_warmpool_replicas'].parse(7)
    FLAGS['agent_sandbox_controller_claim_workers'].parse(12)
    spec = self._Decode()
    self.assertEqual(spec.manifest_ref, 'deadbeef')
    self.assertEqual(spec.sandbox_template.runtime_class, 'gvisor')
    self.assertEqual(spec.sandbox_warmpool.replicas, 7)
    self.assertEqual(spec.controller.claim_workers, 12)


class K8sAgentSandboxTest(pkb_common_test_case.PkbCommonTestCase):

  def testCreateCallsExpectedFunctions(self):
    sandbox = _CreateSandbox()

    mock_install_gvisor = self.enter_context(
        mock.patch.object(sandbox, '_InstallGvisor', autospec=True)
    )
    mock_install_crds_and_rbac = self.enter_context(
        mock.patch.object(sandbox, '_InstallCrdsAndRbac', autospec=True)
    )

    sandbox._Create()

    mock_install_gvisor.assert_called_once()
    mock_install_crds_and_rbac.assert_called_once()

  def testDeleteIsNoOp(self):
    sandbox = _CreateSandbox()
    self.assertIsNone(sandbox._Delete())

  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'ApplyYaml')
  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'WaitForRollout')
  def testInstallGvisorSchedulesOnPkbNodepool(self, mock_wait, mock_apply_yaml):
    sandbox = _CreateSandbox()
    node_selector, tolerations = sandbox._SandboxSchedulingInternal(
        'sandbox', 'sandbox.gke.io/runtime=runsc:NoSchedule'
    )
    sandbox._InstallGvisorImpl(node_selector, tolerations)

    mock_wait.assert_called_once()

    # Verify ApplyYaml was called with the injected manifests.
    manifests = mock_apply_yaml.call_args.args[0]

    daemonset = next(m for m in manifests if m.get('kind') == 'DaemonSet')
    pod_spec = daemonset['spec']['template']['spec']
    self.assertEqual(pod_spec['nodeSelector'], {'pkb_nodepool': 'sandbox'})
    self.assertEqual(pod_spec['tolerations'], tolerations)

  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'ApplyYaml')
  def testApplySandboxTemplateSchedulingAndRuntimeClass(self, mock_apply_yaml):
    template_spec = mock.Mock()
    template_spec.runtime_class = 'runsc'
    template_spec.image = 'img:latest'
    template_spec.cpu_request = '500m'
    template_spec.memory_request = '256Mi'
    template_spec.labels = {'sandbox': 'python-sandbox-bench'}
    sandbox = _CreateSandbox()
    node_selector, tolerations = sandbox._SandboxSchedulingInternal(
        'sandbox', 'sandbox.gke.io/runtime=runsc:NoSchedule'
    )
    sandbox._ApplySandboxTemplateImpl(template_spec, node_selector, tolerations)

    manifests = mock_apply_yaml.call_args.args[0]
    self.assertLen(manifests, 1)
    manifest = manifests[0]

    pod_spec = manifest['spec']['podTemplate']['spec']
    self.assertEqual(pod_spec['nodeSelector'], {'pkb_nodepool': 'sandbox'})
    self.assertEqual(pod_spec['tolerations'], tolerations)
    # runtimeClassName stays as runtime identity, not scheduling.
    self.assertEqual(pod_spec['runtimeClassName'], 'runsc')
    # The old runtime label is no longer used as a node selector.
    self.assertNotIn('sandbox.gke.io/runtime', pod_spec['nodeSelector'])


class K8sAgentSandboxDeleteTest(pkb_common_test_case.PkbCommonTestCase):

  def testTaintToTolerationWithValue(self):
    sandbox = _CreateSandbox()
    self.assertEqual(
        sandbox._TaintToToleration('sandbox.gke.io/runtime=runsc:NoSchedule'),
        {
            'key': 'sandbox.gke.io/runtime',
            'operator': 'Equal',
            'value': 'runsc',
            'effect': 'NoSchedule',
        },
    )

  def testTaintToTolerationNoValue(self):
    sandbox = _CreateSandbox()
    self.assertEqual(
        sandbox._TaintToToleration('dedicated:NoSchedule'),
        {'key': 'dedicated', 'operator': 'Exists', 'effect': 'NoSchedule'},
    )

  def testTaintToTolerationMalformedRaises(self):
    sandbox = _CreateSandbox()
    with self.assertRaises(ValueError):
      sandbox._TaintToToleration('no-effect')

  def testSandboxSchedulingSelectorAndToleration(self):
    sandbox = _CreateSandbox()
    node_selector, tolerations = sandbox._SandboxSchedulingInternal(
        'sandbox', 'sandbox.gke.io/runtime=runsc:NoSchedule'
    )
    self.assertEqual(node_selector, {'pkb_nodepool': 'sandbox'})
    self.assertEqual(
        tolerations,
        [{
            'key': 'sandbox.gke.io/runtime',
            'operator': 'Equal',
            'value': 'runsc',
            'effect': 'NoSchedule',
        }],
    )


if __name__ == '__main__':
  unittest.main()
