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

import os
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec
from tests import pkb_common_test_case
import yaml

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
    assert spec.sandbox_template is not None
    self.assertEqual(spec.sandbox_template.runtime_class, 'gvisor')
    assert spec.sandbox_warmpool is not None
    self.assertEqual(spec.sandbox_warmpool.replicas, 7)
    assert spec.controller is not None
    self.assertEqual(spec.controller.claim_workers, 12)

  def testExplicitNoneSubSpecsFallBackToDefaults(self):
    spec = self._Decode(
        controller=None,
        sandbox_template=None,
        sandbox_warmpool=None,
    )
    self.assertIsNotNone(spec.controller)
    self.assertIsNotNone(spec.sandbox_template)
    self.assertIsNotNone(spec.sandbox_warmpool)


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
    sandbox.spec.namespace = 'custom-ns'
    node_selector, tolerations = sandbox._SandboxSchedulingInternal(
        'sandbox', 'sandbox.gke.io/runtime=runsc:NoSchedule'
    )
    sandbox._ApplySandboxTemplateImpl(template_spec, node_selector, tolerations)

    manifests = mock_apply_yaml.call_args.args[0]
    self.assertLen(manifests, 1)
    manifest = manifests[0]

    self.assertEqual(manifest['metadata']['namespace'], 'custom-ns')
    pod_spec = manifest['spec']['podTemplate']['spec']
    self.assertEqual(pod_spec['nodeSelector'], {'pkb_nodepool': 'sandbox'})
    self.assertEqual(pod_spec['tolerations'], tolerations)
    # runtimeClassName stays as runtime identity, not scheduling.
    self.assertEqual(pod_spec['runtimeClassName'], 'runsc')
    # The old runtime label is no longer used as a node selector.
    self.assertNotIn('sandbox.gke.io/runtime', pod_spec['nodeSelector'])
    resources = pod_spec['containers'][0]['resources']
    self.assertEqual(resources['requests']['ephemeral-storage'], '256Mi')
    self.assertEqual(resources['limits']['ephemeral-storage'], '256Mi')

  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'WaitForResource')
  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'ApplyYaml')
  def testInstallWarmpoolUsesSpecNamespace(self, mock_apply_yaml, mock_wait):
    sandbox = _CreateSandbox()
    sandbox.spec.namespace = 'custom-ns'
    sandbox._InstallWarmpoolImpl(replicas=5)

    manifests = mock_apply_yaml.call_args.args[0]
    self.assertLen(manifests, 1)
    self.assertEqual(manifests[0]['metadata']['namespace'], 'custom-ns')
    mock_wait.assert_called_once_with(
        'sandboxwarmpool/agent-sandbox',
        'jsonpath={.status.readyReplicas}=5',
        namespace='custom-ns',
        condition_type='',
        timeout=600,
    )

  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'WaitForResource')
  @mock.patch.object(k8s_agent_sandbox.kubectl, 'RunKubectlCommand')
  def testInstallCrdsSkipsKustomization(self, mock_kubectl, mock_wait):
    sandbox = _CreateSandbox()
    repo_dir = self.create_tempdir().full_path
    crds_dir = os.path.join(repo_dir, 'k8s', 'crds')
    os.makedirs(crds_dir)
    with open(os.path.join(crds_dir, 'kustomization.yaml'), 'w') as f:
      f.write('resources: []\n')
    crd_path = os.path.join(crds_dir, 'agents.x-k8s.io_sandboxes.yaml')
    with open(crd_path, 'w') as f:
      f.write('kind: CustomResourceDefinition\n')

    sandbox._InstallCrds(repo_dir)

    mock_kubectl.assert_called_once_with(['apply', '-f', crd_path])
    mock_wait.assert_called_once_with(
        'crd/sandboxes.agents.x-k8s.io', 'established'
    )

  @mock.patch.object(k8s_agent_sandbox.vm_util, 'IssueCommand')
  def testDownloadManifestsSupportsCommitShaOrBranch(self, mock_issue):
    sandbox = _CreateSandbox()
    dest_dir = self.create_tempdir().full_path
    os.makedirs(os.path.join(dest_dir, 'agent-sandbox-deadbeef'))

    result = sandbox._DownloadManifests(
        'https://raw.githubusercontent.com/kubernetes-sigs/agent-sandbox',
        'deadbeef',
        dest_dir,
    )

    self.assertEqual(result, os.path.join(dest_dir, 'agent-sandbox-deadbeef'))
    curl_cmd = mock_issue.call_args_list[0].args[0]
    self.assertIn(
        'https://github.com/kubernetes-sigs/agent-sandbox/archive/deadbeef.tar.gz',
        curl_cmd,
    )

  def testGetResourceMetadata(self):
    sandbox = _CreateSandbox()
    metadata = sandbox.GetResourceMetadata()
    self.assertEqual(metadata['agent_sandbox_manifest_ref'], 'v1.0.4')
    self.assertEqual(metadata['agent_sandbox_namespace'], 'default')
    self.assertEqual(metadata['agent_sandbox_nodepool'], 'sandbox')
    self.assertEqual(
        metadata['agent_sandbox_node_taint'],
        'sandbox.gke.io/runtime=runsc:NoSchedule',
    )
    self.assertIn('agent_sandbox_controller_image', metadata)
    self.assertEqual(metadata['agent_sandbox_controller_claim_workers'], 100)
    self.assertEqual(metadata['agent_sandbox_runtime_class'], 'runsc')
    self.assertEqual(metadata['agent_sandbox_warmpool_replicas'], 150)

  def testSandboxSchedulingUsesSpecTaint(self):
    sandbox = _CreateSandbox()
    sandbox.spec.sandbox_node_taint = 'my-taint:NoSchedule'
    mock_nodepool = mock.Mock()
    mock_nodepool.name = 'my-nodepool'
    sandbox.cluster.nodepools.get.return_value = mock_nodepool

    node_selector, tolerations = sandbox._SandboxScheduling()
    self.assertEqual(node_selector, {'pkb_nodepool': 'my-nodepool'})
    self.assertEqual(
        tolerations,
        [{'key': 'my-taint', 'operator': 'Exists', 'effect': 'NoSchedule'}],
    )

  @mock.patch.object(k8s_agent_sandbox.kubernetes_commands, 'WaitForRollout')
  @mock.patch.object(k8s_agent_sandbox.kubectl, 'RunKubectlCommand')
  def testInstallControllerAndRbacSkipsKustomization(
      self, mock_kubectl, mock_wait
  ):
    sandbox = _CreateSandbox()
    repo_dir = self.create_tempdir().full_path
    k8s_dir = os.path.join(repo_dir, 'k8s')
    os.makedirs(k8s_dir)
    with open(os.path.join(k8s_dir, 'kustomization.yaml'), 'w') as f:
      f.write(
          'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\n'
      )
    with open(
        os.path.join(k8s_dir, 'extensions.controller.yaml'),
        'w',
    ) as f:
      f.write(
          'apiVersion: apps/v1\nkind: Deployment\n'
          'metadata:\n  name: agent-sandbox-controller\n'
          'spec:\n  template:\n    spec:\n      containers:\n'
          '      - name: agent-sandbox-controller\n        image: ko://test\n'
      )
    with open(
        os.path.join(k8s_dir, 'auxiliary.yaml'),
        'w',
    ) as f:
      f.write(
          'apiVersion: apps/v1\nkind: Deployment\n'
          'metadata:\n  name: auxiliary-router\n'
          'spec:\n  template:\n    spec:\n      containers:\n'
          '      - name: router\n        image: router:v1\n'
      )

    applied_docs = []

    def _CaptureApply(args):
      with open(args[2]) as manifest_file:
        applied_docs.extend(yaml.safe_load_all(manifest_file))

    mock_kubectl.side_effect = _CaptureApply
    assert sandbox.spec.controller is not None
    sandbox._InstallControllerAndRbac(repo_dir, sandbox.spec.controller)

    mock_kubectl.assert_called_once()
    mock_wait.assert_called_once()
    self.assertLen(applied_docs, 2)
    by_name = {d['metadata']['name']: d for d in applied_docs}
    aux_container = by_name['auxiliary-router']['spec']['template']['spec'][
        'containers'
    ][0]
    self.assertEqual(aux_container['image'], 'router:v1')
    self.assertNotIn('args', aux_container)


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
