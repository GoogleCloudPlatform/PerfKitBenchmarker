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


"""Tests for perfkitbenchmarker.linux_packages.agent_sandbox."""

import os
import unittest
from unittest import mock

import yaml

from perfkitbenchmarker import data
from perfkitbenchmarker.linux_packages import agent_sandbox
from tests import pkb_common_test_case

# Minimal controller.yaml (core file): one non-Deployment doc so the
# non-Deployment apply path runs.
_FAKE_CORE_YAML = yaml.dump({
    'apiVersion': 'v1',
    'kind': 'ServiceAccount',
    'metadata': {
        'name': 'agent-sandbox-controller',
        'namespace': 'agent-sandbox-system',
    },
})

# Minimal extensions.controller.yaml: a Deployment with the structure that
# _configure_controller_manifest expects.
_FAKE_CONTROLLER_YAML = yaml.dump({
    'apiVersion': 'apps/v1',
    'kind': 'Deployment',
    'metadata': {
        'name': 'agent-sandbox-controller',
        'namespace': 'agent-sandbox-system',
    },
    'spec': {
        'selector': {'matchLabels': {'app': 'agent-sandbox-controller'}},
        'template': {
            'metadata': {'labels': {'app': 'agent-sandbox-controller'}},
            'spec': {
                'containers': [{
                    'name': 'controller',
                    'image': 'ko://controller',
                    'args': ['--leader-elect=true'],
                }],
            },
        },
    },
})


def _fake_issue_command(cmd, **kwargs):
  """Returns canned YAML for the two curl calls in install_controller."""
  url = cmd[-1]
  if url.endswith(agent_sandbox._CONTROLLER_FILE):
    return (_FAKE_CONTROLLER_YAML, '', 0)
  return (_FAKE_CORE_YAML, '', 0)


class InstallStackTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.apply_manifest = self.enter_context(
        mock.patch.object(agent_sandbox.kubernetes_commands, 'ApplyManifest')
    )
    self.wait_resource = self.enter_context(
        mock.patch.object(agent_sandbox.kubernetes_commands, 'WaitForResource')
    )
    self.wait_rollout = self.enter_context(
        mock.patch.object(agent_sandbox.kubernetes_commands, 'WaitForRollout')
    )
    self.apply_url = self.enter_context(
        mock.patch.object(agent_sandbox, '_apply_url')
    )
    self.apply_yaml = self.enter_context(
        mock.patch.object(agent_sandbox, '_apply_yaml')
    )
    self.wait_warmpool = self.enter_context(
        mock.patch.object(agent_sandbox, '_wait_warmpool_ready')
    )
    self.create_configmap = self.enter_context(
        mock.patch.object(agent_sandbox, '_create_installer_configmap')
    )
    self.run_kubectl = self.enter_context(
        mock.patch.object(
            agent_sandbox.kubectl, 'RunKubectlCommand', return_value=('', '', 0)
        )
    )
    self.enter_context(
        mock.patch.object(
            agent_sandbox.vm_util,
            'IssueCommand',
            side_effect=_fake_issue_command,
        )
    )
    self.enter_context(
        mock.patch.object(
            agent_sandbox.data, 'ResourcePath', side_effect=lambda p: p
        )
    )

  def testInstallStackOrdersCrdsBeforeController(self):
    # Track the global call order across _apply_url and _apply_yaml.
    call_log = []
    self.apply_url.side_effect = lambda url: call_log.append(('url', url))
    self.apply_yaml.side_effect = lambda s: call_log.append(('yaml', s))

    agent_sandbox.install_stack(
        controller_ref='v0.4.6',
        controller_image='example.com/controller:v0.4.6',
        runtime_class='runsc',
        template_name='python-runtime',
        warmpool_name='default',
        warmpool_replicas=10,
    )

    # Find last CRD _apply_url index and first controller _apply_yaml index.
    crd_index = max(
        i
        for i, (kind, val) in enumerate(call_log)
        if kind == 'url' and 'sandboxclaims' in val
    )
    # The controller Deployment yaml contains 'agent-sandbox-controller'.
    controller_index = next(
        i
        for i, (kind, val) in enumerate(call_log)
        if kind == 'yaml'
        and 'agent-sandbox-controller' in val
        and 'Deployment' in val
    )
    self.assertLess(crd_index, controller_index)

  def testInstallStackScalesWarmpoolToRequestedReplicas(self):
    agent_sandbox.install_stack(
        controller_ref='v0.4.6',
        controller_image='example.com/controller:v0.4.6',
        runtime_class='runsc',
        template_name='python-runtime',
        warmpool_name='default',
        warmpool_replicas=10,
    )

    warmpool_calls = [
        c
        for c in self.apply_manifest.call_args_list
        if 'sandbox-warmpool' in c.args[0]
    ]
    self.assertTrue(warmpool_calls)
    self.assertEqual(warmpool_calls[0].kwargs['replicas'], 10)
    self.wait_warmpool.assert_called_once_with('default', 10)

  def testInstallStackSkipsWarmpoolAtZeroReplicas(self):
    """install_stack with warmpool_replicas=0 must not apply or wait."""
    agent_sandbox.install_stack(
        controller_ref='v0.4.6',
        controller_image=None,
        runtime_class='runsc',
        template_name='python-runtime',
        warmpool_name='default',
        warmpool_replicas=0,
    )

    warmpool_calls = [
        c
        for c in self.apply_manifest.call_args_list
        if 'sandbox-warmpool' in c.args[0]
    ]
    self.assertFalse(warmpool_calls, 'warmpool manifest should not be applied')
    self.wait_warmpool.assert_not_called()

  def testInstallControllerPatchesImageWhenProvided(self):
    """Injects the controller image into the Deployment manifest in memory."""
    agent_sandbox.install_controller(
        controller_ref='abc123',
        controller_image='example.com/controller:abc123',
    )

    # The controller Deployment is applied via _apply_yaml; verify the image
    # was injected into the YAML that was passed.
    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    self.assertEqual(container['image'], 'example.com/controller:abc123')

  def testInstallControllerDoesNotPatchImageWhenImageIsNone(self):
    """Does not replace the image in the manifest when controller_image is None."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6', controller_image=None
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    # When controller_image is None, the image field is left as-is from the
    # fake manifest (the placeholder value).
    self.assertEqual(
        container['image'],
        'ko://controller',
        'image should not be overwritten when controller_image is None',
    )

  def testControllerTuningPatchesArgs(self):
    """install_controller with tuning injects the args into the Deployment YAML."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6',
        controller_image=None,
        controller_tuning={
            'claim_workers': 100,
            'sandbox_workers': 70,
            'kube_api_burst': 600,
        },
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    args = container.get('args', [])
    self.assertIn('--sandbox-claim-concurrent-workers=100', args)
    self.assertIn('--sandbox-concurrent-workers=70', args)
    self.assertIn('--kube-api-burst=600', args)

  def testControllerTuningSetsOtelEnvWhenTracing(self):
    """enable_tracing=True injects --enable-tracing=true and OTEL env in YAML."""
    endpoint = 'http://otel-collector.observability:4317'
    agent_sandbox.install_controller(
        controller_ref='v0.4.6',
        controller_image=None,
        controller_tuning={
            'enable_tracing': True,
            'otel_endpoint': endpoint,
        },
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]

    # --enable-tracing=true must be in the args list.
    args = container.get('args', [])
    self.assertIn('--enable-tracing=true', args)

    # OTEL env vars must be injected into the container env.
    env = {e['name']: e['value'] for e in container.get('env', [])}
    self.assertEqual(env.get('OTEL_EXPORTER_OTLP_ENDPOINT'), endpoint)
    self.assertEqual(env.get('OTEL_EXPORTER_OTLP_INSECURE'), 'true')

  def testNoControllerTuningNoExtraKubectl(self):
    """install_controller with no tuning applies a clean Deployment with no extras."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6', controller_image=None, controller_tuning=None
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]

    # No OTEL env vars when tracing is not enabled.
    env = {e['name']: e['value'] for e in container.get('env', [])}
    self.assertNotIn(
        'OTEL_EXPORTER_OTLP_ENDPOINT',
        env,
        'no OTEL env expected with no tuning',
    )
    # No extra tuning args beyond leader-elect and resources (which are always set).
    args = container.get('args', [])
    extra_tuning_args = [
        a
        for a in args
        if a.startswith('--sandbox-')
        or a.startswith('--kube-api-')
        or a.startswith('--enable-tracing')
    ]
    self.assertFalse(
        extra_tuning_args, 'no extra tuning args expected with no tuning'
    )

  def testInstallControllerDisablesLeaderElectionByDefault(self):
    """install_controller sets --leader-elect=false in the manifest by default."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6', controller_image=None, controller_tuning=None
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    args = container.get('args', [])
    le_args = [a for a in args if a.startswith('--leader-elect')]
    self.assertLen(
        le_args,
        1,
        'expected exactly one --leader-elect flag to avoid duplicates',
    )
    self.assertEqual(le_args[0], '--leader-elect=false')

  def testInstallControllerEnablesLeaderElectionWhenRequested(self):
    """install_controller sets --leader-elect=true when leader_elect=True."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6',
        controller_image=None,
        controller_tuning={'leader_elect': True},
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    args = container.get('args', [])
    le_args = [a for a in args if a.startswith('--leader-elect')]
    self.assertLen(
        le_args,
        1,
        'expected exactly one --leader-elect flag to avoid duplicates',
    )
    self.assertEqual(le_args[0], '--leader-elect=true')

  def testInstallControllerSetsResourceRequests(self):
    """install_controller injects default resource requests/limits into the YAML."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6', controller_image=None, controller_tuning=None
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    resources = container.get('resources', {})
    self.assertEqual(resources['requests']['cpu'], '500m')
    self.assertEqual(resources['requests']['memory'], '256Mi')
    self.assertEqual(resources['limits']['cpu'], '2')
    self.assertEqual(resources['limits']['memory'], '1Gi')

  def testInstallControllerUsesOverriddenResourceValues(self):
    """Resource values in controller_tuning override the defaults in the YAML."""
    agent_sandbox.install_controller(
        controller_ref='v0.4.6',
        controller_image=None,
        controller_tuning={
            'cpu_request': '1',
            'cpu_limit': '4',
            'memory_request': '512Mi',
            'memory_limit': '2Gi',
        },
    )

    controller_yamls = [
        yaml.safe_load(c.args[0])
        for c in self.apply_yaml.call_args_list
        if c.args[0] and yaml.safe_load(c.args[0]).get('kind') == 'Deployment'
    ]
    self.assertTrue(controller_yamls, 'expected a Deployment to be applied')
    container = controller_yamls[0]['spec']['template']['spec']['containers'][0]
    resources = container.get('resources', {})
    self.assertEqual(resources['requests']['cpu'], '1')
    self.assertEqual(resources['requests']['memory'], '512Mi')
    self.assertEqual(resources['limits']['cpu'], '4')
    self.assertEqual(resources['limits']['memory'], '2Gi')


class DaemonSetManifestTest(pkb_common_test_case.PkbCommonTestCase):
  """Verifies daemonset.yaml scheduling matches the BENCHMARK_CONFIG labels."""

  def _load_daemonset(self):
    path = data.ResourcePath('agent_sandbox/gvisor-installer/daemonset.yaml')
    with open(path) as f:
      return yaml.safe_load(f)

  def testNodeSelectorMatchesBenchmarkLabel(self):
    """DaemonSet nodeSelector must match the sandbox nodepool label key."""
    ds = self._load_daemonset()
    node_selector = ds['spec']['template']['spec']['nodeSelector']
    self.assertIn(
        'sandbox.gke.io/runtime',
        node_selector,
        'DaemonSet nodeSelector must use sandbox.gke.io/runtime (the key '
        'set on the sandbox nodepool in BENCHMARK_CONFIG)',
    )

  def testTolerationMatchesBenchmarkTaint(self):
    """DaemonSet must tolerate the NoSchedule taint on the sandbox nodepool."""
    ds = self._load_daemonset()
    tolerations = ds['spec']['template']['spec']['tolerations']
    taint_keys = [t.get('key') for t in tolerations]
    self.assertIn(
        'sandbox.gke.io/runtime',
        taint_keys,
        'DaemonSet must tolerate sandbox.gke.io/runtime=runsc:NoSchedule',
    )
    runsc_tol = next(
        t for t in tolerations if t.get('key') == 'sandbox.gke.io/runtime'
    )
    self.assertEqual(runsc_tol.get('value'), 'runsc')
    self.assertEqual(runsc_tol.get('effect'), 'NoSchedule')


class DataFilesTest(pkb_common_test_case.PkbCommonTestCase):

  def testTemplateAndWarmpoolResolve(self):
    for resource in (
        'agent_sandbox/sandbox-template.yaml.j2',
        'agent_sandbox/sandbox-warmpool.yaml.j2',
        'agent_sandbox/gvisor-installer/daemonset.yaml',
        'agent_sandbox/gvisor-installer/runtimeclass.yaml',
        'agent_sandbox/gvisor-installer/install.sh',
    ):
      self.assertTrue(data.ResourcePath(resource))

  def testInstallScriptIsPresent(self):
    path = data.ResourcePath('agent_sandbox/gvisor-installer/install.sh')
    self.assertTrue(os.path.isfile(path))
    with open(path) as f:
      content = f.read()
    self.assertIn('GVISOR_VERSION', content)
    self.assertIn('containerd-shim-runsc-v1', content)


if __name__ == '__main__':
  unittest.main()
