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
"""Kubernetes implementation of an agent sandbox.

Installs the open-source kubernetes-sigs/agent-sandbox stack (CRDs, RBAC,
controller, gVisor runtime class, SandboxTemplate, and SandboxWarmPool).
"""

# pylint: disable=g-bad-todo,g-doc-args,g-doc-return-or-yield,invalid-name,g-import-not-at-top,line-too-long
import os
import tempfile
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec
import yaml

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'agent_sandbox_manifest_ref',
    'v0.5.0',
    'The git reference to the kubernetes-sigs/agent-sandbox manifest repo.',
)
flags.DEFINE_string(
    'agent_sandbox_runtime_class',
    None,
    'The runtime class name to use for sandboxes.',
)
flags.DEFINE_integer(
    'agent_sandbox_warmpool_replicas',
    None,
    'Number of warm sandboxes to maintain.',
)
flags.DEFINE_integer(
    'agent_sandbox_controller_claim_workers',
    None,
    'Number of workers for processing claims.',
)
SANDBOX_NAME = 'agent-sandbox'


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def __init__(self, spec, cluster):
    super().__init__(spec, cluster)
    self._manifests_dir = None

  def _CreateDependencies(self) -> None:
    """Download manifests before creation."""
    self._GetManifestsDir()

  def _Create(self) -> None:
    """Provision-stage install: gVisor, CRDs, and RBAC."""
    self._InstallGvisor()
    self._InstallCrdsAndRbac()

  def InstallWorkload(self) -> None:
    """Prepare-stage install: controller, sandbox template, and warm pool."""
    self._InstallController()
    self._ApplySandboxTemplate()
    self._InstallWarmpool()

  def _Delete(self) -> None:
    """No-op: the ephemeral cluster teardown reclaims the sandbox stack."""
    pass

  def _GetManifestsDir(self) -> str:
    if not self._manifests_dir:
      tmp_dir = vm_util.PrependTempDir('agent_sandbox_manifests')
      self._manifests_dir = self._DownloadManifests(
          self.spec.manifest_repo, self.spec.manifest_ref, tmp_dir
      )
    return self._manifests_dir

  def _DownloadManifests(
      self, manifest_repo: str, controller_ref: str, dest_dir: str
  ) -> str:
    """Downloads the upstream agent-sandbox manifests to a temporary directory."""
    if not manifest_repo.startswith('http'):
      return manifest_repo

    os.makedirs(dest_dir, exist_ok=True)
    repo = manifest_repo.replace(
        'https://raw.githubusercontent.com', 'https://github.com'
    )
    tarball_url = f'{repo}/archive/refs/tags/{controller_ref}.tar.gz'
    tarball_path = os.path.join(dest_dir, 'repo.tar.gz')
    vm_util.IssueCommand(
        ['curl', '--create-dirs', '-fsSL', tarball_url, '-o', tarball_path],
        raise_on_failure=True,
    )
    vm_util.IssueCommand(
        ['tar', '-xzf', tarball_path, '-C', dest_dir],
        raise_on_failure=True,
    )
    extracted_dirs = [
        d
        for d in os.listdir(dest_dir)
        if os.path.isdir(os.path.join(dest_dir, d))
    ]
    return os.path.join(dest_dir, extracted_dirs[0])

  def _CrdName(self, filename: str) -> str:
    """Derives a CRD resource name from its manifest filename.

    Example: 'crds/agents.x-k8s.io_sandboxes.yaml' ->
    'sandboxes.agents.x-k8s.io'.
    """
    base = filename.split('/')[-1].removesuffix('.yaml')
    group, plural = base.rsplit('_', 1)
    return f'{plural}.{group}'

  def _TaintToToleration(self, taint: str) -> dict[str, str]:
    """Converts a 'key=value:Effect' or 'key:Effect' taint to a toleration dict.

    Parsing mirrors the planned EKS taint parser (PR #6744) but emits a
    Kubernetes toleration so the node taint and the pod toleration stay
    symmetric.
    """
    spec, sep_effect, effect = taint.rpartition(':')
    if not sep_effect:
      raise ValueError(
          f'Malformed taint, expected key[=value]:Effect: {taint!r}'
      )
    key, sep_val, value = spec.partition('=')
    toleration = {'key': key}
    if sep_val:
      toleration['operator'] = 'Equal'
      toleration['value'] = value
    else:
      toleration['operator'] = 'Exists'
    toleration['effect'] = effect
    return toleration

  def _ConfigureControllerManifest(
      self,
      manifest: dict[str, Any],
      controller_spec: k8s_agent_sandbox_spec.ControllerSpec,
  ) -> dict[str, Any]:
    """Injects image and all tuning into a controller Deployment manifest dict."""
    container = manifest['spec']['template']['spec']['containers'][0]

    if controller_spec.image:
      container['image'] = controller_spec.image

    if any([
        controller_spec.cpu_request,
        controller_spec.memory_request,
    ]):
      resources = container.setdefault('resources', {})
      if controller_spec.cpu_request or controller_spec.memory_request:
        requests = resources.setdefault('requests', {})
        if controller_spec.cpu_request:
          requests['cpu'] = controller_spec.cpu_request
        if controller_spec.memory_request:
          requests['memory'] = controller_spec.memory_request
        limits = resources.setdefault('limits', {})
        if controller_spec.cpu_request:
          limits['cpu'] = controller_spec.cpu_request
        if controller_spec.memory_request:
          limits['memory'] = controller_spec.memory_request

    args = container.setdefault('args', [])

    if controller_spec.claim_workers is not None:
      args.append(
          f'--sandbox-claim-concurrent-workers={controller_spec.claim_workers}'
      )
    if controller_spec.sandbox_workers is not None:
      args.append(
          f'--sandbox-concurrent-workers={controller_spec.sandbox_workers}'
      )
    if controller_spec.warmpool_workers is not None:
      args.append(
          f'--sandbox-warm-pool-concurrent-workers={controller_spec.warmpool_workers}'
      )
    if controller_spec.warmpool_max_batch_size is not None:
      args.append(
          f'--sandbox-warm-pool-max-batch-size={controller_spec.warmpool_max_batch_size}'
      )
    if controller_spec.kube_api_burst is not None:
      args.append(f'--kube-api-burst={controller_spec.kube_api_burst}')
    if controller_spec.kube_api_qps is not None:
      args.append(f'--kube-api-qps={controller_spec.kube_api_qps}')
    return manifest

  def _InstallGvisor(self) -> None:
    node_selector, tolerations = self._SandboxScheduling()
    self._InstallGvisorImpl(node_selector, tolerations)

  def _InstallGvisorImpl(
      self, node_selector: dict[str, str], tolerations: list[dict[str, str]]
  ) -> None:
    """Installs gVisor onto cluster nodes via the installer DaemonSet."""
    installer_dir = data.ResourcePath('agent_sandbox/gvisor-installer')
    install_script_path = os.path.join(installer_dir, 'install.sh')
    install_script_content = ''
    if os.path.exists(install_script_path):
      with open(install_script_path) as f:
        install_script_content = f.read()

    manifests = []
    for filename in os.listdir(installer_dir):
      if not filename.endswith('.yaml'):
        continue
      path = os.path.join(installer_dir, filename)
      with open(path) as f:
        for manifest in yaml.safe_load_all(f):
          if not manifest:
            continue
          if manifest.get('kind') == 'DaemonSet':
            manifest['spec']['template']['spec']['nodeSelector'] = node_selector
            manifest['spec']['template']['spec']['tolerations'] = tolerations
          elif (
              manifest.get('kind') == 'ConfigMap'
              and manifest.get('metadata', {}).get('name')
              == 'gvisor-installer-script'
          ):
            if install_script_content:
              manifest.setdefault('data', {})[
                  'install.sh'
              ] = install_script_content
          manifests.append(manifest)

    kubernetes_commands.ApplyYaml(manifests)
    kubernetes_commands.WaitForRollout(
        'daemonset/gvisor-installer', namespace='kube-system'
    )

  def _InstallCrdsAndRbac(self) -> None:
    self._InstallCrds(self._GetManifestsDir())

  def _InstallCrds(self, repo_dir: str) -> None:
    """Applies the agent-sandbox CRDs."""
    crds_dir = os.path.join(repo_dir, 'k8s', 'crds')
    kubectl.RunKubectlCommand(['apply', '-f', crds_dir])
    for filename in os.listdir(crds_dir):
      if filename.endswith('.yaml'):
        kubernetes_commands.WaitForResource(
            f'crd/{self._CrdName(filename)}', 'established'
        )

  def _InstallController(self) -> None:
    self._InstallControllerAndRbac(
        self._GetManifestsDir(), self.spec.controller
    )

  def _InstallControllerAndRbac(
      self,
      repo_dir: str,
      controller_spec: k8s_agent_sandbox_spec.ControllerSpec,
  ) -> None:
    """Installs the controller, extensions, and all cluster-scoped RBAC."""
    k8s_dir = os.path.join(repo_dir, 'k8s')
    all_docs = []

    for root, _, files in os.walk(k8s_dir):
      for filename in files:
        if not filename.endswith('.yaml'):
          continue
        path = os.path.join(root, filename)
        with open(path) as f:
          for doc in yaml.safe_load_all(f):
            if not doc:
              continue

            # Skip the core controller deployment because we want the extensions one
            if (
                doc.get('kind') == 'Deployment'
                and doc.get('metadata', {}).get('name')
                == 'agent-sandbox-controller'
                and not filename.startswith('extensions')
            ):
              continue

            if doc.get('kind') == 'Deployment':
              doc = self._ConfigureControllerManifest(doc, controller_spec)

            all_docs.append(doc)

    # Sort so Namespace and CRDs are created before RoleBindings and Deployments
    def sort_key(doc):
      kind = doc.get('kind')
      if kind == 'Namespace':
        return 0
      if kind == 'CustomResourceDefinition':
        return 1
      if kind == 'ServiceAccount':
        return 2
      if 'Role' in kind:
        return 3
      return 4

    all_docs.sort(key=sort_key)
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.yaml', delete=False
    ) as f:
      yaml.dump_all(all_docs, f)
      manifest_path = f.name

    kubectl.RunKubectlCommand(['apply', '-f', manifest_path])
    kubernetes_commands.WaitForRollout(
        'deployment/agent-sandbox-controller', namespace='agent-sandbox-system'
    )

  def _ApplySandboxTemplate(self) -> None:
    node_selector, tolerations = self._SandboxScheduling()
    self._ApplySandboxTemplateImpl(
        self.spec.sandbox_template, node_selector, tolerations
    )

  def _ApplySandboxTemplateImpl(
      self,
      template_spec: k8s_agent_sandbox_spec.SandboxTemplateSpec,
      node_selector: dict[str, str],
      tolerations: list[dict[str, str]],
  ) -> None:
    """Applies the SandboxTemplate rendered from the template spec."""
    labels = template_spec.labels or {'sandbox': 'python-sandbox-bench'}
    pod_spec = {
        'automountServiceAccountToken': False,
        'securityContext': {'runAsNonRoot': True},
        'containers': [{
            'name': 'python-runtime',
            'image': template_spec.image,
            'ports': [{'containerPort': 8888}],
            'readinessProbe': {
                'httpGet': {'path': '/', 'port': 8888},
                'initialDelaySeconds': 0,
                'periodSeconds': 1,
            },
            'securityContext': {'capabilities': {'drop': ['ALL']}},
            'resources': {
                'requests': {
                    'cpu': template_spec.cpu_request,
                    'memory': template_spec.memory_request,
                    'ephemeral-storage': '256Mi',
                },
                'limits': {
                    'cpu': template_spec.cpu_request,
                    'memory': template_spec.memory_request,
                },
            },
        }],
        'restartPolicy': 'OnFailure',
        'nodeSelector': node_selector,
        'tolerations': tolerations,
    }
    if template_spec.runtime_class:
      pod_spec['runtimeClassName'] = template_spec.runtime_class

    manifest = {
        'apiVersion': 'extensions.agents.x-k8s.io/v1beta1',
        'kind': 'SandboxTemplate',
        'metadata': {'name': SANDBOX_NAME},
        'spec': {
            'podTemplate': {'metadata': {'labels': labels}, 'spec': pod_spec}
        },
    }
    kubernetes_commands.ApplyYaml([manifest])

  def _SandboxSchedulingInternal(
      self, nodepool_name: str, taint: str | None
  ) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Returns (node_selector, tolerations) targeting the sandbox nodepool."""
    node_selector = {'pkb_nodepool': nodepool_name}
    tolerations = [self._TaintToToleration(taint)] if taint else []
    return node_selector, tolerations

  def _SandboxScheduling(self) -> tuple[dict[str, str], list[dict[str, str]]]:
    """Resolves (node_selector, tolerations) from the sandbox nodepool."""
    nodepool = self.cluster.nodepools.get(self.spec.sandbox_nodepool)
    if nodepool is None:
      raise ValueError(
          'Agent sandbox requires a nodepool named'
          f' {self.spec.sandbox_nodepool!r}; add it to the benchmark'
          ' container_cluster nodepools config.'
      )
    return self._SandboxSchedulingInternal(
        nodepool.name, 'sandbox.gke.io/runtime=runsc:NoSchedule'
    )

  def _InstallWarmpool(self) -> None:
    self._InstallWarmpoolImpl(self.spec.sandbox_warmpool.replicas)

  def _WaitWarmpoolReady(
      self, warmpool_name: str, replicas: int, timeout: int = 600
  ) -> None:
    """Polls the SandboxWarmPool until readyReplicas matches the target."""
    kubernetes_commands.WaitForResource(
        f'sandboxwarmpool/{warmpool_name}',
        f'jsonpath={{.status.readyReplicas}}={replicas}',
        condition_type='',
        timeout=timeout,
    )

  def _InstallWarmpoolImpl(self, replicas: int) -> None:
    """Applies a SandboxWarmPool and waits for it to reach the target size."""
    manifest = {
        'apiVersion': 'extensions.agents.x-k8s.io/v1beta1',
        'kind': 'SandboxWarmPool',
        'metadata': {'name': SANDBOX_NAME},
        'spec': {
            'replicas': replicas,
            'sandboxTemplateRef': {'name': SANDBOX_NAME},
        },
    }
    kubernetes_commands.ApplyYaml([manifest])
    if replicas > 0:
      self._WaitWarmpoolReady(SANDBOX_NAME, replicas)
    else:
      logging.info('Warm pool replicas=0; skipping readiness wait.')
