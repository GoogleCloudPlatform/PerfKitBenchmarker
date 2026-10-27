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

_MANIFEST_REF = flags.DEFINE_string(
    'agent_sandbox_manifest_ref',
    'v1.0.8',
    'The git reference to the kubernetes-sigs/agent-sandbox manifest repo.',
)
_RUNTIME_CLASS = flags.DEFINE_string(
    'agent_sandbox_runtime_class',
    None,
    'The runtime class name to use for sandboxes.',
)
_WARMPOOL_REPLICAS = flags.DEFINE_integer(
    'agent_sandbox_warmpool_replicas',
    None,
    'Number of warm sandboxes to maintain.',
)
_WARMPOOL_SHARDS = flags.DEFINE_integer(
    'agent_sandbox_warmpool_shards',
    None,
    'Number of SandboxWarmPool shards to distribute replicas across.',
    lower_bound=1,
)
_CONTROLLER_CLAIM_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_claim_workers',
    None,
    'Number of workers for processing claims.',
)
_NODE_TAINT = flags.DEFINE_string(
    'agent_sandbox_node_taint',
    None,
    'The taint on the sandbox nodepool to tolerate.',
)
_WARMPOOL_REPLENISH_DELAY = flags.DEFINE_string(
    'agent_sandbox_warmpool_replenish_delay',
    None,
    'Delay before WarmPool starts replenishing claimed sandboxes (e.g. "5s").',
)
_TERMINATION_GRACE_PERIOD_SECONDS = flags.DEFINE_integer(
    'agent_sandbox_termination_grace_period_seconds',
    None,
    'Optional terminationGracePeriodSeconds on sandbox pods (e.g. 1 for fast'
    ' teardown).',
    lower_bound=0,
)
_CPU_REQUEST = flags.DEFINE_string(
    'agent_sandbox_cpu_request',
    None,
    'CPU request for each sandbox pod (e.g. "50m" or "100m").',
)
_CPU_LIMIT = flags.DEFINE_string(
    'agent_sandbox_cpu_limit',
    None,
    'CPU limit for each sandbox pod (e.g. "500m" or "none").',
)
_CONTROLLER_SANDBOX_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_sandbox_workers',
    None,
    'Number of workers for processing sandboxes.',
    lower_bound=1,
)
_CONTROLLER_WARMPOOL_WORKERS = flags.DEFINE_integer(
    'agent_sandbox_controller_warmpool_workers',
    None,
    'Number of workers for processing warm pools.',
    lower_bound=1,
)
_CONTROLLER_API_CONNECTIONS = flags.DEFINE_integer(
    'agent_sandbox_controller_api_connections',
    None,
    'Number of HTTP/2 TCP connections from the controller to kube-apiserver.',
    lower_bound=1,
)
_TUNE_APF_WORKLOAD_LOW = flags.DEFINE_boolean(
    'agent_sandbox_tune_apf_workload_low',
    None,
    'If True, locks PriorityLevelConfiguration/workload-low with'
    ' apf.kubernetes.io/autoupdate-spec="false" and expands'
    ' queueLengthLimit/seats.',
)
SANDBOX_NAME = 'agent-sandbox'


def _ManifestSortKey(doc: dict[str, Any]) -> int:
  """Returns sort priority so Namespace/CRDs apply before RBAC/Deployments."""
  kind = doc.get('kind', '')
  if kind == 'Namespace':
    return 0
  if kind == 'CustomResourceDefinition':
    return 1
  if kind == 'ServiceAccount':
    return 2
  if 'Role' in kind:
    return 3
  return 4


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def __init__(
      self,
      spec: k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec,
      cluster: Any,
  ) -> None:
    super().__init__(spec, cluster)
    self.spec: k8s_agent_sandbox_spec.K8sAgentSandboxConfigSpec
    self._manifests_dir: str | None = None

  def _CreateDependencies(self) -> None:
    """Download manifests before creation."""
    self._GetManifestsDir()

  def _NeedsManagedGvisorInstaller(self) -> bool:
    """Returns True if the PKB-managed gVisor DaemonSet installer is required."""
    runtime_class = (
        self.spec.sandbox_template.runtime_class
        if self.spec.sandbox_template
        else None
    )
    return runtime_class in ('runsc', 'runsc-sandbox')

  def _Create(self) -> None:
    """Provision-stage install: gVisor (when using runsc), CRDs, and RBAC."""
    if self._NeedsManagedGvisorInstaller():
      self._InstallGvisor()
    else:
      logging.info(
          'Skipping managed gVisor DaemonSet installation because'
          ' runtime_class=%r.',
          self.spec.sandbox_template.runtime_class
          if self.spec.sandbox_template
          else None,
      )
    self._InstallCrdsAndRbac()

  def InstallWorkload(self) -> None:
    """Prepare-stage install: controller, sandbox template, and warm pool."""
    if self.spec.tune_apf_workload_low:
      self._ConfigureApfWorkloadLow()
    self._EnsureSandboxNodeTaints()
    self._InstallController()
    self._ApplySandboxTemplate()
    self._InstallWarmpool()

  def _ConfigureApfWorkloadLow(self) -> None:
    """Locks PriorityLevelConfiguration/workload-low for high-QPS bursts."""
    plc_manifest = {
        'apiVersion': 'flowcontrol.apiserver.k8s.io/v1',
        'kind': 'PriorityLevelConfiguration',
        'metadata': {
            'name': 'workload-low',
            'annotations': {
                'apf.kubernetes.io/autoupdate-spec': 'false',
            },
        },
        'spec': {
            'type': 'Limited',
            'limited': {
                'nominalConcurrencyShares': 130,
                'lendablePercent': 0,
                'limitResponse': {
                    'type': 'Queue',
                    'queuing': {
                        'queues': 128,
                        'handSize': 8,
                        'queueLengthLimit': 2000,
                    },
                },
            },
        },
    }
    kubernetes_commands.ApplyYaml([plc_manifest])

  def _EnsureSandboxNodeTaints(self) -> None:
    """Applies sandbox_node_taint to nodes in the sandbox nodepool if set."""
    taint = self.spec.sandbox_node_taint
    if not taint:
      return
    nodepool_name = self.spec.sandbox_nodepool
    kubectl.RunKubectlCommand([
        'taint',
        'nodes',
        '-l',
        f'pkb_nodepool={nodepool_name}',
        taint,
        '--overwrite',
    ])

  def _Delete(self) -> None:
    """No-op: the ephemeral cluster teardown reclaims the sandbox stack."""
    pass

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns metadata about the sandbox."""
    metadata = super().GetResourceMetadata()
    metadata.update({
        'agent_sandbox_manifest_ref': self.spec.manifest_ref,
        'agent_sandbox_namespace': self.spec.namespace,
        'agent_sandbox_nodepool': self.spec.sandbox_nodepool,
        'agent_sandbox_manifest_repo': self.spec.manifest_repo,
        'agent_sandbox_node_taint': self.spec.sandbox_node_taint,
        'agent_sandbox_tune_apf_workload_low': self.spec.tune_apf_workload_low,
    })
    if self.spec.controller:
      metadata.update({
          'agent_sandbox_controller_image': self.spec.controller.image,
          'agent_sandbox_controller_claim_workers': (
              self.spec.controller.claim_workers
          ),
          'agent_sandbox_controller_sandbox_workers': (
              self.spec.controller.sandbox_workers
          ),
          'agent_sandbox_controller_warmpool_workers': (
              self.spec.controller.warmpool_workers
          ),
          'agent_sandbox_controller_replicas': self.spec.controller.replicas,
          'agent_sandbox_controller_api_connections': (
              self.spec.controller.api_connections
          ),
          'agent_sandbox_controller_separate_watch_connection': (
              self.spec.controller.separate_watch_connection
          ),
          'agent_sandbox_controller_warmpool_max_refill_rate': (
              self.spec.controller.warmpool_max_refill_rate
          ),
          'agent_sandbox_controller_warmpool_replenish_delay': (
              self.spec.controller.warmpool_replenish_delay
          ),
      })
    if self.spec.sandbox_template:
      metadata.update({
          'agent_sandbox_runtime_class': (
              self.spec.sandbox_template.runtime_class
          ),
          'agent_sandbox_image': self.spec.sandbox_template.image,
          'agent_sandbox_cpu_request': self.spec.sandbox_template.cpu_request,
          'agent_sandbox_cpu_limit': self.spec.sandbox_template.cpu_limit,
          'agent_sandbox_termination_grace_period_seconds': (
              self.spec.sandbox_template.termination_grace_period_seconds
          ),
      })
    if self.spec.sandbox_warmpool:
      metadata.update({
          'agent_sandbox_warmpool_replicas': (
              self.spec.sandbox_warmpool.replicas
          ),
          'agent_sandbox_warmpool_shards': self.spec.sandbox_warmpool.shards,
      })
    return metadata

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
    """Downloads upstream agent-sandbox manifests to a temporary directory."""
    if not manifest_repo.startswith('http'):
      return manifest_repo

    os.makedirs(dest_dir, exist_ok=True)
    repo = manifest_repo.replace(
        'https://raw.githubusercontent.com', 'https://github.com'
    )
    tarball_url = f'{repo}/archive/{controller_ref}.tar.gz'
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

    Args:
      filename: Relative path or filename of the CRD YAML manifest.

    Returns:
      The Kubernetes CRD resource name (`<plural>.<group>`).
    """
    base = filename.split('/')[-1].removesuffix('.yaml')
    group, plural = base.rsplit('_', 1)
    return f'{plural}.{group}'

  def _TaintToToleration(self, taint: str) -> dict[str, str]:
    """Converts a 'key=value:Effect' or 'key:Effect' taint to a toleration dict.

    Parsing mirrors the planned EKS taint parser (PR #6744) but emits a
    Kubernetes toleration so the node taint and the pod toleration stay
    symmetric.

    Args:
      taint: Taint specification string in `key[=value]:Effect` format.

    Returns:
      A Kubernetes pod toleration dictionary.
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
    """Injects image and tuning args into a controller Deployment manifest.

    Modifies `manifest` in place and returns the updated dictionary.

    Args:
      manifest: Parsed Kubernetes Deployment manifest dictionary, mutated in
        place with the configured image, resource requests/limits, and CLI args.
      controller_spec: Controller configuration spec.

    Returns:
      The mutated `manifest` dictionary.
    """
    container = manifest['spec']['template']['spec']['containers'][0]
    if controller_spec.replicas is not None:
      manifest['spec']['replicas'] = controller_spec.replicas

    if controller_spec.image:
      container['image'] = controller_spec.image

    if controller_spec.cpu_request or controller_spec.memory_request:
      resources = container.setdefault('resources', {})
      requests = resources.setdefault('requests', {})
      limits = resources.setdefault('limits', {})
      if controller_spec.cpu_request:
        requests['cpu'] = controller_spec.cpu_request
        if controller_spec.cpu_limit == 'none':
          limits.pop('cpu', None)
        else:
          limits['cpu'] = (
              controller_spec.cpu_limit or controller_spec.cpu_request
          )
      if controller_spec.memory_request:
        requests['memory'] = controller_spec.memory_request
        if controller_spec.memory_limit == 'none':
          limits.pop('memory', None)
        else:
          limits['memory'] = (
              controller_spec.memory_limit or controller_spec.memory_request
          )
      if not limits:
        resources.pop('limits', None)

    args = container.setdefault('args', [])
    if controller_spec.leader_elect is not None:
      args[:] = [a for a in args if not a.startswith('--leader-elect')]
      args.append(f'--leader-elect={str(controller_spec.leader_elect).lower()}')

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
          '--sandbox-warm-pool-concurrent-workers='
          f'{controller_spec.warmpool_workers}'
      )
    if controller_spec.warmpool_max_batch_size is not None:
      args.append(
          '--sandbox-warm-pool-max-batch-size='
          f'{controller_spec.warmpool_max_batch_size}'
      )
    if controller_spec.kube_api_burst is not None:
      args.append(f'--kube-api-burst={controller_spec.kube_api_burst}')
    if controller_spec.kube_api_qps is not None:
      args.append(f'--kube-api-qps={controller_spec.kube_api_qps}')
    if controller_spec.api_connections is not None:
      args.append(f'--api-connections={controller_spec.api_connections}')
    if controller_spec.warmpool_max_refill_rate is not None:
      args.append(
          '--sandbox-warm-pool-max-refill-rate='
          f'{controller_spec.warmpool_max_refill_rate}'
      )
    if controller_spec.warmpool_replenish_delay:
      args.append(
          '--sandbox-warm-pool-replenish-delay='
          f'{controller_spec.warmpool_replenish_delay}'
      )
    if controller_spec.claim_warm_candidate_grace_period:
      args.append(
          '--sandbox-claim-warm-candidate-grace-period='
          f'{controller_spec.claim_warm_candidate_grace_period}'
      )
    if controller_spec.sandbox_write_behind_window:
      args.append(
          '--sandbox-write-behind-window='
          f'{controller_spec.sandbox_write_behind_window}'
      )
    for flag_name, flag_val in (
        (
            '--separate-watch-connection',
            controller_spec.separate_watch_connection,
        ),
        ('--cache-label-selectors', controller_spec.cache_label_selectors),
        ('--disable-sandbox-events', controller_spec.disable_sandbox_events),
        ('--disable-claim-events', controller_spec.disable_claim_events),
        (
            '--disable-claim-observability-annotations',
            controller_spec.disable_claim_observability_annotations,
        ),
    ):
      if flag_val is not None:
        args.append(f'{flag_name}={str(flag_val).lower()}')
    return manifest

  def _InstallGvisor(self) -> None:
    node_selector, tolerations = self._SandboxScheduling()
    self._InstallGvisorImpl(node_selector, tolerations)

  def _InstallGvisorImpl(
      self, node_selector: dict[str, str], tolerations: list[dict[str, str]]
  ) -> None:
    """Installs gVisor onto cluster nodes via the installer DaemonSet."""
    install_script_path = data.ResourcePath(
        'agent_sandbox/gvisor-installer/install.sh'
    )
    install_script_content = ''
    if os.path.exists(install_script_path):
      with open(install_script_path) as f:
        install_script_content = f.read()

    manifests = []
    for filename in ('configmap.yaml', 'daemonset.yaml', 'runtimeclass.yaml'):
      path = data.ResourcePath(f'agent_sandbox/gvisor-installer/{filename}')
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
    crd_files = sorted(
        f
        for f in os.listdir(crds_dir)
        if f.endswith('.yaml') and f != 'kustomization.yaml'
    )
    for filename in crd_files:
      kubectl.RunKubectlCommand(
          ['apply', '-f', os.path.join(crds_dir, filename)]
      )
    for filename in crd_files:
      kubernetes_commands.WaitForResource(
          f'crd/{self._CrdName(filename)}', 'established'
      )

  def _InstallController(self) -> None:
    assert self.spec.controller is not None
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
        if not filename.endswith('.yaml') or filename == 'kustomization.yaml':
          continue
        path = os.path.join(root, filename)
        with open(path) as f:
          for doc in yaml.safe_load_all(f):
            if not doc or doc.get('kind') == 'Kustomization':
              continue

            if (
                doc.get('kind') == 'Deployment'
                and doc.get('metadata', {}).get('name')
                == 'agent-sandbox-controller'
            ):
              if not filename.startswith('extensions'):
                continue
              doc = self._ConfigureControllerManifest(doc, controller_spec)

            all_docs.append(doc)

    # Sort so Namespace and CRDs are created before RoleBindings and Deployments
    all_docs.sort(key=_ManifestSortKey)
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.yaml', delete=False
    ) as f:
      yaml.dump_all(all_docs, f)
      manifest_path = f.name

    try:
      kubectl.RunKubectlCommand(['apply', '-f', manifest_path])
    finally:
      os.unlink(manifest_path)
    kubernetes_commands.WaitForRollout(
        'deployment/agent-sandbox-controller', namespace='agent-sandbox-system'
    )

  def _ApplySandboxTemplate(self) -> None:
    assert self.spec.sandbox_template is not None
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
    limits: dict[str, str] = {'ephemeral-storage': '256Mi'}
    if template_spec.cpu_limit != 'none':
      limits['cpu'] = template_spec.cpu_limit or template_spec.cpu_request
    if template_spec.memory_limit != 'none':
      limits['memory'] = (
          template_spec.memory_limit or template_spec.memory_request
      )
    pod_spec: dict[str, Any] = {
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
                'limits': limits,
            },
        }],
        'restartPolicy': 'OnFailure',
        'nodeSelector': node_selector,
        'tolerations': tolerations,
    }
    if template_spec.runtime_class:
      pod_spec['runtimeClassName'] = template_spec.runtime_class
    if template_spec.termination_grace_period_seconds is not None:
      pod_spec['terminationGracePeriodSeconds'] = (
          template_spec.termination_grace_period_seconds
      )

    spec_body: dict[str, Any] = {
        'podTemplate': {'metadata': {'labels': labels}, 'spec': pod_spec}
    }
    if template_spec.failed_pod_policy:
      spec_body['failedPodPolicy'] = template_spec.failed_pod_policy
    if template_spec.network_policy_management:
      spec_body['networkPolicyManagement'] = (
          template_spec.network_policy_management
      )
    if template_spec.service is not None:
      spec_body['service'] = template_spec.service

    manifest = {
        'apiVersion': 'extensions.agents.x-k8s.io/v1beta1',
        'kind': 'SandboxTemplate',
        'metadata': {'name': SANDBOX_NAME, 'namespace': self.spec.namespace},
        'spec': spec_body,
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
        nodepool.name, self.spec.sandbox_node_taint
    )

  def _InstallWarmpool(self) -> None:
    assert self.spec.sandbox_warmpool is not None
    self._InstallWarmpoolImpl(self.spec.sandbox_warmpool.replicas)

  def _WaitWarmpoolReady(
      self, warmpool_name: str, replicas: int, timeout: int = 600
  ) -> None:
    """Polls the SandboxWarmPool until readyReplicas matches the target."""
    kubernetes_commands.WaitForResource(
        f'sandboxwarmpool/{warmpool_name}',
        f'jsonpath={{.status.readyReplicas}}={replicas}',
        namespace=self.spec.namespace,
        condition_type='',
        timeout=timeout,
    )

  def GetWarmpoolNames(self) -> list[str]:
    """Returns all SandboxWarmPool shard names configured on the cluster."""
    shards = (
        self.spec.sandbox_warmpool.shards if self.spec.sandbox_warmpool else 1
    )
    if shards <= 1:
      return [SANDBOX_NAME]
    return [f'{SANDBOX_NAME}-{i}' for i in range(shards)]

  def ScrapeControllerMetrics(self) -> str:
    """Scrapes Prometheus metrics from the active controller pod on port 8080."""
    pod_out, _, ret = kubectl.RunKubectlCommand(
        [
            'get',
            'pods',
            '-n',
            'agent-sandbox-system',
            '-l',
            'app=agent-sandbox-controller',
            '-o',
            'jsonpath={.items[*].metadata.name}',
        ],
        raise_on_failure=False,
    )
    pod_names = pod_out.split()
    if ret != 0 or not pod_names:
      logging.warning(
          'Could not locate agent-sandbox-controller pod for metrics.'
      )
      return ''
    best_prom = ''
    best_score = -1.0
    for pod_name in pod_names:
      prom_out, _, m_ret = kubectl.RunKubectlCommand(
          [
              'get',
              '--raw',
              f'/api/v1/namespaces/agent-sandbox-system/pods/{pod_name}:8080/proxy/metrics',
          ],
          raise_on_failure=False,
      )
      if m_ret != 0:
        logging.warning(
            'Failed to scrape controller metrics from pod %s.', pod_name
        )
        continue
      score = 0.0
      for line in prom_out.splitlines():
        if line.startswith('controller_runtime_reconcile_total{'):
          parts = line.split()
          if len(parts) >= 2:
            try:
              score += float(parts[-1])
            except ValueError:
              pass
      if score > best_score:
        best_score = score
        best_prom = prom_out
    return best_prom

  def ScrapeControlPlaneMetrics(self) -> dict[str, float]:
    """Scrapes kube-apiserver sizing and APF seat metrics via /metrics."""
    prom_out, _, ret = kubectl.RunKubectlCommand(
        ['get', '--raw', '/metrics'],
        raise_on_failure=False,
    )
    if ret != 0 or not prom_out:
      return {}
    result: dict[str, float] = {}
    nominal_seats_sum = 0.0
    has_nominal_seats = False
    workload_low_rejected = 0.0
    has_workload_low_rejected = False
    for raw_line in prom_out.splitlines():
      line = raw_line.strip()
      if not line or line.startswith('#'):
        continue
      parts = line.split()
      if len(parts) < 2:
        continue
      metric_and_labels, val_str = parts[0], parts[1]
      try:
        val = float(val_str)
      except ValueError:
        continue
      if metric_and_labels.startswith('go_sched_gomaxprocs_threads'):
        result['gomaxprocs'] = val
      elif metric_and_labels.startswith('process_start_time_seconds'):
        result['process_start_time'] = val
      elif metric_and_labels.startswith('process_resident_memory_bytes'):
        result['rss_mb'] = round(val / (1024.0 * 1024.0), 2)
      elif metric_and_labels.startswith(
          'apiserver_flowcontrol_nominal_limit_seats{'
      ):
        nominal_seats_sum += val
        has_nominal_seats = True
        if 'priority_level="workload-low"' in metric_and_labels:
          result['workload_low_seats'] = val
        elif 'priority_level="global-default"' in metric_and_labels:
          result['global_default_seats'] = val
      elif (
          metric_and_labels.startswith(
              'apiserver_flowcontrol_rejected_requests_total{'
          )
          and 'priority_level="workload-low"' in metric_and_labels
      ):
        workload_low_rejected += val
        has_workload_low_rejected = True
    if has_nominal_seats:
      result['nominal_limit_seats'] = nominal_seats_sum
    if has_workload_low_rejected:
      result['workload_low_rejected_requests'] = workload_low_rejected
    return result

  def _InstallWarmpoolImpl(self, replicas: int) -> None:
    """Applies SandboxWarmPool(s) and waits for each shard to reach target size."""
    pool_names = self.GetWarmpoolNames()
    num_shards = len(pool_names)
    base_replicas = replicas // num_shards
    remainder = replicas % num_shards
    manifests = []
    shard_targets: list[tuple[str, int]] = []
    for idx, pool_name in enumerate(pool_names):
      shard_replicas = base_replicas + (1 if idx < remainder else 0)
      shard_targets.append((pool_name, shard_replicas))
      manifests.append({
          'apiVersion': 'extensions.agents.x-k8s.io/v1beta1',
          'kind': 'SandboxWarmPool',
          'metadata': {'name': pool_name, 'namespace': self.spec.namespace},
          'spec': {
              'replicas': shard_replicas,
              'sandboxTemplateRef': {'name': SANDBOX_NAME},
          },
      })
    kubernetes_commands.ApplyYaml(manifests)
    if replicas > 0:
      for pool_name, shard_replicas in shard_targets:
        if shard_replicas > 0:
          self._WaitWarmpoolReady(pool_name, shard_replicas)
    else:
      logging.info('Warm pool replicas=0; skipping readiness wait.')
