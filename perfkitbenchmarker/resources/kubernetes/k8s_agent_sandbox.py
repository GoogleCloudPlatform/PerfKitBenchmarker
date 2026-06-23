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

import yaml
from absl import flags
from absl import logging

from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox_spec
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS


# Mapping from tuning dict keys to the controller CLI flag strings.
_TUNING_ARG_MAP = (
    ('claim_workers', '--sandbox-claim-concurrent-workers={}'),
    ('sandbox_workers', '--sandbox-concurrent-workers={}'),
    ('warmpool_workers', '--sandbox-warm-pool-concurrent-workers={}'),
    ('warmpool_max_batch_size', '--sandbox-warm-pool-max-batch-size={}'),
    ('kube_api_burst', '--kube-api-burst={}'),
    ('kube_api_qps', '--kube-api-qps={}'),
)

_DEFAULT_CPU_REQUEST = '500m'
_DEFAULT_CPU_LIMIT = '2'
_DEFAULT_MEMORY_REQUEST = '256Mi'
_DEFAULT_MEMORY_LIMIT = '1Gi'

_GVISOR_DAEMONSET = 'agent_sandbox/gvisor-installer/daemonset.yaml'
_GVISOR_RUNTIMECLASS = 'agent_sandbox/gvisor-installer/runtimeclass.yaml'
_GVISOR_INSTALLER_SCRIPT = 'agent_sandbox/gvisor-installer/install.sh'
_GVISOR_CONFIGMAP_NAME = 'gvisor-installer-script'
_TEMPLATE_MANIFEST = 'agent_sandbox/sandbox-template.yaml.j2'
_WARMPOOL_MANIFEST = 'agent_sandbox/sandbox-warmpool.yaml.j2'

_RELEASE_BASE = (
    'https://raw.githubusercontent.com/kubernetes-sigs/agent-sandbox'
)

_CRD_FILES = (
    'crds/agents.x-k8s.io_sandboxes.yaml',
    'crds/extensions.agents.x-k8s.io_sandboxclaims.yaml',
    'crds/extensions.agents.x-k8s.io_sandboxtemplates.yaml',
    'crds/extensions.agents.x-k8s.io_sandboxwarmpools.yaml',
)

_RBAC_FILES = (
    'rbac.generated.yaml',
    'extensions-rbac.generated.yaml',
    'extensions.yaml',
)

_CORE_FILE = 'controller.yaml'
_CONTROLLER_FILE = 'extensions.controller.yaml'

SANDBOX_NAME = 'agent-sandbox'

# Config key of the nodepool the sandbox workload runs on (matches the
# `nodepools:` block in linux_benchmarks/agent_sandbox_benchmark.py).
_SANDBOX_NODEPOOL = 'sandbox'

# Taint fencing the sandbox nodepool. PKB does not yet apply nodepool taints to
# nodes (that wiring lands in PR #6741), so keep the canonical taint string here
# and derive the pod toleration from it. Keep this in lockstep with the
# `node_taints` entry in agent_sandbox_benchmark.py BENCHMARK_CONFIG.
# TODO(#6741): read this from cluster.nodepools[_SANDBOX_NODEPOOL].node_taints
# once NodepoolConfig carries node_taints, and delete this constant.
_SANDBOX_TAINT = 'sandbox.gke.io/runtime=runsc:NoSchedule'


def _crd_name(filename):
  """Derives a CRD resource name from its manifest filename.

  Example: 'crds/agents.x-k8s.io_sandboxes.yaml' -> 'sandboxes.agents.x-k8s.io'.
  """
  base = filename.split('/')[-1].removesuffix('.yaml')
  group, plural = base.rsplit('_', 1)
  return f'{plural}.{group}'


def _url(ref, filename):
  return f'{_RELEASE_BASE}/{ref}/k8s/{filename}'


def _apply_url(url):
  """Applies a manifest from a URL. Isolated for test mocking."""
  kubectl.RunKubectlCommand(['apply', '-f', url])


def _wait_warmpool_ready(warmpool_name, replicas, timeout=600):
  """Polls the SandboxWarmPool until readyReplicas matches the target."""
  kubernetes_commands.WaitForResource(
      f'sandboxwarmpool/{warmpool_name}',
      f'jsonpath={{.status.readyReplicas}}={replicas}',
      condition_type='',
      timeout=timeout,
  )


def _taint_to_toleration(taint):
  """Converts a 'key=value:Effect' or 'key:Effect' taint to a toleration dict.

  Parsing mirrors the planned EKS taint parser (PR #6744) but emits a
  Kubernetes toleration so the node taint and the pod toleration stay symmetric.
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


def _sandbox_scheduling(nodepool_name):
  """Returns (node_selector, tolerations) targeting the sandbox nodepool.

  The selector rendezvous is the pkb_nodepool label PKB stamps on every node
  pool (GKE _AddNodeParamsToCmd, EKS _RenderNodeGroupJson), so there is no
  bespoke label to keep in sync. The toleration is derived from _SANDBOX_TAINT.
  """
  node_selector = {'pkb_nodepool': nodepool_name}
  tolerations = [_taint_to_toleration(_SANDBOX_TAINT)]
  return node_selector, tolerations


def _render_gvisor_daemonset(node_selector, tolerations):
  """Loads the installer DaemonSet and injects the sandbox scheduling target."""
  with open(data.ResourcePath(_GVISOR_DAEMONSET)) as manifest_file:
    manifest = yaml.safe_load(manifest_file)
  pod_spec = manifest['spec']['template']['spec']
  pod_spec['nodeSelector'] = node_selector
  pod_spec['tolerations'] = tolerations
  return yaml.dump(manifest, default_flow_style=False)


def _render_template_manifest(template_spec, node_selector, tolerations):
  """Renders the SandboxTemplate and injects the sandbox scheduling target.

  nodeSelector/tolerations are set in Python (not the .j2) so the scheduling
  target has a single source of truth. runtimeClassName stays in the template:
  it is runtime identity, not scheduling.
  """
  labels = template_spec.labels or {'sandbox': 'python-sandbox-bench'}
  rendered = vm_util.ReadAndRenderJinja2Template(
      _TEMPLATE_MANIFEST,
      trim_spaces=False,
      name=SANDBOX_NAME,
      runtime_class=template_spec.runtime_class,
      image=template_spec.image,
      cpu_request=template_spec.cpu_request,
      cpu_limit=template_spec.cpu_limit,
      memory_request=template_spec.memory_request,
      memory_limit=template_spec.memory_limit,
      labels=labels,
  )
  manifest = yaml.safe_load(rendered)
  pod_spec = manifest['spec']['podTemplate']['spec']
  pod_spec['nodeSelector'] = node_selector
  pod_spec['tolerations'] = tolerations
  return yaml.dump(manifest, default_flow_style=False)


def install_gvisor(node_selector, tolerations):
  """Installs gVisor onto cluster nodes via the installer DaemonSet.

  Creates the gvisor-installer-script ConfigMap (from install.sh) in
  kube-system before applying the DaemonSet. The DaemonSet mounts that
  ConfigMap at /scripts; the init container runs /scripts/install.sh.
  The ConfigMap must exist before the DaemonSet pods schedule. The DaemonSet's
  nodeSelector/tolerations are injected so it lands on the sandbox nodepool.
  """
  _create_installer_configmap()
  _apply_yaml(_render_gvisor_daemonset(node_selector, tolerations))
  kubernetes_commands.ApplyManifest(_GVISOR_RUNTIMECLASS)
  kubernetes_commands.WaitForRollout(
      'daemonset/gvisor-installer', namespace='kube-system'
  )


def _create_installer_configmap():
  """Creates or updates the gvisor-installer-script ConfigMap in kube-system.

  Uses --dry-run=client -o yaml to render the ConfigMap manifest (idempotent
  on re-runs), writes it to a temp file, then applies it. A plain
  'kubectl create configmap' would fail if the resource already exists.
  """
  script_path = data.ResourcePath(_GVISOR_INSTALLER_SCRIPT)
  # Render the ConfigMap manifest without hitting the cluster.
  # Use --from-file=install.sh=<path> so the ConfigMap has exactly one key
  # named install.sh, instead of pulling the whole directory (which would
  # also include daemonset.yaml and runtimeclass.yaml as keys).
  yaml_out, _, _ = kubectl.RunKubectlCommand([
      'create',
      'configmap',
      _GVISOR_CONFIGMAP_NAME,
      f'--from-file=install.sh={script_path}',
      '--namespace',
      'kube-system',
      '--dry-run=client',
      '-o',
      'yaml',
  ])
  # Write the rendered manifest to a temp file and apply it.
  tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
  with tmpfile as tmp:
    tmp.write(yaml_out)
    tmp_path = tmp.name
  try:
    kubectl.RunKubectlCommand(['apply', '-f', tmp_path])
  finally:
    os.unlink(tmp_path)


def _apply_yaml(yaml_str):
  """Writes yaml_str to a temp file and applies it with kubectl apply."""
  tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
  with tmpfile as tmp:
    tmp.write(yaml_str)
    tmp_path = tmp.name
  try:
    kubectl.RunKubectlCommand(['apply', '-f', tmp_path])
  finally:
    os.unlink(tmp_path)


def _configure_controller_manifest(manifest_yaml, controller_image, tuning):
  """Injects image and all tuning into a controller Deployment manifest dict.

  Returns the modified manifest as a YAML string ready to pipe to kubectl apply.
  """
  manifest = yaml.safe_load(manifest_yaml)
  container = manifest['spec']['template']['spec']['containers'][0]

  # Image.
  if controller_image:
    container['image'] = controller_image

  # Resources (Burstable QoS; base manifest ships with nothing -> BestEffort).
  container['resources'] = {
      'requests': {
          'cpu': tuning.get('cpu_request', _DEFAULT_CPU_REQUEST),
          'memory': tuning.get('memory_request', _DEFAULT_MEMORY_REQUEST),
      },
      'limits': {
          'cpu': tuning.get('cpu_limit', _DEFAULT_CPU_LIMIT),
          'memory': tuning.get('memory_limit', _DEFAULT_MEMORY_LIMIT),
      },
  }

  # Leader election: base manifest sets --leader-elect=true at args[0]; replace
  # it so exactly one effective flag exists.
  args = container.setdefault('args', [])
  le_flag = (
      '--leader-elect=true'
      if tuning.get('leader_elect')
      else '--leader-elect=false'
  )
  if args and args[0].startswith('--leader-elect'):
    args[0] = le_flag
  else:
    args.insert(0, le_flag)

  # Controller tuning args (appended; order doesn't matter).
  for key, arg_fmt in _TUNING_ARG_MAP:
    value = tuning.get(key)
    if value is not None:
      args.append(arg_fmt.format(value))
  if tuning.get('enable_tracing'):
    args.append('--enable-tracing=true')

  # OTEL env vars (kubectl set env handles absent env array; we do it inline).
  if tuning.get('enable_tracing') and tuning.get('otel_endpoint'):
    env = container.setdefault('env', [])
    env.append({
        'name': 'OTEL_EXPORTER_OTLP_ENDPOINT',
        'value': tuning['otel_endpoint'],
    })
    env.append({'name': 'OTEL_EXPORTER_OTLP_INSECURE', 'value': 'true'})

  return yaml.dump(manifest, default_flow_style=False)


def install_crds_and_rbac(controller_ref):
  """Applies the agent-sandbox CRDs and cluster-scoped RBAC.

  Run in the provision stage. CRDs and RBAC are cluster-scoped, slow to
  establish, and unaffected by controller image or tuning changes, so they are
  not re-applied when the controller is reinstalled in the prepare stage.

  Args:
    controller_ref: Git ref (tag or SHA) for upstream raw asset URLs.
  """
  for filename in _CRD_FILES:
    _apply_url(_url(controller_ref, filename))
  for filename in _CRD_FILES:
    kubernetes_commands.WaitForResource(
        f'crd/{_crd_name(filename)}', 'established'
    )
  for filename in _RBAC_FILES:
    _apply_url(_url(controller_ref, filename))


def install_controller(
    controller_ref, controller_image, controller_tuning=None
):
  """Installs the controller core resources and Deployment from upstream.

  CRDs and RBAC must already be installed (see install_crds_and_rbac). Applies
  the non-Deployment resources from controller.yaml (Namespace, ServiceAccount,
  ClusterRoleBinding, Service), then the image- and tuning-configured
  Deployment, and waits for the rollout. Safe to re-run against an existing
  cluster: kubectl apply is idempotent and a changed image or args triggers a
  rolling update.

  Args:
    controller_ref: Git ref (tag or SHA) for upstream raw asset URLs.
    controller_image: Optional controller container image override. Ref-based
      manifests ship a ko:// placeholder image that is not pullable; callers
      must supply a real image when installing from a ref rather than a tagged
      release (which bundles a resolved image in the manifest).
    controller_tuning: Optional dict. Supported keys:
      claim_workers, sandbox_workers, kube_api_burst, kube_api_qps,
      enable_tracing, otel_endpoint (applied after manifests and image patch);
      leader_elect (bool, default False), cpu_request, cpu_limit,
      memory_request, memory_limit (strings, default values) which control
      the unconditional leader-election and resources patch.
  """
  tuning = controller_tuning or {}
  # Apply everything in controller.yaml except the Deployment. The base
  # Deployment in that file has a placeholder image; skipping it here means
  # the controller Deployment is created exactly once, fully configured, below.
  raw_core, _, _ = vm_util.IssueCommand(
      ['curl', '-fsSL', _url(controller_ref, _CORE_FILE)]
  )
  non_deployment = [
      doc
      for doc in yaml.safe_load_all(raw_core)
      if doc and doc.get('kind') != 'Deployment'
  ]
  if non_deployment:
    _apply_yaml(yaml.dump_all(non_deployment))
  # Download extensions.controller.yaml, inject all configuration in memory,
  # and apply in one shot -- one rollout, correct config from the first apply.
  controller_url = _url(controller_ref, _CONTROLLER_FILE)
  raw_manifest, _, _ = vm_util.IssueCommand(['curl', '-fsSL', controller_url])
  configured = _configure_controller_manifest(
      raw_manifest, controller_image, tuning
  )
  logging.info('Applying controller deployment (image=%s)', controller_image)
  _apply_yaml(configured)
  kubernetes_commands.WaitForRollout(
      'deployment/agent-sandbox-controller', namespace='agent-sandbox-system'
  )


def apply_template(template_spec, node_selector, tolerations):
  """Applies the SandboxTemplate rendered from the template spec."""
  _apply_yaml(
      _render_template_manifest(template_spec, node_selector, tolerations)
  )


def install_warmpool(replicas):
  """Applies a SandboxWarmPool and waits for it to reach the target size.

  If replicas is 0, skips both the manifest apply and the readiness wait.
  Waiting for readyReplicas=0 is ill-defined (the controller may never update
  status on an empty pool), and applying a zero-replica warmpool is unnecessary
  for cold-start benchmarks.
  """
  if replicas == 0:
    logging.info('Warm pool replicas=0; skipping warm pool install.')
    return
  kubernetes_commands.ApplyManifest(
      _WARMPOOL_MANIFEST,
      name=SANDBOX_NAME,
      replicas=replicas,
  )
  _wait_warmpool_ready(SANDBOX_NAME, replicas)


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def _Create(self):
    """Provision-stage install: gVisor, CRDs, and RBAC.

    The controller, sandbox template, and warm pool are installed in the
    prepare stage (see InstallWorkload) so they can be re-applied against an
    existing cluster without re-provisioning.
    """
    self._InstallGvisor()
    self._InstallCrdsAndRbac()

  def InstallWorkload(self):
    """Prepare-stage install: controller, sandbox template, and warm pool.

    Idempotent: safe to re-run against an existing cluster to iterate on
    controller settings (a changed image or tuning triggers a rolling update).
    """
    self._InstallController()
    self._ApplyTemplate()
    self._InstallWarmpool()

  def RefreshSpecFromFlags(self):
    """Rebuilds the prepare-stage sub-specs from current command-line flags.

    The benchmark spec is pickled during provision and unpickled without
    re-applying flags, so on a `--run_stage=prepare` resume the controller,
    template, and warm pool config would otherwise reflect the provision-time
    flags. Rebuilding from the current flags lets controller settings be
    iterated against an existing cluster. The rebuild starts from flag defaults,
    so re-run with the full flag set used at provision plus whatever is being
    changed.
    """
    self.spec.controller = k8s_agent_sandbox_spec.ControllerSpec(
        'agent_sandbox.controller', flag_values=FLAGS
    )
    self.spec.sandbox_template = k8s_agent_sandbox_spec.SandboxTemplateSpec(
        'agent_sandbox.sandbox_template', flag_values=FLAGS
    )
    self.spec.sandbox_warmpool = k8s_agent_sandbox_spec.SandboxWarmPoolSpec(
        'agent_sandbox.sandbox_warmpool', flag_values=FLAGS
    )
    if FLAGS['agent_sandbox_namespace'].present:
      self.spec.namespace = FLAGS.agent_sandbox_namespace
    if FLAGS['agent_sandbox_manifest_ref'].present:
      self.spec.manifest_ref = FLAGS.agent_sandbox_manifest_ref

  def _Delete(self):
    """No-op: the ephemeral cluster teardown reclaims the sandbox stack."""
    pass

  def _InstallGvisor(self):
    node_selector, tolerations = self._SandboxScheduling()
    install_gvisor(node_selector, tolerations)

  def _InstallCrdsAndRbac(self):
    install_crds_and_rbac(self.spec.manifest_ref)

  def _InstallController(self):
    install_controller(
        controller_ref=self.spec.manifest_ref,
        controller_image=self.spec.controller.image,
        controller_tuning=self._BuildTuning(),
    )

  def _ApplyTemplate(self):
    node_selector, tolerations = self._SandboxScheduling()
    apply_template(self.spec.sandbox_template, node_selector, tolerations)

  def _SandboxScheduling(self):
    """Resolves (node_selector, tolerations) from the sandbox nodepool."""
    nodepool = self.cluster.nodepools.get(_SANDBOX_NODEPOOL)
    if nodepool is None:
      raise ValueError(
          f'Agent sandbox requires a nodepool named {_SANDBOX_NODEPOOL!r}; '
          'add it to the benchmark container_cluster nodepools config.'
      )
    return _sandbox_scheduling(nodepool.name)

  def _InstallWarmpool(self):
    install_warmpool(self.spec.sandbox_warmpool.replicas)

  def _BuildTuning(self):
    """Builds the controller_tuning dict from the controller sub-spec."""
    c = self.spec.controller
    tuning = {
        'enable_tracing': c.enable_tracing,
        'leader_elect': c.leader_elect,
        'cpu_request': c.cpu_request,
        'cpu_limit': c.cpu_limit,
        'memory_request': c.memory_request,
        'memory_limit': c.memory_limit,
    }
    for key in (
        'claim_workers', 'sandbox_workers', 'warmpool_workers',
        'warmpool_max_batch_size', 'kube_api_burst', 'kube_api_qps',
        'otel_endpoint',
    ):
      value = getattr(c, key)
      if value is not None:
        tuning[key] = value
    return tuning
