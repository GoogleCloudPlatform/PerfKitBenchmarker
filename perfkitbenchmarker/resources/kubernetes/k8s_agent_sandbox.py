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
from absl import logging

from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands


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

_SANDBOX_NAME = 'agent-sandbox'


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


def install_gvisor():
  """Installs gVisor onto cluster nodes via the installer DaemonSet.

  Creates the gvisor-installer-script ConfigMap (from install.sh) in
  kube-system before applying the DaemonSet. The DaemonSet mounts that
  ConfigMap at /scripts; the init container runs /scripts/install.sh.
  The ConfigMap must exist before the DaemonSet pods schedule.
  """
  _create_installer_configmap()
  # Plain .yaml files: ApplyManifest with NO kwargs.
  kubernetes_commands.ApplyManifest(data.ResourcePath(_GVISOR_DAEMONSET))
  kubernetes_commands.ApplyManifest(data.ResourcePath(_GVISOR_RUNTIMECLASS))
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


def install_controller(
    controller_ref, controller_image, controller_tuning=None
):
  """Installs CRDs, RBAC, and the controller Deployment from upstream.

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
  for filename in _CRD_FILES:
    _apply_url(_url(controller_ref, filename))
  for filename in _CRD_FILES:
    kubernetes_commands.WaitForResource(
        f'crd/{_crd_name(filename)}', 'established'
    )
  for filename in _RBAC_FILES:
    _apply_url(_url(controller_ref, filename))
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


def apply_template(template_name, template_spec):
  """Applies the SandboxTemplate rendered from the template spec."""
  labels = template_spec.labels or {'sandbox': 'python-sandbox-bench'}
  kubernetes_commands.ApplyManifest(
      _TEMPLATE_MANIFEST,
      template_name=template_name,
      runtime_class=template_spec.runtime_class,
      image=template_spec.image,
      cpu_request=template_spec.cpu_request,
      cpu_limit=template_spec.cpu_limit,
      memory_request=template_spec.memory_request,
      memory_limit=template_spec.memory_limit,
      labels=labels,
  )


def install_warmpool(warmpool_name, template_name, replicas):
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
      data.ResourcePath(_WARMPOOL_MANIFEST),
      warmpool_name=warmpool_name,
      template_name=template_name,
      replicas=replicas,
  )
  _wait_warmpool_ready(warmpool_name, replicas)


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def _Create(self):
    """Installs the kubernetes-sigs/agent-sandbox stack onto the cluster."""
    self._InstallGvisor()
    self._InstallController()
    self._ApplyTemplate()
    self._InstallWarmpool()

  def _Delete(self):
    """No-op: the ephemeral cluster teardown reclaims the sandbox stack."""
    pass

  def _InstallGvisor(self):
    install_gvisor()

  def _InstallController(self):
    install_controller(
        controller_ref=self.spec.manifest_ref,
        controller_image=self.spec.controller.image,
        controller_tuning=self._BuildTuning(),
    )

  def _ApplyTemplate(self):
    apply_template(_SANDBOX_NAME, self.spec.sandbox_template)

  def _InstallWarmpool(self):
    install_warmpool(
        _SANDBOX_NAME, _SANDBOX_NAME, self.spec.sandbox_warmpool.replicas)

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
