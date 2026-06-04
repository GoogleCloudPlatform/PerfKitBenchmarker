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
Skeleton only: the install logic is added in a follow-up change.
"""

import yaml

from perfkitbenchmarker.resources import agent_sandbox
from perfkitbenchmarker.resources import agent_sandbox_spec


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


class K8sAgentSandbox(agent_sandbox.BaseAgentSandbox):
  """Installs the open-source kubernetes-sigs/agent-sandbox stack."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def _Create(self):
    """Installs the OSS agent sandbox stack onto the cluster."""
    raise NotImplementedError(
        'OSS agent sandbox install is not yet implemented.'
    )

  def _Delete(self):
    """Removes the OSS agent sandbox stack from the cluster."""
    raise NotImplementedError(
        'OSS agent sandbox teardown is not yet implemented.'
    )
