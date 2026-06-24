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
"""Spec for the Kubernetes agent sandbox."""

from typing import Any

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.resources import agent_sandbox_spec

_RELEASE = 'v0.5.0'
_DEFAULT_CONTROLLER_IMAGE = (
    'us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/'
    'agent-sandbox-controller:v20260627-v0.5.0-10-g1cca985-main'
)
_DEFAULT_SANDBOX_IMAGE = (
    f'registry.k8s.io/agent-sandbox/python-runtime-sandbox:{_RELEASE}'
)
DEFAULT_CPU_REQUEST = '250m'
DEFAULT_CPU_LIMIT = '1'
DEFAULT_MEMORY_REQUEST = '256Mi'
DEFAULT_MEMORY_LIMIT = '1Gi'


class ControllerSpec(spec.BaseSpec):
  """Config for the agent-sandbox controller deployment."""

  def __init__(self, *args, **kwargs):
    self.image: str
    self.claim_workers: int | None
    self.sandbox_workers: int | None
    self.warmpool_workers: int | None
    self.warmpool_max_batch_size: int | None
    self.kube_api_burst: int | None
    self.kube_api_qps: int | None
    self.cpu_request: str
    self.memory_request: str
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'image': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_CONTROLLER_IMAGE},
        ),
        'claim_workers': (
            option_decoders.IntDecoder,
            {'default': 100, 'none_ok': True},
        ),
        'sandbox_workers': (
            option_decoders.IntDecoder,
            {'default': 100, 'none_ok': True},
        ),
        'warmpool_workers': (
            option_decoders.IntDecoder,
            {'default': 5, 'none_ok': True},
        ),
        'warmpool_max_batch_size': (
            option_decoders.IntDecoder,
            {'default': 300, 'none_ok': True},
        ),
        'kube_api_burst': (
            option_decoders.IntDecoder,
            {'default': 750, 'none_ok': True},
        ),
        'kube_api_qps': (
            option_decoders.IntDecoder,
            {'default': -1, 'none_ok': True},
        ),
        'cpu_request': (
            option_decoders.StringDecoder,
            {'default': DEFAULT_CPU_REQUEST},
        ),
        'memory_request': (
            option_decoders.StringDecoder,
            {'default': DEFAULT_MEMORY_REQUEST},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values: dict[str, Any], flag_values: Any) -> None:
    super()._ApplyFlags(config_values, flag_values)
    if flag_values:
      if flag_values['agent_sandbox_controller_claim_workers'].present:
        config_values['claim_workers'] = (
            flag_values.agent_sandbox_controller_claim_workers
        )


class SandboxTemplateSpec(spec.BaseSpec):
  """Config for the SandboxTemplate (models SandboxTemplateSpec).

  Fields rendered into the template: runtime_class, image, resources, labels.
  """

  def __init__(self, *args, **kwargs):
    self.runtime_class: str
    self.image: str
    self.cpu_request: str
    self.memory_request: str
    self.labels: dict[str, str] | None
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'runtime_class': (option_decoders.StringDecoder, {'default': 'runsc'}),
        'image': (
            option_decoders.StringDecoder,
            {'default': _DEFAULT_SANDBOX_IMAGE},
        ),
        'cpu_request': (option_decoders.StringDecoder, {'default': '100m'}),
        'memory_request': (option_decoders.StringDecoder, {'default': '256Mi'}),
        'labels': (
            option_decoders.TypeVerifier,
            {'default': None, 'none_ok': True},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values: dict[str, Any], flag_values: Any) -> None:
    super()._ApplyFlags(config_values, flag_values)
    if flag_values and flag_values['agent_sandbox_runtime_class'].present:
      config_values['runtime_class'] = flag_values.agent_sandbox_runtime_class


class SandboxWarmPoolSpec(spec.BaseSpec):
  """Config for the SandboxWarmPool (models SandboxWarmPoolSpec)."""

  def __init__(self, *args, **kwargs):
    self.replicas: int
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'replicas': (option_decoders.IntDecoder, {'default': 150, 'min': 0}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values: dict[str, Any], flag_values: Any) -> None:
    super()._ApplyFlags(config_values, flag_values)
    if flag_values and flag_values['agent_sandbox_warmpool_replicas'].present:
      config_values['replicas'] = flag_values.agent_sandbox_warmpool_replicas


class _ControllerDecoder(option_decoders.TypeVerifier):
  """Decodes the controller config block into a ControllerSpec."""

  def Decode(
      self, value: Any, component_full_name: str, flag_values: Any
  ) -> ControllerSpec:
    super().Decode(value, component_full_name, flag_values)
    return ControllerSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value,
    )


class _SandboxTemplateDecoder(option_decoders.TypeVerifier):
  """Decodes the sandbox_template config block into a SandboxTemplateSpec."""

  def Decode(
      self, value: Any, component_full_name: str, flag_values: Any
  ) -> SandboxTemplateSpec:
    super().Decode(value, component_full_name, flag_values)
    return SandboxTemplateSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value,
    )


class _SandboxWarmPoolDecoder(option_decoders.TypeVerifier):
  """Decodes the sandbox_warmpool config block into a SandboxWarmPoolSpec."""

  def Decode(
      self, value: Any, component_full_name: str, flag_values: Any
  ) -> SandboxWarmPoolSpec:
    super().Decode(value, component_full_name, flag_values)
    return SandboxWarmPoolSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value,
    )


class K8sAgentSandboxConfigSpec(agent_sandbox_spec.BaseAgentSandboxConfigSpec):
  """Config spec for the Kubernetes agent sandbox."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.manifest_ref: str
    self.namespace: str
    self.sandbox_nodepool: str
    self.manifest_repo: str
    self.controller: ControllerSpec
    self.sandbox_template: SandboxTemplateSpec
    self.sandbox_warmpool: SandboxWarmPoolSpec
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    if self.controller is None:
      self.controller = ControllerSpec(
          '{}.controller'.format(component_full_name), flag_values=flag_values
      )
    if self.sandbox_template is None:
      self.sandbox_template = SandboxTemplateSpec(
          '{}.sandbox_template'.format(component_full_name),
          flag_values=flag_values,
      )
    if self.sandbox_warmpool is None:
      self.sandbox_warmpool = SandboxWarmPoolSpec(
          '{}.sandbox_warmpool'.format(component_full_name),
          flag_values=flag_values,
      )

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'manifest_ref': (
            option_decoders.StringDecoder,
            {'default': _RELEASE},
        ),
        'namespace': (option_decoders.StringDecoder, {'default': 'default'}),
        'sandbox_nodepool': (
            option_decoders.StringDecoder,
            {'default': 'sandbox'},
        ),
        'manifest_repo': (
            option_decoders.StringDecoder,
            {
                'default': (
                    'https://raw.githubusercontent.com/kubernetes-sigs/agent-sandbox'
                )
            },
        ),
        'controller': (_ControllerDecoder, {'default': None, 'none_ok': True}),
        'sandbox_template': (
            _SandboxTemplateDecoder,
            {'default': None, 'none_ok': True},
        ),
        'sandbox_warmpool': (
            _SandboxWarmPoolDecoder,
            {'default': None, 'none_ok': True},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values: dict[str, Any], flag_values: Any) -> None:
    super()._ApplyFlags(config_values, flag_values)
    if flag_values and flag_values['agent_sandbox_manifest_ref'].present:
      config_values['manifest_ref'] = flag_values.agent_sandbox_manifest_ref
