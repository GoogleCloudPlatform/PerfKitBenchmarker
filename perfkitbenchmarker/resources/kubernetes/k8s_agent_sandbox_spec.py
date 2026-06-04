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

from absl import flags

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.resources import agent_sandbox_spec

flags.DEFINE_string(
    'agent_sandbox_manifest_ref', None,
    'agent-sandbox release ref (tag or SHA) for CRD, RBAC, and controller '
    'manifests.')
flags.DEFINE_string(
    'agent_sandbox_namespace', None,
    'Namespace in which SandboxClaims are created.')
flags.DEFINE_string(
    'agent_sandbox_runtime_class', None, 'RuntimeClass for sandbox pods.')
flags.DEFINE_integer(
    'agent_sandbox_warmpool_replicas', None,
    'SandboxWarmPool size to provision in Prepare.')
flags.DEFINE_string(
    'agent_sandbox_controller_image', None, 'Controller container image.')
flags.DEFINE_integer(
    'agent_sandbox_controller_claim_workers', None,
    'Controller --sandbox-claim-concurrent-workers value.')
flags.DEFINE_integer(
    'agent_sandbox_controller_sandbox_workers', None,
    'Controller --sandbox-concurrent-workers value.')
flags.DEFINE_integer(
    'agent_sandbox_controller_warmpool_workers', None,
    'Controller --sandbox-warm-pool-concurrent-workers value.')
flags.DEFINE_integer(
    'agent_sandbox_controller_warmpool_max_batch_size', None,
    'Controller --sandbox-warm-pool-max-batch-size value.')
flags.DEFINE_integer(
    'agent_sandbox_controller_kube_api_burst', None,
    'Controller --kube-api-burst value.')
flags.DEFINE_integer(
    'agent_sandbox_controller_kube_api_qps', None,
    'Controller --kube-api-qps value.')
flags.DEFINE_boolean(
    'agent_sandbox_controller_enable_tracing', False,
    'Enable controller OpenTelemetry tracing.')
flags.DEFINE_string(
    'agent_sandbox_controller_otel_endpoint', None,
    'OTLP exporter endpoint when tracing is enabled.')
flags.DEFINE_boolean(
    'agent_sandbox_controller_leader_elect', False,
    'Whether the controller runs with leader election enabled.')

_DEFAULT_MANIFEST_REF = '32c4f231a116f76eb707fe34510b8143d61268ae'
_DEFAULT_CONTROLLER_IMAGE = (
    'us-central1-docker.pkg.dev/k8s-staging-images/agent-sandbox/'
    'agent-sandbox-controller:v20260527-v0.4.6-31-gd43447b-main')
_DEFAULT_SANDBOX_IMAGE = (
    'registry.k8s.io/agent-sandbox/python-runtime-sandbox:v0.4.6')


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
    self.enable_tracing: bool
    self.otel_endpoint: str | None
    self.leader_elect: bool
    self.cpu_request: str
    self.cpu_limit: str
    self.memory_request: str
    self.memory_limit: str
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'image': (option_decoders.StringDecoder,
                  {'default': _DEFAULT_CONTROLLER_IMAGE}),
        'claim_workers': (option_decoders.IntDecoder,
                          {'default': None, 'none_ok': True}),
        'sandbox_workers': (option_decoders.IntDecoder,
                            {'default': None, 'none_ok': True}),
        'warmpool_workers': (option_decoders.IntDecoder,
                             {'default': None, 'none_ok': True}),
        'warmpool_max_batch_size': (option_decoders.IntDecoder,
                                    {'default': None, 'none_ok': True}),
        'kube_api_burst': (option_decoders.IntDecoder,
                           {'default': None, 'none_ok': True}),
        'kube_api_qps': (option_decoders.IntDecoder,
                         {'default': None, 'none_ok': True}),
        'enable_tracing': (option_decoders.BooleanDecoder, {'default': False}),
        'otel_endpoint': (option_decoders.StringDecoder,
                          {'default': None, 'none_ok': True}),
        'leader_elect': (option_decoders.BooleanDecoder, {'default': False}),
        'cpu_request': (option_decoders.StringDecoder, {'default': '500m'}),
        'cpu_limit': (option_decoders.StringDecoder, {'default': '2'}),
        'memory_request': (option_decoders.StringDecoder, {'default': '256Mi'}),
        'memory_limit': (option_decoders.StringDecoder, {'default': '1Gi'}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['agent_sandbox_controller_image'].present:
      config_values['image'] = flag_values.agent_sandbox_controller_image
    for flag_name, key in (
        ('agent_sandbox_controller_claim_workers', 'claim_workers'),
        ('agent_sandbox_controller_sandbox_workers', 'sandbox_workers'),
        ('agent_sandbox_controller_warmpool_workers', 'warmpool_workers'),
        ('agent_sandbox_controller_warmpool_max_batch_size',
         'warmpool_max_batch_size'),
        ('agent_sandbox_controller_kube_api_burst', 'kube_api_burst'),
        ('agent_sandbox_controller_kube_api_qps', 'kube_api_qps'),
        ('agent_sandbox_controller_otel_endpoint', 'otel_endpoint'),
        ('agent_sandbox_controller_enable_tracing', 'enable_tracing'),
        ('agent_sandbox_controller_leader_elect', 'leader_elect'),
    ):
      if flag_values[flag_name].present:
        config_values[key] = flag_values[flag_name].value


class SandboxTemplateSpec(spec.BaseSpec):
  """Config for the SandboxTemplate (models SandboxTemplateSpec).

  Pod-shape fields (runtime_class, image, resources, labels) are rendered into
  the template. The remaining fields are accepted and validated stubs, not yet
  rendered.
  """

  def __init__(self, *args, **kwargs):
    self.runtime_class: str
    self.image: str
    self.cpu_request: str
    self.cpu_limit: str
    self.memory_request: str
    self.memory_limit: str
    self.labels: dict | None
    self.command: list | None
    self.args: list | None
    self.env: dict | None
    self.service_account: str | None
    self.annotations: dict | None
    self.network_policy_management: str
    self.env_vars_injection_policy: str
    self.service: bool | None
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'runtime_class': (option_decoders.StringDecoder, {'default': 'runsc'}),
        'image': (option_decoders.StringDecoder,
                  {'default': _DEFAULT_SANDBOX_IMAGE}),
        'cpu_request': (option_decoders.StringDecoder, {'default': '100m'}),
        'cpu_limit': (option_decoders.StringDecoder, {'default': '500m'}),
        'memory_request': (option_decoders.StringDecoder, {'default': '256Mi'}),
        'memory_limit': (option_decoders.StringDecoder, {'default': '1Gi'}),
        'labels': (option_decoders.TypeVerifier,
                   {'default': None, 'none_ok': True}),
        'command': (option_decoders.ListDecoder,
                    {'item_decoder': option_decoders.StringDecoder(),
                     'default': None, 'none_ok': True}),
        'args': (option_decoders.ListDecoder,
                 {'item_decoder': option_decoders.StringDecoder(),
                  'default': None, 'none_ok': True}),
        'env': (option_decoders.TypeVerifier,
                {'default': None, 'none_ok': True}),
        'service_account': (option_decoders.StringDecoder,
                            {'default': None, 'none_ok': True}),
        'annotations': (option_decoders.TypeVerifier,
                        {'default': None, 'none_ok': True}),
        'network_policy_management': (option_decoders.EnumDecoder,
                                      {'valid_values': ['Managed', 'Unmanaged'],
                                       'default': 'Managed'}),
        'env_vars_injection_policy': (
            option_decoders.EnumDecoder,
            {'valid_values': ['Disallowed', 'Allowed', 'Overrides'],
             'default': 'Disallowed'}),
        'service': (option_decoders.BooleanDecoder,
                    {'default': None, 'none_ok': True}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['agent_sandbox_runtime_class'].present:
      config_values['runtime_class'] = flag_values.agent_sandbox_runtime_class


class SandboxWarmPoolSpec(spec.BaseSpec):
  """Config for the SandboxWarmPool (models SandboxWarmPoolSpec)."""

  def __init__(self, *args, **kwargs):
    self.replicas: int
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'replicas': (option_decoders.IntDecoder, {'default': 0, 'min': 0}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['agent_sandbox_warmpool_replicas'].present:
      config_values['replicas'] = flag_values.agent_sandbox_warmpool_replicas


class _ControllerDecoder(option_decoders.TypeVerifier):
  """Decodes the controller config block into a ControllerSpec."""

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    return ControllerSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values, **value)


class _SandboxTemplateDecoder(option_decoders.TypeVerifier):
  """Decodes the sandbox_template config block into a SandboxTemplateSpec."""

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    return SandboxTemplateSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values, **value)


class _SandboxWarmPoolDecoder(option_decoders.TypeVerifier):
  """Decodes the sandbox_warmpool config block into a SandboxWarmPoolSpec."""

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    return SandboxWarmPoolSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values, **value)


class K8sAgentSandboxConfigSpec(agent_sandbox_spec.BaseAgentSandboxConfigSpec):
  """Config spec for the Kubernetes agent sandbox."""

  SANDBOX_TYPE = agent_sandbox_spec.DEFAULT_SANDBOX_TYPE

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.manifest_ref: str
    self.namespace: str
    self.controller: ControllerSpec
    self.sandbox_template: SandboxTemplateSpec
    self.sandbox_warmpool: SandboxWarmPoolSpec
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    if self.controller is None:
      self.controller = ControllerSpec(
          '{}.controller'.format(component_full_name), flag_values=flag_values)
    if self.sandbox_template is None:
      self.sandbox_template = SandboxTemplateSpec(
          '{}.sandbox_template'.format(component_full_name),
          flag_values=flag_values)
    if self.sandbox_warmpool is None:
      self.sandbox_warmpool = SandboxWarmPoolSpec(
          '{}.sandbox_warmpool'.format(component_full_name),
          flag_values=flag_values)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'manifest_ref': (option_decoders.StringDecoder,
                         {'default': _DEFAULT_MANIFEST_REF}),
        'namespace': (option_decoders.StringDecoder, {'default': 'default'}),
        'controller': (_ControllerDecoder, {'default': None, 'none_ok': True}),
        'sandbox_template': (_SandboxTemplateDecoder,
                             {'default': None, 'none_ok': True}),
        'sandbox_warmpool': (_SandboxWarmPoolDecoder,
                             {'default': None, 'none_ok': True}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['agent_sandbox_manifest_ref'].present:
      config_values['manifest_ref'] = flag_values.agent_sandbox_manifest_ref
    if flag_values['agent_sandbox_namespace'].present:
      config_values['namespace'] = flag_values.agent_sandbox_namespace
