# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Spec for a Kubernetes Inference Server resource."""

from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

DEFAULT_INFERENCE_SERVER_TYPE = 'KubernetesWGServingInferenceServer'


class BaseStaticInferenceServerConfigSpec(spec.BaseSpec):
  """Spec for static inference server configuration."""

  SPEC_TYPE = 'BaseStaticInferenceServerConfigSpec'
  SPEC_ATTRS = ['INFERENCE_SERVER_TYPE']


class StaticInferenceServerConfigDecoder(option_decoders.TypeVerifier):
  """Decodes a static inference server configuration."""

  def __init__(self, inference_server_type: str, **kwargs):
    super().__init__((dict,), **kwargs)
    self.inference_server_type = inference_server_type

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    config_spec_class = spec.GetSpecClass(
        BaseStaticInferenceServerConfigSpec,
        INFERENCE_SERVER_TYPE=self.inference_server_type,
    )
    if isinstance(type(config_spec_class), BaseStaticInferenceServerConfigSpec):
      raise errors.Config.UnrecognizedOption(
          "Inference server type: {} doesn't support static inference server."
          .format(self.inference_server_type)
      )
    return config_spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value
    )


class BaseInferenceServerConfigSpec(spec.BaseSpec):
  """Spec for inference server configuration.

  Attributes:
    inference_server_type: Type of the inference server (e.g.,
      DefaultKubernetesInferenceServer).
    deployment_timeout: Timeout for waiting for the deployment to be available.
    static_inference_server: Details of a static inference server
  """

  SPEC_TYPE = 'BaseInferenceServerConfigSpec'
  SPEC_ATTRS = ['INFERENCE_SERVER_TYPE']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.type: str
    self.deployment_timeout: int
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'type': (
            option_decoders.StringDecoder,
            {'default': DEFAULT_INFERENCE_SERVER_TYPE, 'none_ok': False},
        ),
        'static_inference_server': (
            StaticInferenceServerConfigDecoder,
            {
                'none_ok': True,
                'default': None,
                'inference_server_type': getattr(cls, 'INFERENCE_SERVER_TYPE'),
            },
        ),
    })
    return result


class InferenceServerConfigDecoder(option_decoders.TypeVerifier):
  """Decodes a default inference server configuration."""

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    inference_server_type = (
        value['type'] if 'type' in value else DEFAULT_INFERENCE_SERVER_TYPE
    )
    config_spec_class = GetInferenceServerConfigSpecClass(inference_server_type)
    if isinstance(type(config_spec_class), BaseInferenceServerConfigSpec):
      raise errors.Config.UnrecognizedOption(
          'Unrecognized inference server type: {}.'.format(
              inference_server_type
          )
      )
    return config_spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value
    )


def GetInferenceServerConfigSpecClass(
    inference_server_type: str,
) -> spec.BaseSpecMetaClass | None:
  """Gets the InferenceServerConfigSpec class."""
  return spec.GetSpecClass(
      BaseInferenceServerConfigSpec, INFERENCE_SERVER_TYPE=inference_server_type
  )
