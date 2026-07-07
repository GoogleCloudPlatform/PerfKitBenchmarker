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
"""Spec for an agent sandbox resource."""

from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

DEFAULT_SANDBOX_TYPE = 'Kubernetes'


class BaseAgentSandboxConfigSpec(spec.BaseSpec):
  """Spec for agent sandbox configuration.

  Attributes:
    type: Type of the agent sandbox (for example Kubernetes or a managed
      offering).
  """

  SPEC_TYPE = 'BaseAgentSandboxConfigSpec'
  SPEC_ATTRS = ['SANDBOX_TYPE']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.type: str
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'type': (
            option_decoders.StringDecoder,
            {'default': DEFAULT_SANDBOX_TYPE, 'none_ok': False},
        ),
    })
    return result


class AgentSandboxConfigDecoder(option_decoders.TypeVerifier):
  """Decodes an agent sandbox configuration block."""

  def __init__(self, **kwargs):
    super().__init__((dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    super().Decode(value, component_full_name, flag_values)
    sandbox_type = value['type'] if 'type' in value else DEFAULT_SANDBOX_TYPE
    config_spec_class = GetAgentSandboxConfigSpecClass(sandbox_type)
    if config_spec_class is None:
      raise errors.Config.UnrecognizedOption(
          'Unrecognized agent sandbox type: {}.'.format(sandbox_type)
      )
    return config_spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value
    )


def GetAgentSandboxConfigSpecClass(sandbox_type):
  """Gets the AgentSandboxConfigSpec class for the given type."""
  return spec.GetSpecClass(
      BaseAgentSandboxConfigSpec, SANDBOX_TYPE=sandbox_type
  )
