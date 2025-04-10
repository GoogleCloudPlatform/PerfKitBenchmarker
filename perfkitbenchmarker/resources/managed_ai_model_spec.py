# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""Spec for a managed AI model resource."""

from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class BaseManagedAiModelSpec(spec.BaseSpec):
  """Spec for a managed AI model resource.

  Attributes:
    model_name: The name of the model to use.
  """

  SPEC_TYPE = 'BaseManagedAiModelSpec'
  SPEC_ATTRS = ['CLOUD', 'MODEL_NAME', 'MODEL_SIZE']
  CLOUD = None
  MODEL_NAME = None
  MODEL_SIZE = None

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.cloud: str
    self.interface: str
    self.model_name: str
    self.model_size: str
    self.max_scale: int
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {
                'valid_values': provider_info.VALID_CLOUDS,
                'default': provider_info.GCP,
            },
        ),
        'interface': (
            option_decoders.EnumDecoder,
            {
                'valid_values': ['CLI', 'SDK', 'MODEL-GARDEN-CLI'],
                'default': 'CLI',
            },
        ),
        'model_name': (
            option_decoders.StringDecoder,
            {
                'none_ok': True,
                'default': '',
            },
        ),
        'model_size': (
            option_decoders.StringDecoder,
            {
                'none_ok': True,
                'default': '',
            },
        ),
        'max_scale': (
            option_decoders.IntDecoder,
            {
                'none_ok': True,
                'default': 1,  # Default to 1 means no scaling.
            },
        ),
    })
    return result


def GetManagedAiModelSpecClass(
    cloud: str, model_name: str, model_size: str
) -> spec.BaseSpecMetaClass | None:
  """Gets the example spec class corresponding to the given attributes."""
  return spec.GetSpecClass(
      BaseManagedAiModelSpec,
      CLOUD=cloud,
      MODEL_NAME=model_name,
      MODEL_SIZE=model_size,
  )
