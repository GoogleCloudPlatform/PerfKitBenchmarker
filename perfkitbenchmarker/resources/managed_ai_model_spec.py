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

  SPEC_TYPE = 'BaseManagedModelSpec'
  SPEC_ATTRS = []

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.cloud: str
    self.model_name: str
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

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
        'model_name': (
            option_decoders.StringDecoder,
            {
                'none_ok': True,
                'default': '',
            },
        ),
    })
    return result
