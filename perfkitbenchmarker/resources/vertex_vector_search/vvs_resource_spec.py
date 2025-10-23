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
"""Classes relating to decoding a Vertex Vector Search resource."""

from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.resources.vertex_vector_search import flags as vvs_flags


class VVSResourcesSpec(spec.BaseSpec):
  """Properties representing Vertex Vector Search resources.

  Attributes:
    vvs_index_name: The name of the VVS index.
    vvs_deployed_index_id: The ID of the deployed VVS index.
    vvs_networking_type: The networking type for the VVS index.
  """

  CLOUD = ['GCP']

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    self.location: str | None = None
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'location': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'dataset_bucket_url': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'vpc_network': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'machine_type': (
            option_decoders.StringDecoder,
            {'default': vvs_flags.STANDARD},
        ),
        'min_replica_count': (
            option_decoders.IntDecoder,
            {'default': 2},
        ),
        'max_replica_count': (
            option_decoders.IntDecoder,
            {'default': 2},
        ),
        'base_url': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present:
      config_values['cloud'] = flag_values.cloud
    if flag_values['vvs_location'].present:
      config_values['location'] = flag_values.vvs_location
    if flag_values['vvs_dataset_bucket_url'].present:
      config_values['dataset_bucket_url'] = flag_values.vvs_dataset_bucket_url
    if flag_values['vvs_vpc_network'].present:
      config_values['vpc_network'] = flag_values.vvs_vpc_network
    if flag_values['vvs_machine_type'].present:
      config_values['machine_type'] = flag_values.vvs_machine_type
    if flag_values['vvs_min_replica_count'].present:
      config_values['min_replica_count'] = flag_values.vvs_min_replica_count
    if flag_values['vvs_max_replica_count'].present:
      config_values['max_replica_count'] = flag_values.vvs_max_replica_count
    if flag_values['vvs_base_url'].present:
      config_values['base_url'] = flag_values.vvs_base_url


class VVSResourcesDecoder(option_decoders.TypeVerifier):
  """Decodes a Vertex Vector Search resources spec."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict, type(None)), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the Vertex Vector Search resources spec."""
    super().Decode(value, component_full_name, flag_values)
    return VVSResourcesSpec(
        component_full_name=self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **(value or {})
    )
