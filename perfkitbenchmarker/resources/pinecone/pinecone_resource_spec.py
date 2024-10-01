# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Classes relating to decoding a pinecone resource."""

from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.resources.pinecone import flags as pinecone_flags


class PineconeResourcesSpec(spec.BaseSpec):
  """Properties representing pinecone.

  Attributes:
    server_type: The type of server to create.
  """

  CLOUD = ['AWS', 'GCP', 'Azure']

  def __init__(self, *args, **kwargs):
    self.server_type: str | None = None
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'server_type': (
            option_decoders.EnumDecoder,
            {
                'default': pinecone_flags.POD,
                'valid_values': [
                    pinecone_flags.POD,
                    pinecone_flags.SERVERLESS,
                ],
            },
        ),
        'server_environment': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'server_pod_type': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'server_api_key': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'server_replicas': (option_decoders.IntDecoder, {'default': 1}),
        'server_shards': (option_decoders.IntDecoder, {'default': 1}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present:
      config_values['cloud'] = flag_values.cloud

    if flag_values['pinecone_server_type'].present:
      config_values['server_type'] = flag_values.pinecone_server_type
    if flag_values['pinecone_server_environment'].present:
      config_values['server_environment'] = (
          flag_values.pinecone_server_environment
      )
    config_values['server_pod_type'] = flag_values.pinecone_server_pod_type
    if flag_values['pinecone_api_key'].present:
      config_values['server_api_key'] = flag_values.pinecone_api_key
    if flag_values['pinecone_server_replicas'].present:
      config_values['server_replicas'] = flag_values.pinecone_server_replicas
    if flag_values['pinecone_server_shards'].present:
      config_values['server_shards'] = flag_values.pinecone_server_shards


class PineconeResourcesDecoder(option_decoders.TypeVerifier):
  """Decodes a pinecone resources spec."""

  def __init__(self, **kwargs):
    super().__init__((dict), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the pinecone resources spec.

    Args:
      value: a dict containing 'cpus' and 'memory' keys.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      The decoded PineconeResourcesSpec.
    """
    super().Decode(value, component_full_name, flag_values)
    return PineconeResourcesSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value
    )
