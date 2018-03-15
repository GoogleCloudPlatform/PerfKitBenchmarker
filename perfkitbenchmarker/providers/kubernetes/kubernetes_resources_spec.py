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
"""Classes relating to decoding a Kubernetes resource limits or requests.
"""

from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class KubernetesResourcesSpec(spec.BaseSpec):
  """Properties representing Kubernetes resources limits or requests.

  Attributes:
    cpus: float. Number of vCPUs.
    memory: string. Representation of the size of memory, expressed in MiB or
        GiB. Must be an integer number of MiB (e.g. "1280MiB", "7.5GiB").
  """

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(
        KubernetesResourcesSpec, cls)._GetOptionDecoderConstructions()
    result.update({'cpus': (option_decoders.FloatDecoder, {'min': 0.1}),
                   'memory': (custom_virtual_machine_spec.MemoryDecoder, {})})
    return result


class KubernetesResourcesDecoder(option_decoders.TypeVerifier):
  """Decodes a kubernetes resources spec."""

  def __init__(self, **kwargs):
    super(KubernetesResourcesDecoder, self).__init__((dict), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the kubernetes resources spec.

    Args:
      value: a dict containing 'cpus' and 'memory' keys.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      The decoded KubernetesResourcesSpec.
    """
    super(KubernetesResourcesDecoder, self).Decode(value, component_full_name,
                                                   flag_values)
    return KubernetesResourcesSpec(self._GetOptionFullName(component_full_name),
                                   flag_values=flag_values, **value)
