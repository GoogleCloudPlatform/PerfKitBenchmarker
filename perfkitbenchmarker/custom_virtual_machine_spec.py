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
"""Classes relating to decoding a custom machine type.
"""

import re

from perfkitbenchmarker import errors
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.azure import flags as azurestack_flags


class MemoryDecoder(option_decoders.StringDecoder):
  """Verifies and decodes a config option value specifying a memory size."""

  _CONFIG_MEMORY_PATTERN = re.compile(r'([0-9.]+)([GM]iB)')

  def Decode(self, value, component_full_name, flag_values):
    """Decodes memory size in MiB from a string.

    The value specified in the config must be a string representation of the
    memory size expressed in MiB or GiB. It must be an integer number of MiB
    Examples: "1280MiB", "7.5GiB".

    Args:
      value: The value specified in the config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      int. Memory size in MiB.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    string = super(MemoryDecoder, self).Decode(value, component_full_name,
                                               flag_values)
    match = self._CONFIG_MEMORY_PATTERN.match(string)
    if not match:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". Examples of valid values: "1280MiB", '
          '"7.5GiB".'.format(self._GetOptionFullName(component_full_name),
                             string))
    try:
      memory_value = float(match.group(1))
    except ValueError:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". "{2}" is not a valid float.'.format(
              self._GetOptionFullName(component_full_name), string,
              match.group(1)))
    memory_units = match.group(2)
    if memory_units == 'GiB':
      memory_value *= 1024
    memory_mib_int = int(memory_value)
    if memory_value != memory_mib_int:
      raise errors.Config.InvalidValue(
          'Invalid {0} value: "{1}". The specified size must be an integer '
          'number of MiB.'.format(self._GetOptionFullName(component_full_name),
                                  string))
    return memory_mib_int


class CustomMachineTypeSpec(spec.BaseSpec):
  """Properties of a custom machine type.

  Attributes:
    cpus: int. Number of vCPUs.
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
    result = super(CustomMachineTypeSpec, cls)._GetOptionDecoderConstructions()
    result.update({'cpus': (option_decoders.IntDecoder, {'min': 1}),
                   'memory': (MemoryDecoder, {})})
    return result


class MachineTypeDecoder(option_decoders.TypeVerifier):
  """Decodes the machine_type option of a VM config."""

  def __init__(self, **kwargs):
    super(MachineTypeDecoder, self).__init__((basestring, dict), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the machine_type option of a VM config.

    Args:
      value: Either a string name of a machine type or a dict containing
          'cpu' and 'memory' keys describing a custom VM.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      If value is a string, returns it unmodified. Otherwise, returns the
      decoded CustomMachineTypeSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super(MachineTypeDecoder, self).Decode(value, component_full_name,
                                           flag_values)
    if isinstance(value, basestring):
      return value
    return CustomMachineTypeSpec(self._GetOptionFullName(component_full_name),
                                 flag_values=flag_values, **value)


class AzureMachineTypeDecoder(option_decoders.TypeVerifier):
  """Decodes the machine_type option of a VM config."""

  def __init__(self, **kwargs):
    super(AzureMachineTypeDecoder, self).__init__((basestring, dict), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the machine_type option of a VM config.

    Args:
      value: Either a string name of a machine type or a dict containing
          'compute_units' and 'tier' keys describing a machine type.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      If value is a string, returns it unmodified. Otherwise, returns the
      decoded CustomMachineTypeSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super(AzureMachineTypeDecoder, self).Decode(value, component_full_name,
                                                flag_values)
    if isinstance(value, basestring):
      return value
    return AzurePerformanceTierDecoder(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values, **value)


class AzurePerformanceTierDecoder(spec.BaseSpec):
  """Properties of a An Azure custom machine type.

  Attributes:
    compute_units: int. Number of compute units.
    tier: Basic, Standard or Premium
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
        AzurePerformanceTierDecoder, cls)._GetOptionDecoderConstructions()
    # https://docs.microsoft.com/en-us/azure/virtual-machines/windows/acu
    # https://docs.microsoft.com/en-us/azure/sql-database/sql-database-service-tiers
    result.update({'compute_units': (option_decoders.IntDecoder, {'min': 50}),
                   'tier': (option_decoders.EnumDecoder, {
                       'valid_values': azure_flags.VALID_TIERS})})
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values.
        May be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    if flag_values['azure_tier'].present:
      config_values['tier'] = flag_values.azure_tier

    if flag_values['azure_compute_units'].present:
      config_values['compute_units'] = flag_values.azure_compute_units
