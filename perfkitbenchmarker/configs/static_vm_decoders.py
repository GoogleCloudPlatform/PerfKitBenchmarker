# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing Static VM Decoders."""

from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker.configs import option_decoders


class StaticVmDecoder(option_decoders.TypeVerifier):
  """Decodes an item of the static_vms list of a VM group config object."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes an item of the static_vms list of a VM group config object.

    Args:
      value: dict mapping static VM config option name string to corresponding
        option value.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      StaticVmSpec decoded from the input dict.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    input_dict = super().Decode(value, component_full_name, flag_values)
    return static_virtual_machine.StaticVmSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **input_dict)


class StaticVmListDecoder(option_decoders.ListDecoder):
  """Decodes the static_vms list of a VM group config object."""

  def __init__(self, **kwargs):
    super().__init__(default=list, item_decoder=StaticVmDecoder(), **kwargs)
