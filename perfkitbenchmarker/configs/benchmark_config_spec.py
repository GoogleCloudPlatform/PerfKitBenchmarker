# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Classes that verify and transform benchmark configuration input.

See perfkitbenchmarker/configs/__init__.py for more information about
configuration files.
"""

import copy
import os

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


_DEFAULT_DISK_COUNT = 1
_DEFAULT_VM_COUNT = 1


class _FlagsDecoder(option_decoders.TypeVerifier):
  """Processes the flags override dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_FlagsDecoder, self).__init__(default=None, none_ok=True,
                                        valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Processes the flags override dictionary of a benchmark config object.

    Args:
      value: None or dict mapping flag name string to flag override value.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Command-line flag values.

    Returns:
      dict mapping flag name string to Flag object. The flag values to use
      when running the benchmark.
    """
    config_flags = super(_FlagsDecoder, self).Decode(value, component_full_name,
                                                     flag_values)
    merged_flag_values = copy.deepcopy(flag_values)
    if config_flags:
      for key, value in config_flags.iteritems():
        if key not in merged_flag_values:
          raise errors.Config.UnrecognizedOption(
              'Unrecognized option {0}.{1}. Each option within {0} must '
              'correspond to a valid command-line flag.'.format(
                  self._GetOptionFullName(component_full_name), key))
        if not merged_flag_values[key].present:
          try:
            merged_flag_values[key].Parse(value)
          except flags.IllegalFlagValue as e:
            raise errors.Config.InvalidValue(
                'Invalid {0}.{1} value: "{2}" (of type "{3}").{4}{5}'.format(
                    self._GetOptionFullName(component_full_name), key, value,
                    value.__class__.__name__, os.linesep, e))
    return merged_flag_values.FlagDict()


class _PerCloudConfigSpec(spec.BaseSpec):
  """Contains one config dict attribute per cloud provider.

  The name of each attribute is the name of the cloud provider.
  """

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_PerCloudConfigSpec, cls)._GetOptionDecoderConstructions()
    for cloud in providers.VALID_CLOUDS:
      result[cloud] = option_decoders.TypeVerifier, {'default': None,
                                                     'valid_types': (dict,)}
    return result


class _PerCloudConfigDecoder(option_decoders.TypeVerifier):
  """Decodes the disk_spec or vm_spec option of a VM group config object."""

  def __init__(self, **kwargs):
    super(_PerCloudConfigDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the disk_spec or vm_spec option of a VM group config object.

    Args:
      value: None or dict mapping cloud provider name string to a dict.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      _PerCloudConfigSpec decoded from the input dict.
    """
    input_dict = super(_PerCloudConfigDecoder, self).Decode(
        value, component_full_name, flag_values)
    return None if input_dict is None else _PerCloudConfigSpec(
        self._GetOptionFullName(component_full_name), flag_values=flag_values,
        **input_dict)


class _StaticVmDecoder(option_decoders.TypeVerifier):
  """Decodes an item of the static_vms list of a VM group config object."""

  def __init__(self, **kwargs):
    super(_StaticVmDecoder, self).__init__(valid_types=(dict,), **kwargs)

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
    input_dict = super(_StaticVmDecoder, self).Decode(
        value, component_full_name, flag_values)
    return static_virtual_machine.StaticVmSpec(
        self._GetOptionFullName(component_full_name), **input_dict)


class _StaticVmListDecoder(option_decoders.ListDecoder):
  """Decodes the static_vms list of a VM group config object."""

  def __init__(self, **kwargs):
    super(_StaticVmListDecoder, self).__init__(
        default=list, item_decoder=_StaticVmDecoder(), **kwargs)


class _VmGroupSpec(spec.BaseSpec):
  """Configurable options of a VM group.

  Attributes:
    cloud: string. Cloud provider of the VMs in this group.
    disk_count: int. Number of data disks to attach to each VM in this group.
    disk_spec: BaseDiskSpec. Configuration for all data disks to be attached to
        VMs in this group.
    os_type: string. OS type of the VMs in this group.
    static_vms: None or list of StaticVmSpecs. Configuration for all static VMs
        in this group.
    vm_count: int. Number of VMs in this group, including static VMs and
        provisioned VMs.
    vm_spec: BaseVmSpec. Configuration for provisioned VMs in this group.
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_VmGroupSpec, self).__init__(component_full_name,
                                       flag_values=flag_values, **kwargs)
    providers.LoadProvider(self.cloud.lower())
    if self.disk_spec:
      disk_config = getattr(self.disk_spec, self.cloud, None)
      if disk_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.disk_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud))
      disk_spec_class = disk.GetDiskSpecClass(self.cloud)
      self.disk_spec = disk_spec_class(
          '{0}.disk_spec.{1}'.format(component_full_name, self.cloud),
          flag_values=flag_values, **disk_config)
    vm_config = getattr(self.vm_spec, self.cloud, None)
    if vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud))
    vm_spec_class = virtual_machine.GetVmSpecClass(self.cloud)
    self.vm_spec = vm_spec_class(
        '{0}.vm_spec.{1}'.format(component_full_name, self.cloud),
        flag_values=flag_values, **vm_config)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_VmGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            'valid_values': providers.VALID_CLOUDS}),
        'disk_count': (option_decoders.IntDecoder, {
            'default': _DEFAULT_DISK_COUNT, 'min': 0}),
        'disk_spec': (_PerCloudConfigDecoder, {'default': None,
                                               'none_ok': True}),
        'os_type': (option_decoders.EnumDecoder, {
            'valid_values': os_types.ALL}),
        'static_vms': (_StaticVmListDecoder, {}),
        'vm_count': (option_decoders.IntDecoder, {
            'default': _DEFAULT_VM_COUNT, 'min': 0}),
        'vm_spec': (_PerCloudConfigDecoder, {})})
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(_VmGroupSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['os_type'].present or 'os_type' not in config_values:
      config_values['os_type'] = flag_values.os_type
    if 'vm_count' in config_values and config_values['vm_count'] is None:
      config_values['vm_count'] = flag_values.num_vms


class _VmGroupsDecoder(option_decoders.TypeVerifier):
  """Validates the vm_groups dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_VmGroupsDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies vm_groups dictionary of a benchmark config object.

    Args:
      value: dict mapping VM group name string to the corresponding VM group
          config dict.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      dict mapping VM group name string to _VmGroupSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    vm_group_configs = super(_VmGroupsDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = {}
    for vm_group_name, vm_group_config in vm_group_configs.iteritems():
      result[vm_group_name] = _VmGroupSpec(
          '{0}.{1}'.format(self._GetOptionFullName(component_full_name),
                           vm_group_name),
          flag_values=flag_values, **vm_group_config)
    return result


class BenchmarkConfigSpec(spec.BaseSpec):
  """Configurable options of a benchmark run.

  Attributes:
    description: None or string. Description of the benchmark to run.
    flags: flags.FlagValues. Values to use for each flag while executing the
        benchmark.
    vm_groups: dict mapping VM group name string to _VmGroupSpec. Configurable
        options for each VM group used by the benchmark.
  """

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(BenchmarkConfigSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'description': (option_decoders.StringDecoder, {'default': None}),
        'flags': (_FlagsDecoder, {}),
        'vm_groups': (_VmGroupsDecoder, {})})
    return result

  def _DecodeAndInit(self, component_full_name, config, decoders, flag_values):
    """Initializes spec attributes from provided config option values.

    Args:
      component_full_name: string. Fully qualified name of the configurable
          component containing the config options.
      config: dict mapping option name string to option value.
      flag_values: flags.FlagValues. Runtime flags that may override provided
          config option values. These flags have already been applied to the
          current config, but they may be passed to the decoders for propagation
          to deeper spec constructors.
      decoders: OrderedDict mapping option name string to ConfigOptionDecoder.
    """
    # Decode benchmark-specific flags first and use them while decoding the
    # rest of the BenchmarkConfigSpec's options.
    decoders = decoders.copy()
    self.flags = decoders.pop('flags').Decode(config.pop('flags', None),
                                              component_full_name, flag_values)
    with flag_util.FlagDictSubstitution(flag_values, lambda: self.flags):
      super(BenchmarkConfigSpec, self)._DecodeAndInit(
          component_full_name, config, decoders, flag_values)
