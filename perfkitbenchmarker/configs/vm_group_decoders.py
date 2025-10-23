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
"""Module containing a VM group spec and related decoders."""

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.configs import static_vm_decoders

_DEFAULT_DISK_COUNT = 1
_DEFAULT_VM_COUNT = 1


class VmGroupSpec(spec.BaseSpec):
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
    placement_group_name: string. Name of placement group that VM group belongs
      to.
    cidr: subnet each vm in this group belongs to
  """

  cloud: str
  disk_count: int
  disk_type: str
  disk_spec: disk.BaseDiskSpec
  os_type: str
  static_vms: list[static_virtual_machine.StaticVmSpec]
  vm_count: int
  vm_spec: virtual_machine.BaseVmSpec
  vm_as_nfs: bool
  vm_as_nfs_disk_spec: disk.BaseNFSDiskSpec | None
  placement_group_name: str
  cidr: str
  provision_order: int
  provision_delay_seconds: int

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )
    ignore_package_requirements = (
        getattr(flag_values, 'ignore_package_requirements', True)
        if flag_values
        else True
    )
    providers.LoadProvider(self.cloud, ignore_package_requirements)
    provider = self.cloud
    if (
        flag_values
        and flag_values['vm_platform'].present
        and flag_values['vm_platform'].value
        != provider_info.DEFAULT_VM_PLATFORM
    ):
      provider = flag_values['vm_platform'].value
    if self.disk_spec:
      disk_config = getattr(self.disk_spec, provider, None)
      if disk_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.disk_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud)
        )
      disk_type = disk_config.get('disk_type', None)
      if flag_values and flag_values['data_disk_type'].present:
        disk_type = flag_values['data_disk_type'].value
      disk_spec_class = disk.GetDiskSpecClass(self.cloud, disk_type)
      self.disk_spec = disk_spec_class(
          '{}.disk_spec.{}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **disk_config
      )

      if self.vm_as_nfs:
        self.vm_as_nfs_disk_spec = disk.BaseNFSDiskSpec(
            '{}.disk_spec.{}'.format(component_full_name, self.cloud),
            flag_values=flag_values,
            **disk_config
        )
    vm_config = getattr(self.vm_spec, provider, None)
    if vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud)
      )
    vm_spec_class = virtual_machine.GetVmSpecClass(cloud=self.cloud)
    self.vm_spec = vm_spec_class(
        '{}.vm_spec.{}'.format(component_full_name, self.cloud),
        flag_values=flag_values,
        **vm_config
    )

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
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'disk_count': (
            option_decoders.IntDecoder,
            {'default': _DEFAULT_DISK_COUNT, 'min': 0, 'none_ok': True},
        ),
        'vm_as_nfs': (
            option_decoders.BooleanDecoder,
            {'default': False, 'none_ok': True},
        ),
        'disk_spec': (
            spec.PerCloudConfigDecoder,
            {'default': None, 'none_ok': True},
        ),
        'os_type': (
            option_decoders.EnumDecoder,
            {'valid_values': os_types.ALL},
        ),
        'static_vms': (static_vm_decoders.StaticVmListDecoder, {}),
        'vm_count': (
            option_decoders.IntDecoder,
            {'default': _DEFAULT_VM_COUNT, 'min': 0},
        ),
        'cidr': (option_decoders.StringDecoder, {'default': None}),
        'vm_spec': (spec.PerCloudConfigDecoder, {}),
        'placement_group_name': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'provision_order': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'provision_delay_seconds': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0, 'none_ok': True},
        ),
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
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['os_type'].present or 'os_type' not in config_values:
      config_values['os_type'] = flag_values.os_type
    if 'vm_count' in config_values and config_values['vm_count'] is None:
      config_values['vm_count'] = flag_values.num_vms


class VmGroupsDecoder(option_decoders.TypeVerifier):
  """Validates the vm_groups dictionary of a benchmark config object."""

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
    vm_group_configs = super().Decode(
        value, component_full_name, flag_values
    )
    result = {}
    for vm_group_name, vm_group_config in vm_group_configs.items():
      result[vm_group_name] = VmGroupSpec(
          '{}.{}'.format(
              self._GetOptionFullName(component_full_name), vm_group_name
          ),
          flag_values=flag_values,
          **vm_group_config
      )
    return result


class VmGroupSpecDecoder(option_decoders.TypeVerifier):
  """Validates a single VmGroupSpec dictionary."""

  def Decode(self, value, component_full_name, flag_values):
    """Verifies vm_groups dictionary of a benchmark config object.

    Args:
      value: dict corresponding to a VM group config.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      dict a _VmGroupSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    vm_group_config = super().Decode(
        value, component_full_name, flag_values
    )
    return VmGroupSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **vm_group_config
    )
