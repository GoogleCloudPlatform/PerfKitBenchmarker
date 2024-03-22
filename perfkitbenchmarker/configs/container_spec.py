# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Classes that verify and transform configuration input for containers.

See perfkitbenchmarker/configs/__init__.py for more information about
configuration files.
"""

from typing import Any, Optional

from absl import flags
from perfkitbenchmarker import custom_virtual_machine_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
import six


_DEFAULT_VM_COUNT = 1
KUBERNETES = 'Kubernetes'
DEFAULT_NODEPOOL = 'default'


class ContainerSpec(spec.BaseSpec):
  """Class containing options for creating containers."""

  def __init__(
      self,
      component_full_name: str,
      flag_values: Optional[flags.FlagValues] = None,
      **kwargs: Any,
  ):
    super().__init__(component_full_name, flag_values, **kwargs)
    self.cpus: float
    self.memory: int
    self.command: list[str]
    self.image: str
    self.container_port: int
    self.static_image: bool

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ):
    """Apply flag settings to the container spec."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['image'].present:
      config_values['image'] = flag_values.image
    if flag_values['static_container_image'].present:
      config_values['static_image'] = flag_values.static_container_image

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'image': (option_decoders.StringDecoder, {'default': None}),
        'static_image': (option_decoders.BooleanDecoder, {'default': False}),
        'cpus': (option_decoders.FloatDecoder, {'default': None}),
        'memory': (
            custom_virtual_machine_spec.MemoryDecoder,
            {'default': None},
        ),
        'command': (_CommandDecoder, {}),
        'container_port': (option_decoders.IntDecoder, {'default': 8080}),
    })
    return result


class _CommandDecoder(option_decoders.ListDecoder):
  """Decodes the command/arg list for containers."""

  def __init__(self, **kwargs):
    super().__init__(
        default=None,
        none_ok=True,
        item_decoder=option_decoders.StringDecoder(),
        **kwargs,
    )


class ContainerRegistrySpec(spec.BaseSpec):
  """Spec containing options for creating a Container Registry."""

  def __init__(
      self,
      component_full_name: str,
      flag_values: Optional[flags.FlagValues] = None,
      **kwargs: Any,
  ):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    self.spec: Optional[dict[str, Any]]
    self.cloud: str
    registry_spec = getattr(self.spec, self.cloud, {})
    self.project: Optional[str] = registry_spec.get('project')
    self.zone: Optional[str] = registry_spec.get('zone')
    self.name: Optional[str] = registry_spec.get('name')
    self.cpus: float
    self.memory: int
    self.command: list[str]
    self.image: str
    self.container_port: int
    self.cloud: str

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ):
    """Apply flag values to the spec."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    updated_spec = {}
    if flag_values['project'].present:
      updated_spec['project'] = flag_values.project
    if flag_values['zone'].present:
      updated_spec['zone'] = flag_values.zone[0]
    cloud = config_values['cloud']
    cloud_spec = config_values.get('spec', {}).get(cloud, {})
    cloud_spec.update(updated_spec)
    config_values['spec'] = {cloud: cloud_spec}

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.StringDecoder, {}),
        'spec': (spec.PerCloudConfigDecoder, {'default': {}}),
    })
    return result


class ContainerRegistryDecoder(option_decoders.TypeVerifier):
  """Validates the container_registry dictionary of a benchmark config."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies container_registry dictionary of a benchmark config object.

    Args:
      value: dict mapping VM group name string to the corresponding container
        spec config dict.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      dict mapping container spec name string to ContainerSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    vm_group_config = super().Decode(value, component_full_name, flag_values)
    return ContainerRegistrySpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **vm_group_config,
    )


class ContainerSpecsDecoder(option_decoders.TypeVerifier):
  """Validates the container_specs dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies container_specs dictionary of a benchmark config object.

    Args:
      value: dict mapping VM group name string to the corresponding container
        spec config dict.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      dict mapping container spec name string to ContainerSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    container_spec_configs = super().Decode(
        value, component_full_name, flag_values
    )
    result = {}
    for spec_name, spec_config in six.iteritems(container_spec_configs):
      result[spec_name] = ContainerSpec(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), spec_name
          ),
          flag_values=flag_values,
          **spec_config,
      )
    return result


class NodepoolSpec(spec.BaseSpec):
  """Configurable options of a Nodepool."""

  vm_spec: spec.PerCloudConfigSpec

  def __init__(
      self, component_full_name, group_name, flag_values=None, **kwargs
  ):
    super().__init__(
        '{0}.{1}'.format(component_full_name, group_name),
        flag_values=flag_values,
        **kwargs,
    )
    self.vm_count: int
    self.vm_spec: spec.PerCloudConfigSpec
    self.sandbox_config: Optional[SandboxSpec]

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
        'vm_count': (
            option_decoders.IntDecoder,
            {'default': _DEFAULT_VM_COUNT, 'min': 0},
        ),
        'vm_spec': (spec.PerCloudConfigDecoder, {}),
        'sandbox_config': (_SandboxDecoder, {'default': None}),
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
    if flag_values['container_cluster_num_vms'].present:
      config_values['vm_count'] = flag_values.container_cluster_num_vms

    # Need to apply the first zone in the zones flag, if specified,
    # to the spec. _NodepoolSpec does not currently support
    # running in multiple zones in a single PKB invocation.
    if flag_values['zone'].present:
      for cloud in config_values['vm_spec']:
        config_values['vm_spec'][cloud]['zone'] = flag_values.zone[0]


class _NodepoolsDecoder(option_decoders.TypeVerifier):
  """Validate the nodepool dictionary of a nodepools config object."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verify Nodepool dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _NodepoolsDecoder built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    nodepools_configs = super(_NodepoolsDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = {}
    for nodepool_name, nodepool_config in six.iteritems(nodepools_configs):
      result[nodepool_name] = NodepoolSpec(
          self._GetOptionFullName(component_full_name),
          nodepool_name,
          flag_values,
          **nodepool_config,
      )
    return result


class SandboxSpec(spec.BaseSpec):
  """Configurable options for sandboxed node pools."""

  def __init__(self, *args, **kwargs):
    self.type: str = None
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'type': (
            option_decoders.StringDecoder,
            {'none_ok': True, 'default': None},
        ),
    })
    return result

  def ToSandboxFlag(self):
    """Returns the string value to pass to gcloud's --sandbox flag."""
    return 'type=%s' % (self.type,)


class _SandboxDecoder(option_decoders.TypeVerifier):
  """Decodes the sandbox configuration option of a nodepool."""

  def __init__(self, **kwargs):
    super(_SandboxDecoder, self).__init__((dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the sandbox configuration option of a nodepool.

    Args:
      value: Dictionary with the sandbox config.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      Returns the decoded _SandboxSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super().Decode(value, component_full_name, flag_values)
    return SandboxSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value,
    )


class ContainerClusterSpec(spec.BaseSpec):
  """Spec containing info needed to create a container cluster."""

  cloud: str
  vm_spec: spec.PerCloudConfigSpec
  nodepools: dict[str, NodepoolSpec]

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    ignore_package_requirements = (
        getattr(flag_values, 'ignore_package_requirements', True)
        if flag_values
        else True
    )
    # TODO(user): Remove LoadProvider cloud once cloud != kubernetes.
    providers.LoadProvider('kubernetes', ignore_package_requirements)
    providers.LoadProvider(self.cloud, ignore_package_requirements)
    vm_config = getattr(self.vm_spec, self.cloud, None)
    if vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud)
      )
    vm_spec_class = virtual_machine.GetVmSpecClass(
        self.cloud, provider_info.DEFAULT_VM_PLATFORM
    )
    self.vm_spec = vm_spec_class(
        '{0}.vm_spec.{1}'.format(component_full_name, self.cloud),
        flag_values=flag_values,
        **vm_config,
    )
    nodepools = {}
    for nodepool_name, nodepool_spec in sorted(six.iteritems(self.nodepools)):
      if nodepool_name == DEFAULT_NODEPOOL:
        raise errors.Config.InvalidValue(
            'Nodepool name {0} is reserved for use during cluster creation. '
            'Please rename nodepool'.format(nodepool_name)
        )
      nodepool_config = getattr(nodepool_spec.vm_spec, self.cloud, None)
      if nodepool_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud)
        )
      nodepool_spec.vm_spec = vm_spec_class(
          '{0}.vm_spec.{1}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **nodepool_config,
      )
      nodepools[nodepool_name] = nodepool_spec

    self.nodepools = nodepools

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
        'static_cluster': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'type': (
            option_decoders.StringDecoder,
            {
                'default': KUBERNETES,
            },
        ),
        'vm_count': (
            option_decoders.IntDecoder,
            {'default': _DEFAULT_VM_COUNT, 'min': 0},
        ),
        'min_vm_count': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True, 'min': 0},
        ),
        'max_vm_count': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True, 'min': 0},
        ),
        # vm_spec is used to define the machine type for the default nodepool
        'vm_spec': (spec.PerCloudConfigDecoder, {}),
        # nodepools specifies a list of additional nodepools to create alongside
        # the default nodepool (nodepool created on cluster creation).
        'nodepools': (_NodepoolsDecoder, {'default': {}, 'none_ok': True}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['container_cluster_type'].present:
      config_values['type'] = flag_values.container_cluster_type
    if flag_values['container_cluster_num_vms'].present:
      config_values['vm_count'] = flag_values.container_cluster_num_vms

    # Need to apply the first zone in the zones flag, if specified,
    # to the spec. ContainerClusters do not currently support
    # running in multiple zones in a single PKB invocation.
    if flag_values['zone'].present:
      for cloud in config_values['vm_spec']:
        config_values['vm_spec'][cloud]['zone'] = flag_values.zone[0]


class ContainerClusterSpecDecoder(option_decoders.TypeVerifier):
  """Validates a ContainerClusterSpec dictionary."""

  def __init__(self, **kwargs):
    super().__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies container_cluster dictionary of a benchmark config object."""
    cluster_config = super().Decode(value, component_full_name, flag_values)

    return ContainerClusterSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **cluster_config,
    )
