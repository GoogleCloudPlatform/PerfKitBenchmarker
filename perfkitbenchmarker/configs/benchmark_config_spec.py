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
"""Classes that verify and transform benchmark configuration input.

See perfkitbenchmarker/configs/__init__.py for more information about
configuration files.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import logging
import os

from perfkitbenchmarker import app_service
from perfkitbenchmarker import container_service
from perfkitbenchmarker import disk
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import os_types
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import static_virtual_machine
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.dpb_service import BaseDpbService
import six

_DEFAULT_DISK_COUNT = 1
_DEFAULT_VM_COUNT = 1


class _DpbApplicationListDecoder(option_decoders.ListDecoder):
  """Decodes the list of applications to be enabled on the dpb service."""

  def __init__(self, **kwargs):
    super(_DpbApplicationListDecoder, self).__init__(
        default=None,
        item_decoder=option_decoders.EnumDecoder(
            [dpb_service.FLINK, dpb_service.HIVE]),
        **kwargs)


class _DpbServiceDecoder(option_decoders.TypeVerifier):
  """Validates the dpb service dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_DpbServiceDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies dpb service dictionary of a benchmark config object.

    Args:
      value: dict Dpb Service config dictionary
      component_full_name: string.  Fully qualified name of the configurable
      component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated
      to BaseSpec constructors.
    Returns:
      _DpbServiceSpec Build from the config passed in in value.
    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    dpb_service_config = super(_DpbServiceDecoder, self).Decode(
        value, component_full_name, flag_values)

    if (dpb_service_config['service_type'] == dpb_service.EMR and
        component_full_name == 'dpb_wordcount_benchmark'):
      if flag_values.dpb_wordcount_fs != BaseDpbService.S3_FS:
        raise errors.Config.InvalidValue('EMR service requires S3.')
    result = _DpbServiceSpec(self._GetOptionFullName(component_full_name),
                             flag_values, **dpb_service_config)
    return result


class _DpbServiceSpec(spec.BaseSpec):
  """Configurable options of an Distributed Processing Backend Service.

    We may add more options here, such as disk specs, as necessary.
    When there are flags for these attributes, the convention is that
    the flag is prefixed with dpb.
    Attributes:
      service_type: string.  pkb_managed or dataflow,dataproc,emr, etc.
      static_dpb_service_instance: if user has pre created a container, the id
      worker_group: Vm group spec for workers.
      worker_count: the number of workers part of the dpb service
      applications: An enumerated list of applications that need
        to be enabled on the dpb service
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_DpbServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict)
      pair. The pair specifies a decoder class and its __init__() keyword
      arguments to construct in order to decode the named option.
    """
    result = super(_DpbServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'static_dpb_service_instance': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True
        }),
        'service_type': (option_decoders.EnumDecoder, {
            'default':
                dpb_service.DATAPROC,
            'valid_values': [
                dpb_service.DATAPROC, dpb_service.DATAFLOW, dpb_service.EMR
            ]
        }),
        'worker_group': (_VmGroupSpecDecoder, {}),
        'worker_count': (option_decoders.IntDecoder, {
            'default': dpb_service.DEFAULT_WORKER_COUNT,
            'min': 2
        }),
        'applications': (_DpbApplicationListDecoder, {})
    })
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
    super(_DpbServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['static_dpb_service_instance'].present:
      config_values['static_dpb_service_instance'] = (
          flag_values.static_dpb_service_instance)
    # TODO(saksena): Update the documentation for zones assignment
    if flag_values['zones'].present:
      group = 'worker_group'
      if group in config_values:
        for cloud in config_values[group]['vm_spec']:
          config_values[group]['vm_spec'][cloud]['zone'] = (
              flag_values.zones[0])


class _TpuGroupSpec(spec.BaseSpec):
  """Configurable options of a TPU."""

  def __init__(self, component_full_name, group_name, flag_values=None,
               **kwargs):
    super(_TpuGroupSpec, self).__init__('{0}.{1}'.format(
        component_full_name, group_name), flag_values=flag_values, **kwargs)
    if not self.tpu_name:
      self.tpu_name = 'pkb-tpu-{group_name}-{run_uri}'.format(
          group_name=group_name, run_uri=flag_values.run_uri)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_TpuGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            'valid_values': providers.VALID_CLOUDS
        }),
        'tpu_cidr_range': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_accelerator_type': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_description': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_network': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_tf_version': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_zone': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_name': (option_decoders.StringDecoder, {
            'default': None
        }),
        'tpu_preemptible': (option_decoders.BooleanDecoder, {
            'default': False
        })
    })
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
    super(_TpuGroupSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present:
      config_values['cloud'] = flag_values.cloud
    if flag_values['tpu_cidr_range'].present:
      config_values['tpu_cidr_range'] = flag_values.tpu_cidr_range
    if flag_values['tpu_accelerator_type'].present:
      config_values['tpu_accelerator_type'] = flag_values.tpu_accelerator_type
    if flag_values['tpu_description'].present:
      config_values['tpu_description'] = flag_values.tpu_description
    if flag_values['tpu_network'].present:
      config_values['tpu_network'] = flag_values.tpu_network
    if flag_values['tpu_tf_version'].present:
      config_values['tpu_tf_version'] = flag_values.tpu_tf_version
    if flag_values['tpu_zone'].present:
      config_values['tpu_zone'] = flag_values.tpu_zone
    if flag_values['tpu_name'].present:
      config_values['tpu_name'] = flag_values.tpu_name
    if flag_values['tpu_preemptible'].present:
      config_values['tpu_preemptible'] = flag_values.tpu_preemptible


class _EdwServiceDecoder(option_decoders.TypeVerifier):
  """Validates the edw service dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_EdwServiceDecoder, self).__init__(
        valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies edw service dictionary of a benchmark config object.

    Args:
      value: dict edw service config dictionary
      component_full_name: string.  Fully qualified name of the configurable
      component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated
      to BaseSpec constructors.
    Returns:
      _EdwServiceSpec Built from the config passed in in value.
    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    edw_service_config = super(_EdwServiceDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = _EdwServiceSpec(self._GetOptionFullName(
        component_full_name), flag_values, **edw_service_config)
    return result


class _EdwServiceSpec(spec.BaseSpec):
  """Configurable options of an EDW service.

    When there are flags for these attributes, the convention is that
    the flag is prefixed with edw_service.

  Attributes:
    cluster_name  : string. If set, the name of the cluster
    type: string. The type of EDW service (redshift)
    node_type: string, type of node comprising the cluster
    node_count: integer, number of nodes in the cluster
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_EdwServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments to
      construct in order to decode the named option.
    """
    result = super(_EdwServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'type': (option_decoders.StringDecoder, {
            'default': 'redshift',
            'none_ok': False}),
        'cluster_identifier': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'endpoint': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'concurrency': (option_decoders.IntDecoder, {
            'default': 5,
            'none_ok': True}),
        'db': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'user': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'password': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'node_type': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'node_count': (option_decoders.IntDecoder, {
            'default': edw_service.DEFAULT_NUMBER_OF_NODES,
            'min': edw_service.DEFAULT_NUMBER_OF_NODES}),
        'snapshot': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'cluster_subnet_group': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'cluster_parameter_group': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'resource_group': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'server_name': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True}),
        'iam_role': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True})
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
    super(_EdwServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    # TODO(saksena): Add cluster_subnet_group and cluster_parameter_group flags
    # Restoring from a snapshot, so defer to the user supplied cluster details
    if flag_values['edw_service_cluster_snapshot'].present:
      config_values['snapshot'] = flag_values.edw_service_cluster_snapshot
    if flag_values['edw_service_cluster_identifier'].present:
      config_values['cluster_identifier'] = (
          flag_values.edw_service_cluster_identifier)
    if flag_values['edw_service_endpoint'].present:
      config_values['endpoint'] = flag_values.edw_service_endpoint
    if flag_values['edw_service_cluster_concurrency'].present:
      config_values['concurrency'] = flag_values.edw_service_cluster_concurrency
    if flag_values['edw_service_cluster_db'].present:
      config_values['db'] = flag_values.edw_service_cluster_db
    if flag_values['edw_service_cluster_user'].present:
      config_values['user'] = flag_values.edw_service_cluster_user
    if flag_values['edw_service_cluster_password'].present:
      config_values['password'] = flag_values.edw_service_cluster_password


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
    input_dict = super(_StaticVmDecoder,
                       self).Decode(value, component_full_name, flag_values)
    return static_virtual_machine.StaticVmSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **input_dict)


class _StaticVmListDecoder(option_decoders.ListDecoder):
  """Decodes the static_vms list of a VM group config object."""

  def __init__(self, **kwargs):
    super(_StaticVmListDecoder, self).__init__(
        default=list, item_decoder=_StaticVmDecoder(), **kwargs)


class _RelationalDbSpec(spec.BaseSpec):
  """Configurable options of a database service."""

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_RelationalDbSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)
    # TODO(ferneyhough): This is a lot of boilerplate, and is repeated
    # below in VmGroupSpec. See if some can be consolidated. Maybe we can
    # specify a VmGroupSpec instead of both vm_spec and disk_spec.
    ignore_package_requirements = (getattr(flag_values,
                                           'ignore_package_requirements', True)
                                   if flag_values else True)
    providers.LoadProvider(self.cloud, ignore_package_requirements)

    if self.db_disk_spec:
      disk_config = getattr(self.db_disk_spec, self.cloud, None)
      if disk_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.db_disk_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud))
      disk_spec_class = disk.GetDiskSpecClass(self.cloud)
      self.db_disk_spec = disk_spec_class(
          '{0}.db_disk_spec.{1}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **disk_config)

    db_vm_config = getattr(self.db_spec, self.cloud, None)
    if db_vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.db_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud))
    db_vm_spec_class = virtual_machine.GetVmSpecClass(self.cloud)
    self.db_spec = db_vm_spec_class(
        '{0}.db_spec.{1}'.format(component_full_name, self.cloud),
        flag_values=flag_values,
        **db_vm_config)

    # Set defaults that were not able to be set in
    # GetOptionDecoderConstructions()
    if not self.engine_version:
      db_class = relational_db.GetRelationalDbClass(self.cloud)
      self.engine_version = db_class.GetDefaultEngineVersion(self.engine)
    if not self.database_name:
      self.database_name = 'pkb-db-%s' % flag_values.run_uri
    if not self.database_username:
      self.database_username = 'pkb%s' % flag_values.run_uri
    if not self.database_password:
      self.database_password = relational_db.GenerateRandomDbPassword()

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_RelationalDbSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            'valid_values': providers.VALID_CLOUDS
        }),
        'engine': (option_decoders.EnumDecoder, {
            'valid_values': [
                relational_db.MYSQL,
                relational_db.POSTGRES,
                relational_db.AURORA_POSTGRES,
                relational_db.AURORA_MYSQL,
                relational_db.AURORA_MYSQL56,
            ]
        }),
        'zones': (option_decoders.ListDecoder, {
            'item_decoder': option_decoders.StringDecoder(),
            'default': None
        }),
        'engine_version': (option_decoders.StringDecoder, {
            'default': None
        }),
        'database_name': (option_decoders.StringDecoder, {
            'default': None
        }),
        'database_password': (option_decoders.StringDecoder, {
            'default': None
        }),
        'database_username': (option_decoders.StringDecoder, {
            'default': None
        }),
        'high_availability': (option_decoders.BooleanDecoder, {
            'default': False
        }),
        'backup_enabled': (option_decoders.BooleanDecoder, {
            'default': True
        }),
        'backup_start_time': (option_decoders.StringDecoder, {
            'default': '07:00'
        }),
        'db_spec': (option_decoders.PerCloudConfigDecoder, {}),
        'db_disk_spec': (option_decoders.PerCloudConfigDecoder, {}),
        'vm_groups': (_VmGroupsDecoder, {
            'default': {}
        })
    })
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
    # TODO(ferneyhough): Add flags for db_disk_spec.
    # Currently the only way to modify the disk spec of the
    # db is to change the benchmark spec in the benchmark source code
    # itself.
    super(_RelationalDbSpec, cls)._ApplyFlags(config_values, flag_values)

    # TODO(jerlawson): Rename flags 'managed_db_' -> 'db_'.
    has_db_machine_type = flag_values['managed_db_machine_type'].present
    has_db_cpus = flag_values['managed_db_cpus'].present
    has_db_memory = flag_values['managed_db_memory'].present
    has_custom_machine_type = has_db_cpus and has_db_memory
    has_client_machine_type = flag_values['client_vm_machine_type'].present
    has_client_vm_cpus = flag_values['client_vm_cpus'].present
    has_client_vm_memory = flag_values['client_vm_memory'].present
    has_client_custom_machine_type = has_client_vm_cpus and has_client_vm_memory

    if has_custom_machine_type and has_db_machine_type:
      raise errors.config.UnrecognizedOption(
          'db_cpus/db_memory can not be specified with '
          'db_machine_type.   Either specify a custom machine '
          'with cpus and memory or specify a predefined machine type.')

    if (not has_custom_machine_type and (has_db_cpus or has_db_memory)):
      raise errors.config.MissingOption(
          'To specify a custom database machine instance, both managed_db_cpus '
          'and managed_db_memory must be specified.')

    if has_client_custom_machine_type and has_client_machine_type:
      raise errors.config.UnrecognizedOption(
          'client_vm_cpus/client_vm_memory can not be specified with '
          'client_vm_machine_type.   Either specify a custom machine '
          'with cpus and memory or specify a predefined machine type.')

    if (not has_client_custom_machine_type and
        (has_client_vm_cpus or has_client_vm_memory)):
      raise errors.config.MissingOption(
          'To specify a custom client VM, both client_vm_cpus '
          'and client_vm_memory must be specified.')

    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['managed_db_engine'].present:
      config_values['engine'] = flag_values.managed_db_engine
    if flag_values['managed_db_engine_version'].present:
      config_values['engine_version'] = flag_values.managed_db_engine_version
    if flag_values['managed_db_database_name'].present:
      config_values['database_name'] = flag_values.managed_db_database_name
    if flag_values['managed_db_database_username'].present:
      config_values['database_username'] = (
          flag_values.managed_db_database_username)
    if flag_values['managed_db_database_password'].present:
      config_values['database_password'] = (
          flag_values.managed_db_database_password)
    if flag_values['managed_db_high_availability'].present:
      config_values['high_availability'] = (
          flag_values.managed_db_high_availability)
    if flag_values['managed_db_backup_enabled'].present:
      config_values['backup_enabled'] = (flag_values.managed_db_backup_enabled)
    if flag_values['managed_db_backup_start_time'].present:
      config_values['backup_start_time'] = (
          flag_values.managed_db_backup_start_time)

    cloud = config_values['cloud']
    if flag_values['managed_db_zone'].present:
      config_values['db_spec'][cloud]['zone'] = flag_values.managed_db_zone[0]
      config_values['zones'] = flag_values.managed_db_zone
      config_values['vm_groups']['servers']['vm_spec'][cloud]['zone'] = (
          flag_values.managed_db_zone[0])
    if flag_values['client_vm_zone'].present:
      config_values['vm_groups']['clients']['vm_spec'][cloud]['zone'] = (
          flag_values.client_vm_zone)
    if has_db_machine_type:
      config_values['db_spec'][cloud]['machine_type'] = (
          flag_values.managed_db_machine_type)
      config_values['vm_groups']['servers']['vm_spec'][cloud][
          'machine_type'] = (
              flag_values.managed_db_machine_type)
    if has_custom_machine_type:
      config_values['db_spec'][cloud]['machine_type'] = {
          'cpus': flag_values.managed_db_cpus,
          'memory': flag_values.managed_db_memory
      }
      # tox and pylint have contradictory closing brace rules, so avoid having
      # opening and closing brackets on different lines.
      config_values_vm_groups = config_values['vm_groups']
      config_values_vm_groups['servers']['vm_spec'][cloud]['machine_type'] = {
          'cpus': flag_values.managed_db_cpus,
          'memory': flag_values.managed_db_memory
      }
    if has_client_machine_type:
      config_values['vm_groups']['clients']['vm_spec'][cloud][
          'machine_type'] = (
              flag_values.client_vm_machine_type)
    if has_client_custom_machine_type:
      config_values_vm_groups = config_values['vm_groups']
      config_values_vm_groups['clients']['vm_spec'][cloud]['machine_type'] = {
          'cpus': flag_values.client_vm_cpus,
          'memory': flag_values.client_vm_memory
      }
    if flag_values['mysql_flags'].present:
      config_values['db_spec'][cloud]['mysql_flags'] = flag_values.mysql_flags
    if flag_values['managed_db_disk_size'].present:
      config_values['db_disk_spec'][cloud]['disk_size'] = (
          flag_values.managed_db_disk_size)
      config_values['vm_groups']['servers']['disk_spec'][cloud]['disk_size'] = (
          flag_values.managed_db_disk_size)
    if flag_values['managed_db_disk_type'].present:
      config_values['db_disk_spec'][cloud]['disk_type'] = (
          flag_values.managed_db_disk_type)
      config_values['vm_groups']['servers']['disk_spec'][cloud]['disk_type'] = (
          flag_values.managed_db_disk_type)
    if flag_values['client_vm_disk_size'].present:
      config_values['vm_groups']['clients']['disk_spec'][cloud]['disk_size'] = (
          flag_values.client_vm_disk_size)
    if flag_values['client_vm_disk_type'].present:
      config_values['vm_groups']['clients']['disk_spec'][cloud]['disk_type'] = (
          flag_values.client_vm_disk_type)
    logging.warning('Relational db config values: %s', config_values)


class _SparkServiceSpec(spec.BaseSpec):
  """Configurable options of an Apache Spark Service.

  We may add more options here, such as disk specs, as necessary.
  When there are flags for these attributes, the convention is that
  the flag is prefixed with spark.  For example, the static_cluster_id
  is overridden by the flag spark_static_cluster_id

  Attributes:
    service_type: string.  pkb_managed or managed_service
    static_cluster_id: if user has created a cluster, the id of the cluster.
    worker_group: Vm group spec for workers.
    master_group: Vm group spec for master
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_SparkServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_SparkServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'static_cluster_id': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True
        }),
        'service_type': (option_decoders.EnumDecoder, {
            'default':
                spark_service.PROVIDER_MANAGED,
            'valid_values': [
                spark_service.PROVIDER_MANAGED, spark_service.PKB_MANAGED
            ]
        }),
        'worker_group': (_VmGroupSpecDecoder, {}),
        'master_group': (_VmGroupSpecDecoder, {
            'default': None,
            'none_ok': True
        })
    })
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
    super(_SparkServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['spark_static_cluster_id'].present:
      config_values['static_cluster_id'] = (flag_values.spark_static_cluster_id)
    if flag_values['zones'].present:
      for group in ('master_group', 'worker_group'):
        if group in config_values:
          for cloud in config_values[group]['vm_spec']:
            config_values[group]['vm_spec'][cloud]['zone'] = (
                flag_values.zones[0])


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
    placement_group_name: string. Name of placement group
        that VM group belongs to.
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_VmGroupSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)
    ignore_package_requirements = (getattr(flag_values,
                                           'ignore_package_requirements', True)
                                   if flag_values else True)
    providers.LoadProvider(self.cloud, ignore_package_requirements)
    if self.disk_spec:
      disk_config = getattr(self.disk_spec, self.cloud, None)
      if disk_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.disk_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud))
      disk_spec_class = disk.GetDiskSpecClass(self.cloud)
      self.disk_spec = disk_spec_class(
          '{0}.disk_spec.{1}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **disk_config)
    vm_config = getattr(self.vm_spec, self.cloud, None)
    if vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud))
    vm_spec_class = virtual_machine.GetVmSpecClass(self.cloud)
    self.vm_spec = vm_spec_class(
        '{0}.vm_spec.{1}'.format(component_full_name, self.cloud),
        flag_values=flag_values,
        **vm_config)

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
            'valid_values': providers.VALID_CLOUDS
        }),
        'disk_count': (option_decoders.IntDecoder, {
            'default': _DEFAULT_DISK_COUNT,
            'min': 0,
            'none_ok': True
        }),
        'disk_spec': (option_decoders.PerCloudConfigDecoder, {
            'default': None,
            'none_ok': True
        }),
        'os_type': (option_decoders.EnumDecoder, {
            'valid_values': os_types.ALL
        }),
        'static_vms': (_StaticVmListDecoder, {}),
        'vm_count': (option_decoders.IntDecoder, {
            'default': _DEFAULT_VM_COUNT,
            'min': 0
        }),
        'vm_spec': (option_decoders.PerCloudConfigDecoder, {}),
        'placement_group_name': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True
        }),
    })
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
    for vm_group_name, vm_group_config in six.iteritems(vm_group_configs):
      result[vm_group_name] = _VmGroupSpec(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), vm_group_name),
          flag_values=flag_values,
          **vm_group_config)
    return result


class _VmGroupSpecDecoder(option_decoders.TypeVerifier):
  """Validates a single VmGroupSpec dictionary."""

  def __init__(self, **kwargs):
    super(_VmGroupSpecDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies vm_groups dictionary of a benchmark config object.

    Args:
      value: dict corresonding to a VM group config.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      dict a _VmGroupSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    vm_group_config = super(_VmGroupSpecDecoder, self).Decode(
        value, component_full_name, flag_values)
    return _VmGroupSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **vm_group_config)


class _PlacementGroupSpecsDecoder(option_decoders.TypeVerifier):
  """Validates the placement_group_specs dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_PlacementGroupSpecsDecoder, self).__init__(
        valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies placement_group_specs dictionary of a benchmark config object.

    Args:
      value: dict mapping Placement Group Spec name string to the
          corresponding placement group spec config dict.
      component_full_name: string. Fully qualified name of the configurable
          component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
          BaseSpec constructors.

    Returns:
      dict mapping Placement Group Spec name string
          to placement_group.BasePlacementGroupSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    placement_group_spec_configs = (super(_PlacementGroupSpecsDecoder, self)
                                    .Decode(
                                        value,
                                        component_full_name,
                                        flag_values))
    result = {}
    for placement_group_name, placement_group_spec_config in six.iteritems(
        placement_group_spec_configs):
      placement_group_spec_class = placement_group.GetPlacementGroupSpecClass(
          self.cloud)
      result[placement_group_name] = placement_group_spec_class(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name),
              placement_group_name),
          flag_values=flag_values,
          **placement_group_spec_config)
    return result


class _ContainerRegistryDecoder(option_decoders.TypeVerifier):
  """Validates the container_registry dictionary of a benchmark config."""

  def __init__(self, **kwargs):
    super(_ContainerRegistryDecoder, self).__init__(valid_types=(dict,),
                                                    **kwargs)

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
    vm_group_config = super(_ContainerRegistryDecoder, self).Decode(
        value, component_full_name, flag_values)
    return container_service.ContainerRegistrySpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **vm_group_config)


class _ContainerSpecsDecoder(option_decoders.TypeVerifier):
  """Validates the container_specs dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_ContainerSpecsDecoder, self).__init__(valid_types=(dict,), **kwargs)

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
    container_spec_configs = super(_ContainerSpecsDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = {}
    for spec_name, spec_config in six.iteritems(container_spec_configs):
      result[spec_name] = container_service.ContainerSpec(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), spec_name),
          flag_values=flag_values,
          **spec_config)
    return result


class _ContainerClusterSpec(spec.BaseSpec):
  """Spec containing info needed to create a container cluster."""

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_ContainerClusterSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)
    ignore_package_requirements = (getattr(flag_values,
                                           'ignore_package_requirements', True)
                                   if flag_values else True)
    providers.LoadProvider(self.cloud, ignore_package_requirements)
    vm_config = getattr(self.vm_spec, self.cloud, None)
    if vm_config is None:
      raise errors.Config.MissingOption(
          '{0}.cloud is "{1}", but {0}.vm_spec does not contain a '
          'configuration for "{1}".'.format(component_full_name, self.cloud))
    vm_spec_class = virtual_machine.GetVmSpecClass(self.cloud)
    self.vm_spec = vm_spec_class(
        '{0}.vm_spec.{1}'.format(component_full_name, self.cloud),
        flag_values=flag_values,
        **vm_config)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_ContainerClusterSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            'valid_values': providers.VALID_CLOUDS
        }),
        'type': (option_decoders.StringDecoder, {
            'default': 'Kubernetes',
        }),
        'vm_count': (option_decoders.IntDecoder, {
            'default': _DEFAULT_VM_COUNT,
            'min': 0
        }),
        'min_vm_count': (option_decoders.IntDecoder, {
            'default': None,
            'none_ok': True,
            'min': 0
        }),
        'max_vm_count': (option_decoders.IntDecoder, {
            'default': None,
            'none_ok': True,
            'min': 0
        }),
        'vm_spec': (option_decoders.PerCloudConfigDecoder, {})
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super(_ContainerClusterSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['container_cluster_cloud'].present:
      config_values['cloud'] = flag_values.container_cluster_cloud
    if flag_values['container_cluster_type'].present:
      config_values['type'] = flag_values.container_cluster_type
    if flag_values['container_cluster_num_vms'].present:
      config_values['vm_count'] = flag_values.container_cluster_num_vms

    # Need to apply the first zone in the zones flag, if specified,
    # to the spec. ContainerClusters do not currently support
    # running in multiple zones in a single PKB invocation.
    if flag_values['zones'].present:
      for cloud in config_values['vm_spec']:
        config_values['vm_spec'][cloud]['zone'] = (
            flag_values.zones[0])


class _ContainerClusterSpecDecoder(option_decoders.TypeVerifier):
  """Validates a ContainerClusterSpec dictionairy."""

  def __init__(self, **kwargs):
    super(_ContainerClusterSpecDecoder, self).__init__(
        valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies container_cluster dictionairy of a benchmark config object."""
    cluster_config = super(_ContainerClusterSpecDecoder, self).Decode(
        value, component_full_name, flag_values)

    return _ContainerClusterSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **cluster_config)


class _SparkServiceDecoder(option_decoders.TypeVerifier):
  """Validates the spark_service dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_SparkServiceDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifies spark_service dictionary of a benchmark config object.

    Args:
      value: dict Spark Service config dictionary
      component_full_name: string.  Fully qualified name of the configurable
      component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.
    Returns:
      _SparkServiceSpec Build from the config passed in in value.
    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    spark_service_config = super(_SparkServiceDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = _SparkServiceSpec(
        self._GetOptionFullName(component_full_name), flag_values,
        **spark_service_config)
    return result


class _RelationalDbDecoder(option_decoders.TypeVerifier):
  """Validate the relational_db dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_RelationalDbDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verify relational_db dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _RelationalDbService built from the config passed in in value.

    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    relational_db_config = super(_RelationalDbDecoder,
                                 self).Decode(value, component_full_name,
                                              flag_values)
    result = _RelationalDbSpec(
        self._GetOptionFullName(component_full_name), flag_values,
        **relational_db_config)
    return result


class _TpuGroupsDecoder(option_decoders.TypeVerifier):
  """Validate the tpu dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_TpuGroupsDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verify tpu dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
      component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _Tpu built from the config passed in in value.

    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    tpu_group_configs = super(_TpuGroupsDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = {}
    for tpu_group_name, tpu_group_config in six.iteritems(tpu_group_configs):
      result[tpu_group_name] = _TpuGroupSpec(
          self._GetOptionFullName(component_full_name),
          tpu_group_name,
          flag_values,
          **tpu_group_config)
    return result


class _CloudRedisSpec(spec.BaseSpec):
  """Specs needed to configure a cloud redis instance.
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_CloudRedisSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs)
    if not self.redis_name:
      self.redis_name = 'pkb-cloudredis-{0}'.format(flag_values.run_uri)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments to
      construct in order to decode the named option.
    """
    result = super(_CloudRedisSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.EnumDecoder, {
            'valid_values': providers.VALID_CLOUDS}),
        'redis_name': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': False}),
        'redis_version': (option_decoders.EnumDecoder, {
            'default': managed_memory_store.REDIS_3_2,
            'valid_values': managed_memory_store.REDIS_VERSIONS}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(_CloudRedisSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud


class _CloudRedisDecoder(option_decoders.TypeVerifier):
  """Validate the cloud_redis dictionary of a benchmark config object.
  """

  def __init__(self, **kwargs):
    super(_CloudRedisDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verify cloud_redis dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _CloudRedis built from the config passed in in value.

    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    cloud_redis_config = super(
        _CloudRedisDecoder, self).Decode(value, component_full_name,
                                         flag_values)
    result = _CloudRedisSpec(
        self._GetOptionFullName(component_full_name), flag_values,
        **cloud_redis_config)
    return result


class _AppGroupSpec(spec.BaseSpec):
  """Configurable options of a AppService group."""

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super(_AppGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'app_runtime': (option_decoders.StringDecoder, {
            'default': None, 'none_ok': True}),
        'app_type': (option_decoders.StringDecoder, {
            'default': None, 'none_ok': True}),
        'appservice_count': (
            option_decoders.IntDecoder, {'default': 1}),
        'appservice_spec': (_AppServiceDecoder, {})
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super(_AppGroupSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['appservice_count'].present:
      config_values['appservice_count'] = flag_values.appservice_count
    if flag_values['app_runtime'].present:
      config_values['app_runtime'] = flag_values.app_runtime
    if flag_values['app_type'].present:
      config_values['app_type'] = flag_values.app_type


class _AppGroupsDecoder(option_decoders.TypeVerifier):
  """Verify app_groups dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_AppGroupsDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verifys app_groups dictionary of a benchmark config object.

    Args:
      value: dict. Config dictionary.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
     dict mapping app group name string to _AppGroupSpec.

    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    app_group_configs = super(_AppGroupsDecoder, self).Decode(
        value, component_full_name, flag_values)
    result = {}
    for app_group_name, app_group_config in six.iteritems(app_group_configs):
      result[app_group_name] = _AppGroupSpec(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), app_group_name),
          flag_values=flag_values,
          **app_group_config)
    return result


class _AppServiceDecoder(option_decoders.TypeVerifier):
  """Verify app_service dict of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_AppServiceDecoder, self).__init__(valid_types=(dict,), **kwargs)

  def Decode(self, value, component_full_name, flag_values):
    """Verify app_service dict of a benchmark config object.

    Args:
      value: dict. Config dictionary.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      AppService object built from config.

    Raises:
      errors.Config.InvalidateValue upon invalid input value.
    """
    config = super(_AppServiceDecoder, self).Decode(
        value, component_full_name, flag_values)
    spec_cls = app_service.GetAppServiceSpecClass(
        flag_values.appservice or config.get('appservice'))
    return spec_cls(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values, **config)


class BenchmarkConfigSpec(spec.BaseSpec):
  """Configurable options of a benchmark run.

  Attributes:
    description: None or string. Description of the benchmark to run.
    name: Optional. The name of the benchmark
    flags: dict. Values to use for each flag while executing the
        benchmark.
    vm_groups: dict mapping VM group name string to _VmGroupSpec. Configurable
        options for each VM group used by the benchmark.
  """

  def __init__(self, component_full_name, expected_os_types=None, **kwargs):
    """Initializes a BenchmarkConfigSpec.

    Args:
      component_full_name: string. Fully qualified name of the benchmark config
          dict within the config file.
      expected_os_types: Optional series of strings from os_types.ALL.
      **kwargs: Keyword arguments for the BaseSpec constructor.

    Raises:
      errors.Config.InvalidValue: If expected_os_types is provided and any of
          the VM groups are configured with an OS type that is not included.
    """
    super(BenchmarkConfigSpec, self).__init__(component_full_name, **kwargs)
    if expected_os_types is not None:
      mismatched_os_types = []
      for group_name, group_spec in sorted(six.iteritems(self.vm_groups)):
        if group_spec.os_type not in expected_os_types:
          mismatched_os_types.append('{0}.vm_groups[{1}].os_type: {2}'.format(
              component_full_name, repr(group_name), repr(group_spec.os_type)))
      if mismatched_os_types:
        raise errors.Config.InvalidValue(
            'VM groups in {0} may only have the following OS types: {1}. The '
            'following VM group options are invalid:{2}{3}'.format(
                component_full_name, ', '.join(
                    repr(os_type) for os_type in expected_os_types), os.linesep,
                os.linesep.join(mismatched_os_types)))

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
        'description': (option_decoders.StringDecoder, {
            'default': None
        }),
        'name': (option_decoders.StringDecoder, {
            'default': None
        }),
        'flags': (option_decoders.TypeVerifier, {
            'default': None,
            'none_ok': True,
            'valid_types': (dict,)
        }),
        'vm_groups': (_VmGroupsDecoder, {
            'default': {}
        }),
        'placement_group_specs': (_PlacementGroupSpecsDecoder, {
            'default': {}
        }),
        'spark_service': (_SparkServiceDecoder, {
            'default': None
        }),
        'container_cluster': (_ContainerClusterSpecDecoder, {
            'default': None
        }),
        'container_registry': (_ContainerRegistryDecoder, {
            'default': None
        }),
        'container_specs': (_ContainerSpecsDecoder, {
            'default': None
        }),
        'dpb_service': (_DpbServiceDecoder, {
            'default': None
        }),
        'relational_db': (_RelationalDbDecoder, {
            'default': None
        }),
        'tpu_groups': (_TpuGroupsDecoder, {
            'default': {}
        }),
        'edw_service': (_EdwServiceDecoder, {
            'default': None
        }),
        'cloud_redis': (_CloudRedisDecoder, {
            'default': None
        }),
        'app_groups': (_AppGroupsDecoder, {
            'default': {}
        }),
    })
    return result

  def _DecodeAndInit(self, component_full_name, config, decoders, flag_values):
    """Initializes spec attributes from provided config option values.

    Args:
      component_full_name: string. Fully qualified name of the configurable
          component containing the config options.
      config: dict mapping option name string to option value.
      decoders: OrderedDict mapping option name string to ConfigOptionDecoder.
      flag_values: flags.FlagValues. Runtime flags that may override provided
          config option values. These flags have already been applied to the
          current config, but they may be passed to the decoders for propagation
          to deeper spec constructors.
    """
    # Decode benchmark-specific flags first and use them while decoding the
    # rest of the BenchmarkConfigSpec's options.
    decoders = decoders.copy()
    self.flags = config.get('flags')
    with self.RedirectFlags(flag_values):
      super(BenchmarkConfigSpec, self)._DecodeAndInit(
          component_full_name, config, decoders, flag_values)

  @contextlib.contextmanager
  def RedirectFlags(self, flag_values):
    """Redirects flag reads and writes to the benchmark-specific flags object.

    Args:
      flag_values: flags.FlagValues object. Within the enclosed code block,
          reads and writes to this object are redirected to self.flags.
    Yields:
      context manager that redirects flag reads and writes.
    """
    with flag_util.OverrideFlags(flag_values, self.flags):
      yield
