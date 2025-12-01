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
"""Module containing the spec for relational database services."""

import copy

from absl import logging
from perfkitbenchmarker import db_util
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import freeze_restore_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.configs import vm_group_decoders


_NONE_OK = {'default': None, 'none_ok': True}


def GetRelationalDbSpecClass(engine):
  """Get the RelationalDbSpec class corresponding to 'engine'."""
  if engine in [
      sql_engine_utils.SPANNER_GOOGLESQL,
      sql_engine_utils.SPANNER_POSTGRES,
  ]:
    return spec.GetSpecClass(RelationalDbSpec, SERVICE_TYPE='spanner')
  if engine == sql_engine_utils.AURORA_DSQL_POSTGRES:
    return spec.GetSpecClass(RelationalDbSpec, SERVICE_TYPE='aurora-dsql')
  return RelationalDbSpec


class RelationalDbSpec(freeze_restore_spec.FreezeRestoreSpec):
  """Configurable options of a database service.

  Attributes:
    cloud: The cloud of the database service.
    engine: The engine of the database service, i.e. MySQL, Postgres.
    engine_version: The engine version.
    database_name: Name of the database.
    database_username: Admin username for the database.
    database_password: Admin password for the database.
    is_managed_db: Specifies whether or not this is a managed DB service as
      opposed to unmanaged (installed on infrastructure).
    db_tier: Specifies what tier the database is in.
    db_disk_spec: disk.BaseDiskSpec: Configurable disk options.
    db_spec: virtual_machine.BaseVmSpec: Configurable VM options.
  """

  SPEC_TYPE = 'RelationalDbSpec'
  SPEC_ATTRS = ['SERVICE_TYPE']

  cloud: str
  engine: str
  engine_version: str
  database_name: str
  database_username: str
  database_password: str
  is_managed_db: bool
  db_tier: str
  db_disk_spec: disk.BaseDiskSpec
  db_spec: virtual_machine.BaseVmSpec
  load_machine_type: str

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )
    # TODO(user): This is a lot of boilerplate, and is repeated
    # below in VmGroupSpec. See if some can be consolidated. Maybe we can
    # specify a VmGroupSpec instead of both vm_spec and disk_spec.
    ignore_package_requirements = (
        getattr(flag_values, 'ignore_package_requirements', True)
        if flag_values
        else True
    )
    providers.LoadProvider(self.cloud, ignore_package_requirements)

    if self.db_disk_spec:
      disk_config = getattr(self.db_disk_spec, self.cloud, None)
      if disk_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.db_disk_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud)
        )
      disk_spec_class = disk.GetDiskSpecClass(
          self.cloud, disk_config.get('disk_type', None)
      )
      self.db_disk_spec = disk_spec_class(
          '{}.db_disk_spec.{}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **disk_config
      )

    if self.db_spec:
      db_vm_config = getattr(self.db_spec, self.cloud, None)
      if db_vm_config is None:
        raise errors.Config.MissingOption(
            '{0}.cloud is "{1}", but {0}.db_spec does not contain a '
            'configuration for "{1}".'.format(component_full_name, self.cloud)
        )
      db_vm_spec_class = virtual_machine.GetVmSpecClass(self.cloud)
      self.db_spec = db_vm_spec_class(
          '{}.db_spec.{}'.format(component_full_name, self.cloud),
          flag_values=flag_values,
          **db_vm_config
      )

    # Set defaults that were not able to be set in
    # GetOptionDecoderConstructions()
    if not self.database_name:
      self.database_name = 'pkb-db-%s' % flag_values.run_uri
    if not self.database_username:
      self.database_username = 'pkb%s' % flag_values.run_uri
    if not self.database_password:
      self.database_password = db_util.GenerateRandomDbPassword()

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
        'engine': (
            option_decoders.EnumDecoder,
            {
                'valid_values': sql_engine_utils.ALL_ENGINES,
            },
        ),
        'zones': (
            option_decoders.ListDecoder,
            {'item_decoder': option_decoders.StringDecoder(), 'default': None},
        ),
        'engine_version': (option_decoders.StringDecoder, {'default': None}),
        'database_name': (option_decoders.StringDecoder, {'default': None}),
        'database_password': (option_decoders.StringDecoder, {'default': None}),
        'database_username': (option_decoders.StringDecoder, {'default': None}),
        'high_availability': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
        'high_availability_type': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'backup_enabled': (option_decoders.BooleanDecoder, {'default': True}),
        'is_managed_db': (option_decoders.BooleanDecoder, {'default': True}),
        'db_tier': (option_decoders.StringDecoder, {'default': None}),
        'db_spec': (spec.PerCloudConfigDecoder, {}),
        'db_disk_spec': (spec.PerCloudConfigDecoder, {}),
        'vm_groups': (vm_group_decoders.VmGroupsDecoder, {'default': {}}),
        'db_flags': (
            option_decoders.ListDecoder,
            {'item_decoder': option_decoders.StringDecoder(), 'default': None},
        ),
        'load_machine_type': (option_decoders.StringDecoder, {'default': None}),
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
    # TODO(user): Add flags for db_disk_spec.
    # Currently the only way to modify the disk spec of the
    # db is to change the benchmark spec in the benchmark source code
    # itself.
    super()._ApplyFlags(config_values, flag_values)

    # TODO(user): Rename flags 'managed_db_' -> 'db_'.
    has_db_machine_type = flag_values['db_machine_type'].present
    has_db_cpus = flag_values['db_cpus'].present
    has_db_memory = flag_values['db_memory'].present
    has_custom_machine_type = has_db_cpus and has_db_memory
    has_client_machine_type = flag_values['client_vm_machine_type'].present
    has_client_vm_cpus = flag_values['client_vm_cpus'].present
    has_client_vm_memory = flag_values['client_vm_memory'].present
    has_client_custom_machine_type = has_client_vm_cpus and has_client_vm_memory

    if has_custom_machine_type and has_db_machine_type:
      raise errors.Config.UnrecognizedOption(
          'db_cpus/db_memory can not be specified with '
          'db_machine_type.   Either specify a custom machine '
          'with cpus and memory or specify a predefined machine type.'
      )

    if not has_custom_machine_type and (has_db_cpus or has_db_memory):
      raise errors.Config.MissingOption(
          'To specify a custom database machine instance, both db_cpus '
          'and db_memory must be specified.'
      )

    if has_client_custom_machine_type and has_client_machine_type:
      raise errors.Config.UnrecognizedOption(
          'client_vm_cpus/client_vm_memory can not be specified with '
          'client_vm_machine_type.   Either specify a custom machine '
          'with cpus and memory or specify a predefined machine type.'
      )

    if not has_client_custom_machine_type and (
        has_client_vm_cpus or has_client_vm_memory
    ):
      raise errors.Config.MissingOption(
          'To specify a custom client VM, both client_vm_cpus '
          'and client_vm_memory must be specified.'
      )

    if flag_values['use_managed_db'].present:
      config_values['is_managed_db'] = flag_values.use_managed_db

    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    if flag_values['db_engine'].present:
      config_values['engine'] = flag_values.db_engine
    if flag_values['db_engine_version'].present:
      config_values['engine_version'] = flag_values.db_engine_version
    if flag_values['database_name'].present:
      config_values['database_name'] = flag_values.database_name
    if flag_values['database_username'].present:
      config_values['database_username'] = flag_values.database_username
    if flag_values['database_password'].present:
      config_values['database_password'] = flag_values.database_password
    if flag_values['db_high_availability'].present:
      config_values['high_availability'] = flag_values.db_high_availability
    if flag_values['db_high_availability_type'].present:
      config_values['high_availability_type'] = (
          flag_values.db_high_availability_type
      )
    if flag_values['db_backup_enabled'].present:
      config_values['backup_enabled'] = flag_values.db_backup_enabled
    if flag_values['db_flags'].present:
      config_values['db_flags'] = flag_values.db_flags
    cloud = config_values['cloud']
    has_unmanaged_dbs = 'vm_groups' in config_values and (
        'servers' in config_values['vm_groups']
        or 'servers_replicas' in config_values['vm_groups']
    )

    # Set zone for db server
    # flag value order: db_zone (if specified) -> zone (if specified)
    if flag_values['db_zone'].present:
      config_values['db_spec'][cloud]['zone'] = flag_values.db_zone[0]
      config_values['zones'] = flag_values.db_zone
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['vm_spec'][cloud]['zone'] = (
            flag_values.db_zone[0]
        )
    elif (
        flag_values['zone'].present
        and 'db_spec' in config_values
        and 'zones' in config_values
    ):
      config_values['db_spec'][cloud]['zone'] = flag_values.zone[0]
      config_values['zones'] = flag_values.zone
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['vm_spec'][cloud]['zone'] = (
            flag_values.zone[0]
        )

    if flag_values['client_vm_count'].present:
      config_values['vm_groups']['clients'][
          'vm_count'
      ] = flag_values.client_vm_count

    # Set zone for client vm
    # flag value order: client_vm_zone (if specified) -> zone (if specified)
    if flag_values['client_vm_zone'].present:
      config_values['vm_groups']['clients']['vm_spec'][cloud][
          'zone'
      ] = flag_values.client_vm_zone
    elif (
        flag_values['zone'].present
        and 'vm_groups' in config_values
        and 'clients' in config_values['vm_groups']
    ):
      config_values['vm_groups']['clients']['vm_spec'][cloud]['zone'] = (
          flag_values.zone[0]
      )

    # Set zone for controller vm
    if (
        flag_values['zone'].present
        and 'vm_groups' in config_values
        and 'controller' in config_values['vm_groups']
    ):
      config_values['vm_groups']['controller']['vm_spec'][cloud]['zone'] = (
          flag_values.zone[0]
      )

    if has_db_machine_type:
      config_values['db_spec'][cloud][
          'machine_type'
      ] = flag_values.db_machine_type
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['vm_spec'][cloud][
            'machine_type'
        ] = flag_values.db_machine_type
    if has_custom_machine_type:
      config_values['db_spec'][cloud]['machine_type'] = {
          'cpus': flag_values.db_cpus,
          'memory': flag_values.db_memory,
      }
      # tox and pylint have contradictory closing brace rules, so avoid having
      # opening and closing brackets on different lines.
      config_values_vm_groups = config_values['vm_groups']
      if has_unmanaged_dbs:
        config_values_vm_groups['servers']['vm_spec'][cloud]['machine_type'] = {
            'cpus': flag_values.db_cpus,
            'memory': flag_values.db_memory,
        }
    if flag_values['managed_db_azure_compute_units'].present:
      config_values['db_spec'][cloud]['machine_type'][
          'compute_units'
      ] = flag_values.managed_db_azure_compute_units
    if flag_values['managed_db_tier'].present:
      config_values['db_tier'] = flag_values.managed_db_tier
    if has_client_machine_type:
      config_values['vm_groups']['clients']['vm_spec'][cloud][
          'machine_type'
      ] = flag_values.client_vm_machine_type
    if has_client_custom_machine_type:
      config_values_vm_groups = config_values['vm_groups']
      config_values_vm_groups['clients']['vm_spec'][cloud]['machine_type'] = {
          'cpus': flag_values.client_vm_cpus,
          'memory': flag_values.client_vm_memory,
      }
    if flag_values['db_num_striped_disks'].present and has_unmanaged_dbs:
      config_values['vm_groups']['servers']['disk_spec'][cloud][
          'num_striped_disks'
      ] = flag_values.db_num_striped_disks
    if flag_values['db_disk_size'].present:
      config_values['db_disk_spec'][cloud][
          'disk_size'
      ] = flag_values.db_disk_size
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['disk_spec'][cloud][
            'disk_size'
        ] = flag_values.db_disk_size
    if flag_values['db_disk_type'].present:
      config_values['db_disk_spec'][cloud][
          'disk_type'
      ] = flag_values.db_disk_type
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['disk_spec'][cloud][
            'disk_type'
        ] = flag_values.db_disk_type
    if flag_values['db_disk_iops'].present:
      # This value will be used in aws_relation_db.py druing db creation
      config_values['db_disk_spec'][cloud][
          'provisioned_iops'
      ] = flag_values.db_disk_iops
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['disk_spec'][cloud][
            'provisioned_iops'
        ] = flag_values.db_disk_iops
    if flag_values['db_disk_throughput'].present:
      config_values['db_disk_spec'][cloud][
          'provisioned_throughput'
      ] = flag_values.db_disk_throughput
      if has_unmanaged_dbs:
        config_values['vm_groups']['servers']['disk_spec'][cloud][
            'provisioned_throughput'
        ] = flag_values.db_disk_throughput

    if flag_values['client_vm_os_type'].present:
      config_values['vm_groups']['clients'][
          'os_type'
      ] = flag_values.client_vm_os_type
    if flag_values['server_vm_os_type'].present:
      config_values['vm_groups']['servers'][
          'os_type'
      ] = flag_values.server_vm_os_type

    if flag_values['client_gcp_min_cpu_platform'].present:
      config_values['vm_groups']['clients']['vm_spec'][cloud][
          'min_cpu_platform'
      ] = flag_values.client_gcp_min_cpu_platform
    if flag_values['server_gcp_min_cpu_platform'].present:
      config_values['vm_groups']['servers']['vm_spec'][cloud][
          'min_cpu_platform'
      ] = flag_values.server_gcp_min_cpu_platform
    if flag_values['server_gce_num_local_ssds'].present and has_unmanaged_dbs:
      config_values['vm_groups']['servers']['vm_spec'][cloud][
          'num_local_ssds'
      ] = flag_values.server_gce_num_local_ssds
    if flag_values['server_gce_ssd_interface'].present and has_unmanaged_dbs:
      config_values['vm_groups']['servers']['vm_spec'][cloud][
          'ssd_interface'
      ] = flag_values.server_gce_ssd_interface
      config_values['vm_groups']['servers']['disk_spec'][cloud][
          'interface'
      ] = flag_values.server_gce_ssd_interface
    if flag_values['client_vm_disk_size'].present:
      config_values['vm_groups']['clients']['disk_spec'][cloud][
          'disk_size'
      ] = flag_values.client_vm_disk_size
    if flag_values['client_vm_disk_type'].present:
      config_values['vm_groups']['clients']['disk_spec'][cloud][
          'disk_type'
      ] = flag_values.client_vm_disk_type
    if flag_values['client_vm_disk_iops'].present:
      config_values['vm_groups']['clients']['disk_spec'][cloud][
          'provisioned_iops'
      ] = flag_values.client_vm_disk_iops

    # Copy the servers vm group to the server replicas vm group if unmanaged
    # dbs are present.
    if (
        has_unmanaged_dbs
        and 'servers_replicas' in config_values['vm_groups']
        and 'vm_count' in config_values['vm_groups']['servers_replicas']
        and config_values['vm_groups']['servers_replicas']['vm_count'] > 0
    ):
      config_values['vm_groups']['servers_replicas'] = copy.deepcopy(
          config_values['vm_groups']['servers']
      )
      # Set the zone for replica server (if specified)
      if flag_values['db_replica_zones'].present:
        if (
            config_values['vm_groups']['servers']['vm_spec'][cloud]['zone']
            == flag_values.db_replica_zones[0]
        ):
          config_values['vm_groups']['servers_replicas']['vm_spec'][cloud][
              'zone'
          ] = flag_values.db_replica_zones[1]
        else:
          config_values['vm_groups']['servers_replicas']['vm_spec'][cloud][
              'zone'
          ] = flag_values.db_replica_zones[0]

      # Clear all the zones if the zone flag is present. This will prevent zone
      # values to be overwritten by the benchmark
      # spec.ConstructVirtualMachineGroup() method.
      if flag_values['zone'].present:
        flag_values.zone.clear()

    logging.warning('Relational db config values: %s', config_values)
