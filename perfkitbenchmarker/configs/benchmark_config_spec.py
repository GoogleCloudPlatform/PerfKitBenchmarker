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

import contextlib
import os

from perfkitbenchmarker import app_service
from perfkitbenchmarker import data_discovery_service
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import key
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker.configs import container_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.resources import example_resource_spec
from perfkitbenchmarker.resources import jobs_setter
# Included to import & load Kubernetes' __init__.py somewhere.
from perfkitbenchmarker.resources import kubernetes  # pylint:disable=unused-import
from perfkitbenchmarker.resources import managed_ai_model_spec
import six

_NONE_OK = {'default': None, 'none_ok': True}


class _DpbApplicationListDecoder(option_decoders.ListDecoder):
  """Decodes the list of applications to be enabled on the dpb service."""

  def __init__(self, **kwargs):
    super(_DpbApplicationListDecoder, self).__init__(
        default=None,
        item_decoder=option_decoders.EnumDecoder(
            [dpb_constants.FLINK, dpb_constants.HIVE]
        ),
        **kwargs,
    )


class _DpbServiceDecoder(option_decoders.TypeVerifier):
  """Validates the dpb service dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verifies dpb service dictionary of a benchmark config object.

    Args:
      value: dict Dpb Service config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _DpbServiceSpec Build from the config passed in in value.
    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    dpb_service_config = super().Decode(value, component_full_name, flag_values)

    if (
        dpb_service_config['service_type'] == dpb_constants.EMR
        and component_full_name == 'dpb_wordcount_benchmark'
    ):
      if flag_values.dpb_wordcount_fs != dpb_constants.S3_FS:
        raise errors.Config.InvalidValue('EMR service requires S3.')
    result = _DpbServiceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **dpb_service_config,
    )
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
    applications: An enumerated list of applications that need to be enabled on
      the dpb service
    version: string. The version of software to install inside the service.
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_DpbServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )

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
        'static_dpb_service_instance': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'service_type': (
            option_decoders.EnumDecoder,
            {
                'default': dpb_constants.DATAPROC,
                'valid_values': [
                    dpb_constants.DATAPROC,
                    dpb_constants.DATAPROC_FLINK,
                    dpb_constants.DATAPROC_GKE,
                    dpb_constants.DATAPROC_SERVERLESS,
                    dpb_constants.DATAFLOW,
                    dpb_constants.EMR,
                    dpb_constants.EMR_SERVERLESS,
                    dpb_constants.GLUE,
                    dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER,
                    dpb_constants.UNMANAGED_SPARK_CLUSTER,
                    dpb_constants.KUBERNETES_SPARK_CLUSTER,
                    dpb_constants.KUBERNETES_FLINK_CLUSTER,
                ],
            },
        ),
        'worker_group': (vm_group_decoders.VmGroupSpecDecoder, {}),
        'worker_count': (
            option_decoders.IntDecoder,
            {'default': dpb_constants.DEFAULT_WORKER_COUNT, 'min': 0},
        ),
        'applications': (_DpbApplicationListDecoder, {}),
        'version': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'gke_cluster_name': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'gke_cluster_nodepools': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'gke_cluster_location': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataflow_max_worker_count': (
            option_decoders.IntDecoder,
            {
                'default': None,
                'none_ok': True,
            },
        ),
        'dataproc_serverless_core_count': (
            option_decoders.IntDecoder,
            {
                'default': None,
                'none_ok': True,
            },
        ),
        'dataproc_serverless_initial_executors': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_min_executors': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_max_executors': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_memory': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_driver_memory': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_executor_memory': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_off_heap_memory': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'dataproc_serverless_memory_overhead': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'emr_serverless_executor_count': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'emr_serverless_core_count': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
        ),
        'emr_serverless_memory': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True},
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
    super(_DpbServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['static_dpb_service_instance'].present:
      config_values['static_dpb_service_instance'] = (
          flag_values.static_dpb_service_instance
      )
    # TODO(saksena): Update the documentation for zones assignment
    if flag_values['zone'].present:
      group = 'worker_group'
      if group in config_values:
        for cloud in config_values[group]['vm_spec']:
          config_values[group]['vm_spec'][cloud]['zone'] = flag_values.zone[0]


class _ExampleResourceDecoder(option_decoders.TypeVerifier):
  """Validate the example_resource dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify example_resource dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      ExampleResourceSpec built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    example_config = super().Decode(value, component_full_name, flag_values)
    if 'example_type' in example_config:
      spec_class = example_resource_spec.GetExampleResourceSpecClass(
          example_config['example_type']
      )
    else:
      raise errors.Config.InvalidValue(
          'Required attribute `example_type` missing from example_resource '
          'config.'
      )
    return spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **example_config,
    )


class _BaseJobDecoder(option_decoders.TypeVerifier):
  """Validates the base_job dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verifies base_job dictionary of a benchmark config object.

    Args :
      value: dict base_job config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
        BaseJobSpec built from the config passed in value.
    Raises:
        errors.Config.InvalidValue upon invalid input value.
    """
    base_job_config = super().Decode(value, component_full_name, flag_values)
    if 'job_type' in base_job_config:
      spec_class = jobs_setter.GetJobSpecClass(base_job_config['job_type'])
    else:
      raise errors.Config.InvalidValue(
          'job_type is required for base_job config'
      )
    return spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **base_job_config,
    )


class _ManagedAiModelSpecDecoder(option_decoders.TypeVerifier):
  """Validate the managed_model dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify managed_model dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      ManagedAiModelSpec built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    config = super().Decode(value, component_full_name, flag_values)
    if 'cloud' in config and 'model_name' in config:
      providers.LoadProvider(config['cloud'])
      spec_class = managed_ai_model_spec.GetManagedAiModelSpecClass(
          config['cloud'], config['model_name']
      )
    else:
      raise errors.Config.InvalidValue(
          'Required attribute missing from model spec '
          f'config {config}.'
      )
    return spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **config,
    )


class _TpuGroupSpec(spec.BaseSpec):
  """Configurable options of a TPU."""

  tpu_name: str

  def __init__(
      self, component_full_name, group_name, flag_values=None, **kwargs
  ):
    super(_TpuGroupSpec, self).__init__(
        '{0}.{1}'.format(component_full_name, group_name),
        flag_values=flag_values,
        **kwargs,
    )
    if not self.tpu_name:
      self.tpu_name = 'pkb-tpu-{group_name}-{run_uri}'.format(
          group_name=group_name, run_uri=flag_values.run_uri
      )

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
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'tpu_cidr_range': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'tpu_accelerator_type': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'tpu_description': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'tpu_network': (option_decoders.StringDecoder, {'default': None}),
        'tpu_tf_version': (
            option_decoders.StringDecoder,
            {'default': None},
        ),
        'tpu_zone': (option_decoders.StringDecoder, {'default': None}),
        'tpu_name': (option_decoders.StringDecoder, {'default': None}),
        'tpu_preemptible': (
            option_decoders.BooleanDecoder,
            {'default': False},
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

  def Decode(self, value, component_full_name, flag_values):
    """Verifies edw service dictionary of a benchmark config object.

    Args:
      value: dict edw service config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _EdwServiceSpec Built from the config passed in in value.
    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    edw_service_config = super(_EdwServiceDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = _EdwServiceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **edw_service_config,
    )
    return result


class _EdwServiceSpec(spec.BaseSpec):
  """Configurable options of an EDW service.

    When there are flags for these attributes, the convention is that
    the flag is prefixed with edw_service.

  Attributes: cluster_name  : string. If set, the name of the cluster
    type: string. The type of EDW service (redshift)
    node_type: string, type of node comprising the cluster
    node_count: integer, number of nodes in the cluster
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_EdwServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )

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
        'type': (
            option_decoders.StringDecoder,
            {'default': 'redshift', 'none_ok': False},
        ),
        'cluster_identifier': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'endpoint': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'concurrency': (
            option_decoders.IntDecoder,
            {'default': 5, 'none_ok': True},
        ),
        'db': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'user': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'password': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'node_type': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'node_count': (
            option_decoders.IntDecoder,
            {
                'default': edw_service.DEFAULT_NUMBER_OF_NODES,
                'min': edw_service.DEFAULT_NUMBER_OF_NODES,
            },
        ),
        'snapshot': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'cluster_subnet_group': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'cluster_parameter_group': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'resource_group': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'server_name': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'iam_role': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
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
    super(_EdwServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    # TODO(saksena): Add cluster_subnet_group and cluster_parameter_group flags
    # Restoring from a snapshot, so defer to the user supplied cluster details
    if flag_values['edw_service_cluster_snapshot'].present:
      config_values['snapshot'] = flag_values.edw_service_cluster_snapshot
    if flag_values['edw_service_cluster_identifier'].present:
      config_values['cluster_identifier'] = (
          flag_values.edw_service_cluster_identifier
      )
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


class _EdwComputeResourceDecoder(option_decoders.TypeVerifier):
  """Validates the edw compute resource dictionary of a benchmark config object."""

  def __init__(self, **kwargs):
    super(_EdwComputeResourceDecoder, self).__init__(
        valid_types=(dict,), **kwargs
    )

  def Decode(self, value, component_full_name, flag_values):
    """Verifies edw compute resource dictionary of a benchmark config object.

    Args:
      value: dict edw_compute_resource config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _EdwComputeResourceSpec Built from the config passed in value.
    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    edw_compute_resource_config = super(
        _EdwComputeResourceDecoder, self
    ).Decode(value, component_full_name, flag_values)
    result = _EdwComputeResourceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **edw_compute_resource_config,
    )
    return result


class _EdwComputeResourceSpec(spec.BaseSpec):
  """Configurable options of an EDW compute resource.

  Attributes:
    type: string. The type of the EDW compute resource (bigquery_slots, etc.)
  """

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_EdwComputeResourceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super(
        _EdwComputeResourceSpec, cls
    )._GetOptionDecoderConstructions()
    result.update({
        'type': (
            option_decoders.StringDecoder,
            {'default': 'bigquery_slots', 'none_ok': False},
        ),
        'cloud': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super(_EdwComputeResourceSpec, cls)._ApplyFlags(config_values, flag_values)
    if 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud


class _PlacementGroupSpecsDecoder(option_decoders.TypeVerifier):
  """Validates the placement_group_specs dictionary of a benchmark config object."""

  cloud: str

  def __init__(self, **kwargs):
    super(_PlacementGroupSpecsDecoder, self).__init__(
        valid_types=(dict,), **kwargs
    )

  def Decode(self, value, component_full_name, flag_values):
    """Verifies placement_group_specs dictionary of a benchmark config object.

    Args:
      value: dict mapping Placement Group Spec name string to the corresponding
        placement group spec config dict.
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
    placement_group_spec_configs = super(
        _PlacementGroupSpecsDecoder, self
    ).Decode(value, component_full_name, flag_values)
    result = {}
    for placement_group_name, placement_group_spec_config in six.iteritems(
        placement_group_spec_configs
    ):
      placement_group_spec_class = placement_group.GetPlacementGroupSpecClass(
          self.cloud
      )
      result[placement_group_name] = placement_group_spec_class(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), placement_group_name
          ),
          flag_values=flag_values,
          **placement_group_spec_config,
      )
    return result


class _RelationalDbDecoder(option_decoders.TypeVerifier):
  """Validate the relational_db dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify relational_db dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      RelationalDbSpec built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    relational_db_config = super().Decode(
        value, component_full_name, flag_values
    )
    if 'engine' in relational_db_config:
      if flag_values['db_engine'].present:
        db_spec_class = relational_db_spec.GetRelationalDbSpecClass(
            flag_values['db_engine'].value
        )
      else:
        db_spec_class = relational_db_spec.GetRelationalDbSpecClass(
            relational_db_config['engine']
        )
    else:
      raise errors.Config.InvalidValue(
          'Required attribute `engine` missing from relational_db config.'
      )
    return db_spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **relational_db_config,
    )


class _NonRelationalDbDecoder(option_decoders.TypeVerifier):
  """Validate the non_relational_db dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify non_relational_db dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _NonRelationalDbService built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    non_relational_db_config = super().Decode(
        value, component_full_name, flag_values
    )
    if 'service_type' in non_relational_db_config:
      db_spec_class = non_relational_db.GetNonRelationalDbSpecClass(
          non_relational_db_config['service_type']
      )
    else:
      raise errors.Config.InvalidValue(
          'Required attribute `service_type` missing from non_relational_db '
          'config.'
      )
    return db_spec_class(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **non_relational_db_config,
    )


class _TpuGroupsDecoder(option_decoders.TypeVerifier):
  """Validate the tpu dictionary of a benchmark config object."""

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
      errors.Config.InvalidValue upon invalid input value.
    """
    tpu_group_configs = super(_TpuGroupsDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = {}
    for tpu_group_name, tpu_group_config in six.iteritems(tpu_group_configs):
      result[tpu_group_name] = _TpuGroupSpec(
          self._GetOptionFullName(component_full_name),
          tpu_group_name,
          flag_values,
          **tpu_group_config,
      )
    return result


class _CloudRedisSpec(spec.BaseSpec):
  """Specs needed to configure a cloud redis instance."""

  redis_name: str

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_CloudRedisSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )
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
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'redis_name': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': False},
        ),
        'redis_version': (
            option_decoders.EnumDecoder,
            {
                'default': managed_memory_store.REDIS_3_2,
                'valid_values': managed_memory_store.REDIS_VERSIONS,
            },
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super(_CloudRedisSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud


class _CloudRedisDecoder(option_decoders.TypeVerifier):
  """Validate the cloud_redis dictionary of a benchmark config object."""

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
      errors.Config.InvalidValue upon invalid input value.
    """
    cloud_redis_config = super(_CloudRedisDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = _CloudRedisSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **cloud_redis_config,
    )
    return result


class _VPNServiceSpec(spec.BaseSpec):
  """Spec needed to configure a vpn tunnel between two vm_groups.

  Since vpn_gateway may be across cloud providers we only create tunnel when
  vpn_gateway's are up and known
  """

  name: str

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super(_VPNServiceSpec, self).__init__(
        component_full_name, flag_values=flag_values, **kwargs
    )
    if not self.name:
      self.name = 'pkb-vpn-svc-{0}'.format(flag_values.run_uri)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments to
      construct in order to decode the named option.
    """
    result = super(_VPNServiceSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'shared_key': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'name': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'tunnel_count': (
            option_decoders.IntDecoder,
            {'default': 1, 'none_ok': True},
        ),
        'gateway_count': (
            option_decoders.IntDecoder,
            {'default': 1, 'none_ok': True},
        ),
        'routing_type': (
            option_decoders.StringDecoder,
            {'default': 'static', 'none_ok': True},
        ),
        'ike_version': (
            option_decoders.IntDecoder,
            {'default': 1, 'none_ok': True},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super(_VPNServiceSpec, cls)._ApplyFlags(config_values, flag_values)
    if flag_values['vpn_service_tunnel_count'].present:
      config_values['tunnel_count'] = flag_values.vpn_service_tunnel_count
    if flag_values['vpn_service_gateway_count'].present:
      config_values['gateway_count'] = flag_values.vpn_service_gateway_count
    if flag_values['vpn_service_name'].present:
      config_values['name'] = flag_values.vpn_service_name
    if flag_values['vpn_service_shared_key'].present:
      config_values['shared_key'] = flag_values.vpn_service_shared_key
    if flag_values['vpn_service_routing_type'].present:
      config_values['routing_type'] = flag_values.vpn_service_routing_type
    if flag_values['vpn_service_ike_version'].present:
      config_values['ike_version'] = flag_values.vpn_service_ike_version


class _VPNServiceDecoder(option_decoders.TypeVerifier):
  """Validate the vpn_service dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify vpn_service dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _VPNService built from the config passed in in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    vpn_service_config = super(_VPNServiceDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = _VPNServiceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **vpn_service_config,
    )
    return result


class AppGroupSpec(spec.BaseSpec):
  """Configurable options of a AppService group.

  Attributes:

  SPEC_TYPE: The class / spec name.
  app_runtime: The runtime environment (e.g. java).
  app_type: The type / workload of the app (e.g. echo).
  appservice_count: The number of app services in the group.
  """

  SPEC_TYPE = 'AppGroupSpec'
  app_runtime: str
  app_type: str
  appservice_count: int

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
        'app_runtime': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'app_type': (
            option_decoders.StringDecoder,
            {'default': None, 'none_ok': True},
        ),
        'appservice_count': (option_decoders.IntDecoder, {'default': 1}),
        'appservice_spec': (_AppServiceDecoder, {}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['appservice_count'].present:
      config_values['appservice_count'] = flag_values.appservice_count
    if flag_values['app_runtime'].present:
      config_values['app_runtime'] = flag_values.app_runtime
    if flag_values['app_type'].present:
      config_values['app_type'] = flag_values.app_type


class _AppGroupsDecoder(option_decoders.TypeVerifier):
  """Verify app_groups dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verifys app_groups dictionary of a benchmark config object.

    Args:
      value: dict. Config dictionary.
      component_full_name: string. Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues. Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
     dict mapping app group name string to AppGroupSpec.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    app_group_configs = super(_AppGroupsDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    result = {}
    for app_group_name, app_group_config in six.iteritems(app_group_configs):
      result[app_group_name] = AppGroupSpec(
          '{0}.{1}'.format(
              self._GetOptionFullName(component_full_name), app_group_name
          ),
          flag_values=flag_values,
          **app_group_config,
      )
    return result


class _AppServiceDecoder(option_decoders.TypeVerifier):
  """Verify app_service dict of a benchmark config object."""

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
      errors.Config.InvalidValue upon invalid input value.
    """
    config = super(_AppServiceDecoder, self).Decode(
        value, component_full_name, flag_values
    )
    spec_cls = app_service.GetAppServiceSpecClass(
        flag_values.appservice or config.get('appservice')
    )
    return spec_cls(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **config,
    )


class _MessagingServiceSpec(spec.BaseSpec):
  """Specs needed to configure messaging service."""

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments to
      construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        # TODO(odiego): Add support for push delivery mechanism
        'delivery': (option_decoders.EnumDecoder, {'valid_values': ('pull',)}),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    # TODO(odiego): Handle delivery when adding more delivery mechanisms


class _MessagingServiceDecoder(option_decoders.TypeVerifier):
  """Validate the messaging_service dictionary of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify messaging_service dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _MessagingServiceSpec object built from the config passed in in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    messaging_service_config = super().Decode(
        value, component_full_name, flag_values
    )
    result = _MessagingServiceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **messaging_service_config,
    )
    return result


class _DataDiscoveryServiceSpec(spec.BaseSpec):
  """Specs needed to configure data discovery service."""

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments to
      construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (
            option_decoders.EnumDecoder,
            {'valid_values': provider_info.VALID_CLOUDS},
        ),
        'service_type': (
            option_decoders.EnumDecoder,
            {
                'default': data_discovery_service.GLUE,
                'valid_values': [
                    data_discovery_service.GLUE,
                ],
            },
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud


class _DataDiscoveryServiceDecoder(option_decoders.TypeVerifier):
  """Validate the data_discovery_service dict of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verify data_discovery_service dict of a benchmark config object.

    Args:
      value: dict. Config dictionary
      component_full_name: string.  Fully qualified name of the configurable
        component containing the config option.
      flag_values: flags.FlagValues.  Runtime flag values to be propagated to
        BaseSpec constructors.

    Returns:
      _DataDiscoveryServiceSpec object built from the config passed in value.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    if value is None:
      value = {}
    data_discovery_service_config = super().Decode(
        value, component_full_name, flag_values
    )
    result = _DataDiscoveryServiceSpec(
        self._GetOptionFullName(component_full_name),
        flag_values,
        **data_discovery_service_config,
    )
    return result


class _KeyDecoder(option_decoders.TypeVerifier):
  """Validates the key dict of a benchmark config object."""

  def Decode(self, value, component_full_name, flag_values):
    """Verifies the key dict of a benchmark config object."""
    key_config = super().Decode(value, component_full_name, flag_values)
    if 'cloud' in key_config:
      providers.LoadProvider(key_config['cloud'])
      key_spec_class = key.GetKeySpecClass(key_config['cloud'])
    else:
      raise errors.Config.InvalidValue(
          'Required attribute "cloud" missing from "key" config.'
      )
    return key_spec_class(
        self._GetOptionFullName(component_full_name), flag_values, **key_config
    )


class BenchmarkConfigSpec(spec.BaseSpec):
  """Configurable options of a benchmark run.

  Attributes:
    description: None or string. Description of the benchmark to run.
    name: Optional. The name of the benchmark
    flags: dict. Values to use for each flag while executing the benchmark.
    vm_groups: dict mapping VM group name string to _VmGroupSpec. Configurable
      options for each VM group used by the benchmark.
  """

  vm_groups: dict[str, vm_group_decoders.VmGroupSpec]

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
          mismatched_os_types.append(
              '{0}.vm_groups[{1}].os_type: {2}'.format(
                  component_full_name,
                  repr(group_name),
                  repr(group_spec.os_type),
              )
          )
      if mismatched_os_types:
        raise errors.Config.InvalidValue(
            'VM groups in {0} may only have the following OS types: {1}. The '
            'following VM group options are invalid:{2}{3}'.format(
                component_full_name,
                ', '.join(repr(os_type) for os_type in expected_os_types),
                os.linesep,
                os.linesep.join(mismatched_os_types),
            )
        )

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
        'name': (option_decoders.StringDecoder, {'default': None}),
        'flags': (
            option_decoders.TypeVerifier,
            {'default': None, 'none_ok': True, 'valid_types': (dict,)},
        ),
        'vm_groups': (vm_group_decoders.VmGroupsDecoder, {'default': {}}),
        'placement_group_specs': (_PlacementGroupSpecsDecoder, {'default': {}}),
        'container_cluster': (
            container_spec.ContainerClusterSpecDecoder,
            {'default': None},
        ),
        'container_registry': (
            container_spec.ContainerRegistryDecoder,
            {'default': None},
        ),
        'container_specs': (
            container_spec.ContainerSpecsDecoder,
            {'default': None},
        ),
        'dpb_service': (_DpbServiceDecoder, {'default': None}),
        'relational_db': (_RelationalDbDecoder, {'default': None}),
        'tpu_groups': (_TpuGroupsDecoder, {'default': {}}),
        'edw_compute_resource': (_EdwComputeResourceDecoder, {'default': None}),
        'edw_service': (_EdwServiceDecoder, {'default': None}),
        'example_resource': (_ExampleResourceDecoder, {'default': None}),
        'base_job': (_BaseJobDecoder, {'default': None}),
        'cloud_redis': (_CloudRedisDecoder, {'default': None}),
        'vpn_service': (_VPNServiceDecoder, {'default': None}),
        'app_groups': (_AppGroupsDecoder, {'default': {}}),
        'vpc_peering': (
            option_decoders.BooleanDecoder,
            {
                'default': False,
                'none_ok': True,
            },
        ),
        'non_relational_db': (
            _NonRelationalDbDecoder,
            {
                'default': None,
                'none_ok': True,
            },
        ),
        'messaging_service': (
            _MessagingServiceDecoder,
            {
                'default': None,
            },
        ),
        'ai_model': (_ManagedAiModelSpecDecoder, {'default': None}),
        'data_discovery_service': (
            _DataDiscoveryServiceDecoder,
            {
                'default': None,
                'none_ok': True,
            },
        ),
        'key': (
            _KeyDecoder,
            {
                'default': None,
                'none_ok': True,
            },
        ),
        # A place to hold temporary data
        'temporary': (
            option_decoders.TypeVerifier,
            {'default': None, 'none_ok': True, 'valid_types': (dict,)},
        ),
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
          component_full_name, config, decoders, flag_values
      )

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
