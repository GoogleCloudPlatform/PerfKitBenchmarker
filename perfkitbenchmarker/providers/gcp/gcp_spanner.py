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
"""Module containing class for GCP's spanner instances.

Instances can be created and deleted. Providing an instance ID and database ID
causes the instance to be user_managed, meaning PKB will not manage any
lifecycle for the instance.
"""

import dataclasses
import datetime
import json
import logging
import statistics
from typing import Any, Dict

from absl import flags
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query
import numpy as np
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.gcp import util
import requests


FLAGS = flags.FLAGS
flags.DEFINE_string(
    'cloud_spanner_config',
    None,
    'The config for the Cloud Spanner instance. Use default config if unset.',
)
flags.DEFINE_integer(
    'cloud_spanner_nodes',
    None,
    'The number of nodes for the Cloud Spanner instance.',
)
_LOAD_NODES = flags.DEFINE_integer(
    'cloud_spanner_load_nodes',
    None,
    'The number of nodes for the Cloud Spanner instance to use for the load'
    ' phase. Assumes that the benchmark calls UpdateRunCapacity to set the '
    ' correct node count manually before the run phase.',
)
flags.DEFINE_string(
    'cloud_spanner_project',
    None,
    'The project for the Cloud Spanner instance. Use default project if unset.',
)
flags.DEFINE_string(
    'cloud_spanner_instance',
    None,
    'The name of the static Cloud Spanner instance. New '
    'instance created if unset.',
)
flags.DEFINE_string(
    'cloud_spanner_database',
    None,
    'The name of the static Cloud Spanner database. New '
    'database created if unset. cloud_spanner_instance flag is '
    'mandatory to use this flag.',
)
_MAX_COMMIT_DELAY = flags.DEFINE_integer(
    'cloud_spanner_max_commit_delay',
    None,
    'A delay to batch writes to increase throughput but also increase latency.'
    ' See https://cloud.google.com/spanner/docs/throughput-optimized-writes',
)

# Flags related to managed autoscaler
# https://cloud.google.com/spanner/docs/create-manage-instances#gcloud
flags.DEFINE_boolean(
    'cloud_spanner_autoscaler',
    False,
    'Turn on the autoscaler for the spanner instance.',
)

flags.DEFINE_integer(
    'cloud_spanner_autoscaler_min_processing_units',
    None,
    'Minimum number of processing units. Autoscaler will not go lower than'
    ' this.',
)

flags.DEFINE_integer(
    'cloud_spanner_autoscaler_max_processing_units',
    None,
    'Maximum number of processing units. Autoscaler will not go higher than'
    ' this.',
)

flags.DEFINE_integer(
    'cloud_spanner_autoscaler_high_priority_cpu_target',
    None,
    'The CPU usage when spanner starts scaling.',
)

flags.DEFINE_integer(
    'cloud_spanner_autoscaler_storage_target',
    None,
    'Storage target when spanner starts scaling.',
)


# Type aliases
_RelationalDbSpec = relational_db_spec.RelationalDbSpec

_DEFAULT_REGION = 'us-central1'
_DEFAULT_DESCRIPTION = 'Created by PKB.'
_DEFAULT_ENDPOINT = 'https://spanner.googleapis.com'
_DEFAULT_NODES = 1
_FROZEN_NODE_COUNT = 1

_DEFAULT_MIN_PROCESSING_UNITS = 5000
_DEFAULT_MAX_PROCESSING_UNITS = 50000
_DEFAULT_HIGH_PRIORITY_CPU_TARGET = 65
_DEFAULT_STORAGE_TARGET = 95

# Dialect options
GOOGLESQL = 'GOOGLE_STANDARD_SQL'
POSTGRESQL = 'POSTGRESQL'
_VALID_DIALECTS = frozenset([GOOGLESQL, POSTGRESQL])

# Common decoder configuration option.
_NONE_OK = {'default': None, 'none_ok': True}

# Spanner CPU Monitoring API has a 3 minute delay. See
# https://cloud.google.com/monitoring/api/metrics_gcp#gcp-spanner,
CPU_API_DELAY_MINUTES = 3
CPU_API_DELAY_SECONDS = CPU_API_DELAY_MINUTES * 60

# For more information on QPS expectations, see
# https://cloud.google.com/spanner/docs/performance#typical-workloads
_READ_OPS_PER_NODE = 10000
_WRITE_OPS_PER_NODE = 2000
_ADJUSTED_READ_OPS_PER_NODE = 15000
_ADJUSTED_WRITE_OPS_PER_NODE = 3000
_ADJUSTED_CONFIG_REGIONS = ['regional-us-east4']


@dataclasses.dataclass
class SpannerSpec(relational_db_spec.RelationalDbSpec):
  """Configurable options of a Spanner instance."""

  SERVICE_TYPE = 'spanner'

  spanner_instance_id: str
  spanner_database_id: str
  spanner_description: str
  spanner_config: str
  spanner_nodes: int
  spanner_load_nodes: int
  spanner_project: str
  spanner_autoscaler: bool
  spanner_min_processing_units: int
  spanner_max_processing_units: int
  spanner_high_priority_cpu_target: int
  spanner_storage_target: int

  def __init__(
      self,
      component_full_name: str,
      flag_values: flags.FlagValues | None = None,
      **kwargs,
  ):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'spanner_instance_id': (option_decoders.StringDecoder, _NONE_OK),
        # Base class supplies a database_name, but this is easier to find.
        'spanner_database_id': (option_decoders.StringDecoder, _NONE_OK),
        'spanner_description': (option_decoders.StringDecoder, _NONE_OK),
        'spanner_config': (option_decoders.StringDecoder, _NONE_OK),
        'spanner_nodes': (option_decoders.IntDecoder, _NONE_OK),
        'spanner_load_nodes': (option_decoders.IntDecoder, _NONE_OK),
        'spanner_project': (option_decoders.StringDecoder, _NONE_OK),
        'db_spec': (spec.PerCloudConfigDecoder, _NONE_OK),
        'db_disk_spec': (spec.PerCloudConfigDecoder, _NONE_OK),
        'spanner_autoscaler': (option_decoders.BooleanDecoder, _NONE_OK),
        'spanner_min_processing_units': (option_decoders.IntDecoder, _NONE_OK),
        'spanner_max_processing_units': (option_decoders.IntDecoder, _NONE_OK),
        'spanner_high_priority_cpu_target': (
            option_decoders.IntDecoder,
            _NONE_OK,
        ),
        'spanner_storage_target': (option_decoders.IntDecoder, _NONE_OK),
    })
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud_spanner_instance'].present:
      config_values['spanner_instance_id'] = flag_values.cloud_spanner_instance
    if flag_values['cloud_spanner_database'].present:
      config_values['spanner_database_id'] = flag_values.cloud_spanner_database
    if flag_values['cloud_spanner_config'].present:
      config_values['spanner_config'] = flag_values.cloud_spanner_config
    if flag_values['cloud_spanner_nodes'].present:
      config_values['spanner_nodes'] = flag_values.cloud_spanner_nodes
    if flag_values['cloud_spanner_load_nodes'].present:
      config_values['spanner_load_nodes'] = flag_values.cloud_spanner_load_nodes
    if flag_values['cloud_spanner_project'].present:
      config_values['spanner_project'] = flag_values.cloud_spanner_project

    if flag_values['cloud_spanner_autoscaler'].present:
      config_values['spanner_autoscaler'] = flag_values.cloud_spanner_autoscaler
    if flag_values['cloud_spanner_autoscaler_min_processing_units'].present:
      config_values['spanner_min_processing_units'] = (
          flag_values.cloud_spanner_autoscaler_min_processing_units
      )
    if flag_values['cloud_spanner_autoscaler_max_processing_units'].present:
      config_values['spanner_max_processing_units'] = (
          flag_values.cloud_spanner_autoscaler_max_processing_units
      )
    if flag_values['cloud_spanner_autoscaler_high_priority_cpu_target'].present:
      config_values['spanner_high_priority_cpu_target'] = (
          flag_values.cloud_spanner_autoscaler_high_priority_cpu_target
      )
    if flag_values['cloud_spanner_autoscaler_storage_target'].present:
      config_values['spanner_storage_target'] = (
          flag_values.cloud_spanner_autoscaler_storage_target
      )


class GcpSpannerInstance(relational_db.BaseRelationalDb):
  """Object representing a GCP Spanner Instance.

  The project and Cloud Spanner config must already exist. Instance and database
  will be created and torn down before and after the test.

  The following parameters are overridden by the corresponding FLAGs.
    project:     FLAGS.cloud_spanner_project
    config:      FLAGS.cloud_spanner_config
    nodes:       FLAGS.cloud_spanner_nodes

  Attributes:
    name:        Name of the instance to create.
    description: Description of the instance.
    database:    Name of the database to create
  """

  CLOUD = 'GCP'
  IS_MANAGED = True
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']

  def __init__(self, db_spec: SpannerSpec, **kwargs):
    super().__init__(db_spec, **kwargs)
    self.instance_id = (
        db_spec.spanner_instance_id or f'pkb-instance-{FLAGS.run_uri}'
    )
    self.database = (
        db_spec.spanner_database_id or f'pkb-database-{FLAGS.run_uri}'
    )
    self._description = db_spec.spanner_description or _DEFAULT_DESCRIPTION
    self._config = db_spec.spanner_config or self._GetDefaultConfig()
    self.nodes = db_spec.spanner_nodes or _DEFAULT_NODES
    self._load_nodes = db_spec.spanner_load_nodes or self.nodes
    self._api_endpoint = None
    self._autoscaler = db_spec.spanner_autoscaler
    self._min_processing_units = (
        db_spec.spanner_min_processing_units or _DEFAULT_MIN_PROCESSING_UNITS
    )
    self._max_processing_units = (
        db_spec.spanner_max_processing_units or _DEFAULT_MAX_PROCESSING_UNITS
    )
    self._high_priority_cpu_target = (
        db_spec.spanner_high_priority_cpu_target
        or _DEFAULT_HIGH_PRIORITY_CPU_TARGET
    )
    self._storage_target = (
        db_spec.spanner_storage_target or _DEFAULT_STORAGE_TARGET
    )

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = (
        db_spec.spanner_project or FLAGS.project or util.GetDefaultProject()
    )

  def _GetDefaultConfig(self) -> str:
    """Gets the config that corresponds the region used for the test."""
    try:
      region = util.GetRegionFromZone(FLAGS.zone[0])
    except IndexError:
      region = _DEFAULT_REGION
    return f'regional-{region}'

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    return 'default'

  def _Create(self) -> None:
    """Creates the instance, the database, and update the schema."""
    cmd = util.GcloudCommand(
        self, 'spanner', 'instances', 'create', self.instance_id
    )
    cmd.flags['description'] = self._description
    cmd.flags['config'] = self._config

    if self.spec.db_tier:
      cmd.flags['edition'] = self.spec.db_tier

    if self._autoscaler:
      cmd.use_beta_gcloud = True
      cmd.flags['autoscaling-min-processing-units'] = self._min_processing_units
      cmd.flags['autoscaling-max-processing-units'] = self._max_processing_units
      cmd.flags['autoscaling-high-priority-cpu-target'] = (
          self._high_priority_cpu_target
      )
      cmd.flags['autoscaling-storage-target'] = self._storage_target
    else:
      cmd.flags['nodes'] = self.nodes
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      # TODO(user) Currently loops if the database doesn't exist. To fix
      # this we should move waiting for the database to be ready from Exists
      # to this function.
      if self._Exists():
        logging.info(
            'Found an existing instance, setting user_managed to True.'
        )
        self.user_managed = True
      else:
        logging.error('Create GCP Spanner instance failed.')
        return

    self._UpdateLabels(util.GetDefaultTags())

    self.CreateDatabase(self.database)

  def CreateTables(self, ddl: str) -> None:
    """Creates the tables specified by the DDL.

    If the table already exists, this is a no-op.

    Args:
      ddl: The DDL statement used to create the table schema.

    Raises:
      errors.Benchmarks.RunError if updating the DDL fails.
    """
    cmd = util.GcloudCommand(
        self, 'spanner', 'databases', 'ddl', 'update', self.database
    )
    cmd.flags['instance'] = self.instance_id
    cmd.flags['ddl'] = ddl
    _, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if 'Duplicate name in schema' in stderr:
      logging.info('Table already exists, skipping.')
      return
    if retcode != 0:
      raise errors.Benchmarks.RunError('Failed to update GCP Spanner DDL.')

  def _Delete(self) -> None:
    """Deletes the instance."""
    cmd = util.GcloudCommand(
        self, 'spanner', 'instances', 'delete', self.instance_id
    )
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Delete GCP Spanner instance failed.')
    else:
      logging.info('Deleted GCP Spanner instance.')

  def CreateDatabase(self, database_name: str) -> tuple[str, str]:
    """Creates the database."""
    cmd = util.GcloudCommand(
        self, 'spanner', 'databases', 'create', database_name
    )
    cmd.flags['instance'] = self.instance_id
    cmd.flags['database-dialect'] = self.dialect
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner database failed.')
    return stdout, stderr

  def DeleteDatabase(self, database_name: str) -> tuple[str, str]:
    """Deletes the database."""
    cmd = util.GcloudCommand(
        self, 'spanner', 'databases', 'delete', database_name
    )
    cmd.flags['instance'] = self.instance_id
    stdout, stderr, _ = cmd.Issue(raise_on_failure=False)
    return stdout, stderr

  def _Exists(self, instance_only: bool = False) -> bool:
    """Returns true if the instance and the database exists."""
    _, retcode = self._DescribeInstance(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP Spanner instance %s.', self.instance_id)
      return False

    if instance_only:
      return True

    cmd = util.GcloudCommand(
        self, 'spanner', 'databases', 'describe', self.database
    )
    cmd.flags['instance'] = self.instance_id

    # Do not log error or warning when checking existence.
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP Spanner database %s.', self.database)
      return False
    elif json.loads(stdout)['state'] != 'READY':
      logging.info('Waiting for database to be ready.')
      return False

    return True

  def GetApiEndPoint(self) -> str:
    """Returns the API endpoint override for Cloud Spanner."""
    if self._api_endpoint:
      return self._api_endpoint

    cmd = util.GcloudCommand(
        self, 'config', 'get-value', 'api_endpoint_overrides/spanner'
    )
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.warning('Fail to retrieve cloud spanner end point.')
    self._api_endpoint = json.loads(stdout) or _DEFAULT_ENDPOINT
    return self._api_endpoint

  def _WaitUntilInstanceReady(self) -> None:
    """Waits until the instance is ready."""
    # TODO(user): Refactor Spanner instance to use this method in Create
    while True:
      instance, _ = self._DescribeInstance()
      if json.loads(instance)['state'] == 'READY':
        break

  def _SetNodes(self, nodes: int) -> None:
    """Sets the number of nodes on the Spanner instance."""
    # Not yet supported for user managed instances since instance attributes
    # are not discovered.
    if self.user_managed:
      return

    # Cannot set nodes with autoscaler.
    if self._autoscaler:
      return
    current_nodes = self._GetNodes()
    if nodes == current_nodes:
      return
    logging.info('Updating node count from %s to %s.', current_nodes, nodes)
    cmd = util.GcloudCommand(
        self, 'spanner', 'instances', 'update', self.instance_id
    )
    cmd.flags['nodes'] = nodes
    cmd.Issue(raise_on_failure=True)
    self._WaitUntilInstanceReady()

  def UpdateCapacityForLoad(self) -> None:
    """See base class."""
    self._SetNodes(self._load_nodes)

  def UpdateCapacityForRun(self) -> None:
    """See base class."""
    self._SetNodes(self.nodes)

  def _Restore(self) -> None:
    """See base class.

    Increases the number of nodes on the instance to the specified number.  See
    https://cloud.google.com/spanner/pricing for Spanner pricing info.
    """
    self._SetNodes(self.nodes)

  def _Freeze(self) -> None:
    """See base class.

    Lowers the number of nodes on the instance to one. Note there are
    restrictions to being able to lower the number of nodes on an instance. See
    https://cloud.google.com/spanner/docs/create-manage-instances.
    """
    self._SetNodes(_FROZEN_NODE_COUNT)

  def _DescribeInstance(self, raise_on_failure=True) -> tuple[str, int]:
    """Returns `spanner instances describe` output for this instance."""
    cmd = util.GcloudCommand(
        self, 'spanner', 'instances', 'describe', self.instance_id
    )
    stdout, _, retcode = cmd.Issue(raise_on_failure=raise_on_failure)
    return stdout, retcode

  def _GetLabels(self) -> dict[str, Any]:
    """Gets labels from the current instance."""
    return json.loads(self._DescribeInstance()[0]).get('labels', {})

  def _GetNodes(self) -> int:
    """Gets node count from the current instance."""
    return json.loads(self._DescribeInstance()[0])['nodeCount']

  def _UpdateLabels(self, labels: Dict[str, Any]) -> None:
    """Updates the labels of the current instance."""
    header = {'Authorization': f'Bearer {util.GetAccessToken()}'}
    url = (
        f'{self.GetApiEndPoint()}/v1/projects/'
        f'{self.project}/instances/{self.instance_id}'
    )
    # Keep any existing labels
    tags = self._GetLabels()
    tags.update(labels)
    args = {
        'instance': {'labels': tags},
        'fieldMask': 'labels',
    }
    response = requests.patch(url, headers=header, json=args)
    logging.info(
        'Update labels: status code %s, %s', response.status_code, response.text
    )
    if response.status_code != 200:
      raise errors.Resource.UpdateError(
          f'Unable to update Spanner instance: {response.text}'
      )

  def _UpdateTimeout(self, timeout_minutes: int) -> None:
    """See base class."""
    labels = util.GetDefaultTags(timeout_minutes)
    self._UpdateLabels(labels)

  def GetResourceMetadata(self) -> Dict[Any, Any]:
    """Returns useful metadata about the instance."""
    metadata = {
        'gcp_spanner_name': self.instance_id,
        'gcp_spanner_database': self.database,
        'gcp_spanner_database_dialect': self.dialect,
        'gcp_spanner_config': self._config,
        'gcp_spanner_endpoint': self.GetApiEndPoint(),
        'gcp_spanner_load_node_count': self._load_nodes,
    }

    if self._autoscaler:
      metadata.update({
          'gcp_spanner_autoscaler': self._autoscaler,
          'gcp_spanner_min_processing_units': self._min_processing_units,
          'gcp_spanner_max_processing_units': self._max_processing_units,
          'gcp_spanner_high_priority_cpu_target': (
              self._high_priority_cpu_target
          ),
          'gcp_spanner_storage_target': self._storage_target,
      })
    else:
      metadata['gcp_spanner_node_count'] = self.nodes
    if _MAX_COMMIT_DELAY.value:
      metadata['gcp_spanner_max_commit_delay'] = _MAX_COMMIT_DELAY.value
    return metadata

  def GetAverageCpuUsage(
      self, duration_minutes: int, end_time: datetime.datetime
  ) -> float:
    """Gets the average high priority CPU usage through the time duration.

    Note that there is a delay for the API to get data, so this returns the
    average CPU usage in the period ending at `end_time` with missing data
    in the last CPU_API_DELAY_SECONDS.

    Args:
      duration_minutes: The time duration for which to measure the average CPU
        usage.
      end_time: The ending timestamp of the workload.

    Returns:
      The average CPU usage during the time period.
    """
    if duration_minutes * 60 <= CPU_API_DELAY_SECONDS:
      raise ValueError(
          f'Spanner API has a {CPU_API_DELAY_SECONDS} sec. delay in receiving'
          ' data, choose a longer duration to get CPU usage.'
      )

    client = monitoring_v3.MetricServiceClient()
    # It takes up to 3 minutes for CPU metrics to appear.
    cpu_query = query.Query(
        client,
        project=self.project,
        metric_type=(
            'spanner.googleapis.com/instance/cpu/utilization_by_priority'
        ),
        end_time=end_time,
        minutes=duration_minutes,
    )
    # Filter by high priority
    cpu_query = cpu_query.select_metrics(
        database=self.database, priority='high'
    )
    # Filter by the Spanner instance
    cpu_query = cpu_query.select_resources(
        instance_id=self.instance_id, project_id=self.project
    )
    # Aggregate user and system high priority by the minute
    time_series = list(cpu_query)
    # Expect 2 metrics: user and system high-priority CPU
    if len(time_series) != 2:
      raise errors.Benchmarks.RunError(
          'Expected 2 metrics (user and system) for Spanner high-priority CPU '
          f'utilization query, got {len(time_series)}'
      )
    logging.info('Instance %s utilization by minute:', self.instance_id)
    cpu_aggregated = []
    for user, system in zip(time_series[0].points, time_series[1].points):
      point_utilization = user.value.double_value + system.value.double_value
      point_time = datetime.datetime.fromtimestamp(
          user.interval.start_time.timestamp()
      )
      logging.info('%s: %s', point_time, point_utilization)
      cpu_aggregated.append(point_utilization)
    average_cpu = statistics.mean(cpu_aggregated)
    logging.info(
        'Instance %s average CPU utilization: %s',
        self.instance_id,
        average_cpu,
    )
    return average_cpu

  def CalculateTheoreticalMaxThroughput(
      self, read_proportion: float, write_proportion: float
  ) -> int:
    """Returns the theoretical max throughput based on the workload and nodes.

    This is the published theoretical max throughput of a Spanner node, see
    https://cloud.google.com/spanner/docs/cpu-utilization#recommended-max for
    more info.

    Args:
      read_proportion: the proportion of read requests in the workload.
      write_proportion: the propoertion of write requests in the workload.

    Returns:
      The max total QPS taking into account the published single node limits.
    """
    if read_proportion + write_proportion != 1:
      raise errors.Benchmarks.RunError(
          'Unrecognized workload, read + write proportion must be equal to 1, '
          f'got {read_proportion} + {write_proportion}.'
      )
    read_ops_per_node = _READ_OPS_PER_NODE
    write_ops_per_node = _WRITE_OPS_PER_NODE
    if self._config in _ADJUSTED_CONFIG_REGIONS:
      read_ops_per_node = _ADJUSTED_READ_OPS_PER_NODE
      write_ops_per_node = _ADJUSTED_WRITE_OPS_PER_NODE
    # Calculates the starting throughput based off of each node being able to
    # handle 10k QPS of reads or 2k QPS of writes. For example, for a 50/50
    # workload, run at a QPS target of 1666 reads + 1666 writes = 3333 (round).
    a = np.array([
        [1 / read_ops_per_node, 1 / write_ops_per_node],
        [write_proportion, -(1 - write_proportion)],
    ])
    b = np.array([1, 0])
    result = np.linalg.solve(a, b)
    return int(sum(result) * self.nodes)


class GoogleSqlGcpSpannerInstance(GcpSpannerInstance):
  """GoogleSQL-based Spanner instance."""

  ENGINE = sql_engine_utils.SPANNER_GOOGLESQL

  def __init__(self, db_spec: SpannerSpec, **kwargs: Any):
    super().__init__(db_spec, **kwargs)
    self.dialect = GOOGLESQL

  def _PostCreate(self):
    # Overrides BaseRelationalDB _PostCreate since client utils are
    # not yet implemented.
    if self.spec.db_flags:
      self._ApplyDbFlags()
    self._SetEndpoint()

  def GetDefaultPort(self) -> int:
    return 0  # Port is unused


class PostgresGcpSpannerInstance(GcpSpannerInstance):
  """PostgreSQL-based Spanner instance."""

  ENGINE = sql_engine_utils.SPANNER_POSTGRES

  def __init__(self, db_spec: SpannerSpec, **kwargs: Any):
    super().__init__(db_spec, **kwargs)
    self.dialect = POSTGRESQL
    self._endpoint = 'localhost'

  def GetDefaultPort(self) -> int:
    return relational_db.DEFAULT_POSTGRES_PORT

  def _GetDbConnectionProperties(
      self,
  ) -> sql_engine_utils.DbConnectionProperties:
    return sql_engine_utils.DbConnectionProperties(
        self.spec.engine,
        self.spec.engine_version,
        self.endpoint,
        self.port,
        self.spec.database_username,
        self.spec.database_password,
        self.instance_id,
        self.database,
        self.project,
    )
