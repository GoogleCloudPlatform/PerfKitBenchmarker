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
import time
from typing import Any, Dict, Optional

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
from perfkitbenchmarker.providers.gcp import util
import requests


FLAGS = flags.FLAGS
flags.DEFINE_string('cloud_spanner_config',
                    None,
                    'The config for the Cloud Spanner instance. Use default '
                    'config if unset.')
flags.DEFINE_integer('cloud_spanner_nodes', None,
                     'The number of nodes for the Cloud Spanner instance.')
flags.DEFINE_string('cloud_spanner_project',
                    None,
                    'The project for the Cloud Spanner instance. Use default '
                    'project if unset.')
flags.DEFINE_string('cloud_spanner_instance', None,
                    'The name of the static Cloud Spanner instance. New '
                    'instance created if unset.')
flags.DEFINE_string('cloud_spanner_database', None,
                    'The name of the static Cloud Spanner database. New '
                    'database created if unset. cloud_spanner_instance flag is '
                    'mandatory to use this flag.')

# Type aliases
_RelationalDbSpec = relational_db_spec.RelationalDbSpec

_DEFAULT_REGION = 'us-central1'
_DEFAULT_DESCRIPTION = 'Created by PKB.'
_DEFAULT_ENDPOINT = 'https://spanner.googleapis.com'
_DEFAULT_NODES = 1
_FROZEN_NODE_COUNT = 1

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
# https://cloud.google.com/spanner/docs/instance-configurations#regional-performance
_READ_OPS_PER_NODE = 10000
_WRITE_OPS_PER_NODE = 2000


@dataclasses.dataclass
class SpannerSpec(relational_db_spec.RelationalDbSpec):
  """Configurable options of a Spanner instance."""

  SERVICE_TYPE = 'spanner'

  spanner_instance_id: str
  spanner_database_id: str
  spanner_description: str
  spanner_config: str
  spanner_nodes: int
  spanner_project: str

  def __init__(self,
               component_full_name: str,
               flag_values: Optional[flags.FlagValues] = None,
               **kwargs):
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
        'spanner_project': (option_decoders.StringDecoder, _NONE_OK),
        'db_spec': (option_decoders.PerCloudConfigDecoder, _NONE_OK),
        'db_disk_spec': (option_decoders.PerCloudConfigDecoder, _NONE_OK),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values: dict[str, Any],
                  flag_values: flags.FlagValues) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
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
    if flag_values['cloud_spanner_project'].present:
      config_values['spanner_project'] = flag_values.cloud_spanner_project


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
    super(GcpSpannerInstance, self).__init__(db_spec, **kwargs)
    self.instance_id = (
        db_spec.spanner_instance_id or f'pkb-instance-{FLAGS.run_uri}'
    )
    self.database = (
        db_spec.spanner_database_id or f'pkb-database-{FLAGS.run_uri}'
    )
    self._description = db_spec.spanner_description or _DEFAULT_DESCRIPTION
    self._config = db_spec.spanner_config or self._GetDefaultConfig()
    self.nodes = db_spec.spanner_nodes or _DEFAULT_NODES
    self._end_point = None

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = (
        db_spec.spanner_project or FLAGS.project or util.GetDefaultProject())

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
    cmd.flags['nodes'] = self.nodes
    cmd.flags['config'] = self._config
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

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'create',
                             self.database)
    cmd.flags['instance'] = self.instance_id
    cmd.flags['database-dialect'] = self.dialect
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner database failed.')

  def CreateTables(self, ddl: str) -> None:
    """Creates the tables specified by the DDL.

    If the table already exists, this is a no-op.

    Args:
      ddl: The DDL statement used to create the table schema.

    Raises:
      errors.Benchmarks.RunError if updating the DDL fails.
    """
    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'ddl', 'update',
                             self.database)
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
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'delete',
                             self.instance_id)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Delete GCP Spanner instance failed.')
    else:
      logging.info('Deleted GCP Spanner instance.')

  def _Exists(self, instance_only: bool = False) -> bool:
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self.instance_id)

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP Spanner instance %s.', self.instance_id)
      return False

    if instance_only:
      return True

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'describe',
                             self.database)
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

  def GetEndPoint(self) -> str:
    """Returns the end point for Cloud Spanner."""
    if self._end_point:
      return self._end_point

    cmd = util.GcloudCommand(self, 'config', 'get-value',
                             'api_endpoint_overrides/spanner')
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.warning('Fail to retrieve cloud spanner end point.')
    self._end_point = json.loads(stdout) or _DEFAULT_ENDPOINT
    return self._end_point

  def _SetNodes(self, nodes: int) -> None:
    """Sets the number of nodes on the Spanner instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'update',
                             self.instance_id)
    cmd.flags['nodes'] = nodes
    cmd.Issue(raise_on_failure=True)

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

  def _GetLabels(self) -> Dict[str, Any]:
    """Gets labels from the current instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self.instance_id)
    stdout, _, _ = cmd.Issue(raise_on_failure=True)
    return json.loads(stdout).get('labels', {})

  def _UpdateLabels(self, labels: Dict[str, Any]) -> None:
    """Updates the labels of the current instance."""
    header = {'Authorization': f'Bearer {util.GetAccessToken()}'}
    url = (f'{self.GetEndPoint()}/v1/projects/'
           f'{self.project}/instances/{self.instance_id}')
    # Keep any existing labels
    tags = self._GetLabels()
    tags.update(labels)
    args = {
        'instance': {
            'labels': tags
        },
        'fieldMask': 'labels',
    }
    response = requests.patch(url, headers=header, json=args)
    logging.info('Update labels: status code %s, %s',
                 response.status_code, response.text)
    if response.status_code != 200:
      raise errors.Resource.UpdateError(
          f'Unable to update Spanner instance: {response.text}')

  def _UpdateTimeout(self, timeout_minutes: int) -> None:
    """See base class."""
    labels = util.GetDefaultTags(timeout_minutes)
    self._UpdateLabels(labels)

  def GetResourceMetadata(self) -> Dict[Any, Any]:
    """Returns useful metadata about the instance."""
    return {
        'gcp_spanner_name': self.instance_id,
        'gcp_spanner_database': self.database,
        'gcp_spanner_database_dialect': self.dialect,
        'gcp_spanner_node_count': self.nodes,
        'gcp_spanner_config': self._config,
        'gcp_spanner_endpoint': self.GetEndPoint()
    }

  def GetAverageCpuUsage(self, duration_minutes: int) -> float:
    """Gets the average high priority CPU usage through the time duration."""
    client = monitoring_v3.MetricServiceClient()
    # It takes up to 3 minutes for CPU metrics to appear.
    end_timestamp = time.time() - CPU_API_DELAY_SECONDS
    cpu_query = query.Query(
        client,
        project=self.project,
        metric_type=(
            'spanner.googleapis.com/instance/cpu/utilization_by_priority'
        ),
        end_time=datetime.datetime.utcfromtimestamp(end_timestamp),
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
    cpu_aggregated = [
        user.value.double_value + system.value.double_value
        for user, system in zip(time_series[0].points, time_series[1].points)
    ]
    average_cpu = statistics.mean(cpu_aggregated)
    logging.info('CPU aggregated: %s', cpu_aggregated)
    logging.info(
        'Average CPU for the %s minutes ending at %s: %s',
        duration_minutes,
        datetime.datetime.fromtimestamp(end_timestamp),
        average_cpu,
    )
    return average_cpu

  def CalculateRecommendedThroughput(
      self, read_proportion: float, write_proportion: float
  ) -> int:
    """Returns the recommended throughput based on the workload and nodes."""
    if read_proportion + write_proportion != 1:
      raise errors.Benchmarks.RunError(
          'Unrecognized workload, read + write proportion must be equal to 1, '
          f'got {read_proportion} + {write_proportion}.'
      )
    # Calculates the starting throughput based off of each node being able to
    # handle 10k QPS of reads or 2k QPS of writes. For example, for a 50/50
    # workload, run at a QPS target of 1666 reads + 1666 writes = 3333 (round).
    a = np.array([
        [1 / _READ_OPS_PER_NODE, 1 / _WRITE_OPS_PER_NODE],
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

  def _PostCreate(self):
    super()._PostCreate()
    # TODO(user) move to superclass.
    background_tasks.RunThreaded(
        lambda client_query_tools: client_query_tools.InstallPackages(),
        self.client_vms_query_tools,
    )

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
