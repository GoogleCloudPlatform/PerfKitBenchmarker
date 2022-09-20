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

Instances can be created and deleted.
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
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import freeze_restore_spec
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
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

# Valid GCP Spanner types:
DEFAULT_SPANNER_TYPE = 'default'

_DEFAULT_REGION = 'us-central1'
_DEFAULT_DESCRIPTION = 'Spanner instance created by PKB.'
_DEFAULT_DDL = """
  CREATE TABLE pkb_table (
    id     STRING(MAX),
    field0 STRING(MAX)
  ) PRIMARY KEY(id)
  """
_DEFAULT_ENDPOINT = 'https://spanner.googleapis.com'
_DEFAULT_NODES = 1
_FROZEN_NODE_COUNT = 1

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
class SpannerSpec(freeze_restore_spec.FreezeRestoreSpec):
  """Configurable options of a Spanner instance."""

  # Needed for registering the spec class.
  SPEC_TYPE = 'SpannerSpec'
  SPEC_ATTRS = ['SERVICE_TYPE']
  SERVICE_TYPE = DEFAULT_SPANNER_TYPE

  service_type: str
  name: str
  description: str
  database: str
  ddl: str
  config: str
  nodes: int
  project: str

  def __init__(self,
               component_full_name: str,
               flag_values: Optional[flags.FlagValues] = None,
               **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

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
        'service_type': (
            option_decoders.EnumDecoder,
            {
                'valid_values': [
                    DEFAULT_SPANNER_TYPE,
                ],
                'default': DEFAULT_SPANNER_TYPE
            }),
        'name': (option_decoders.StringDecoder, _NONE_OK),
        'database': (option_decoders.StringDecoder, _NONE_OK),
        'description': (option_decoders.StringDecoder, _NONE_OK),
        'ddl': (option_decoders.StringDecoder, _NONE_OK),
        'config': (option_decoders.StringDecoder, _NONE_OK),
        'nodes': (option_decoders.IntDecoder, _NONE_OK),
        'project': (option_decoders.StringDecoder, _NONE_OK),
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
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud_spanner_config'].present:
      config_values['config'] = flag_values.cloud_spanner_config
    if flag_values['cloud_spanner_nodes'].present:
      config_values['nodes'] = flag_values.cloud_spanner_nodes
    if flag_values['cloud_spanner_project'].present:
      config_values['project'] = flag_values.cloud_spanner_project


def GetSpannerSpecClass(service_type) -> Optional[spec.BaseSpecMetaClass]:
  """Return the SpannerSpec class corresponding to 'service_type'."""
  return spec.GetSpecClass(SpannerSpec, SERVICE_TYPE=service_type)


class GcpSpannerInstance(resource.BaseResource):
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
    ddl:         The schema of the database.
  """
  # Required for registering the class.
  RESOURCE_TYPE = 'GcpSpannerInstance'
  REQUIRED_ATTRS = ['SERVICE_TYPE']
  SERVICE_TYPE = DEFAULT_SPANNER_TYPE

  def __init__(self,
               name: Optional[str] = None,
               description: Optional[str] = None,
               database: Optional[str] = None,
               ddl: Optional[str] = None,
               config: Optional[str] = None,
               nodes: Optional[int] = None,
               project: Optional[str] = None,
               **kwargs):
    super(GcpSpannerInstance, self).__init__(**kwargs)
    self.name = name or f'pkb-instance-{FLAGS.run_uri}'
    self.database = database or f'pkb-database-{FLAGS.run_uri}'
    self._description = description or _DEFAULT_DESCRIPTION
    self._ddl = ddl or _DEFAULT_DDL
    self._config = config or self._GetDefaultConfig()
    self.nodes = nodes or _DEFAULT_NODES
    self._end_point = None

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = (
        project or FLAGS.project or util.GetDefaultProject())
    self.zone = None

  def _GetDefaultConfig(self) -> str:
    """Gets the config that corresponds the region used for the test."""
    try:
      region = util.GetRegionFromZone(FLAGS.zone[0])
    except IndexError:
      region = _DEFAULT_REGION
    return f'regional-{region}'

  @classmethod
  def FromSpec(cls, spanner_spec: SpannerSpec) -> 'GcpSpannerInstance':
    """Initialize Spanner from the provided spec."""
    return cls(
        name=spanner_spec.name,
        description=spanner_spec.description,
        database=spanner_spec.database,
        ddl=spanner_spec.ddl,
        config=spanner_spec.config,
        nodes=spanner_spec.nodes,
        project=spanner_spec.project,
        enable_freeze_restore=spanner_spec.enable_freeze_restore,
        create_on_restore_error=spanner_spec.create_on_restore_error,
        delete_on_freeze_error=spanner_spec.delete_on_freeze_error)

  def _Create(self) -> None:
    """Creates the instance, the database, and update the schema."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'create', self.name)
    cmd.flags['description'] = self._description
    cmd.flags['nodes'] = self.nodes
    cmd.flags['config'] = self._config
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner instance failed.')
      return

    self._UpdateLabels(util.GetDefaultTags())

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'create',
                             self.database)
    cmd.flags['instance'] = self.name
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner database failed.')
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'ddl', 'update',
                             self.database)
    cmd.flags['instance'] = self.name
    cmd.flags['ddl'] = self._ddl
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Update GCP Spanner database schema failed.')
    else:
      logging.info('Created GCP Spanner instance and database.')

  def _Delete(self) -> None:
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'delete',
                             self.name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Delete GCP Spanner instance failed.')
    else:
      logging.info('Deleted GCP Spanner instance.')

  def _Exists(self, instance_only: bool = False) -> bool:
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self.name)

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP Spanner instance %s.', self.name)
      return False

    if instance_only:
      return True

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'describe',
                             self.database)
    cmd.flags['instance'] = self.name

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find GCP Spanner database %s.', self.database)
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
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'update', self.name)
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
                             self.name)
    stdout, _, _ = cmd.Issue(raise_on_failure=True)
    return json.loads(stdout).get('labels', {})

  def _UpdateLabels(self, labels: Dict[str, Any]) -> None:
    """Updates the labels of the current instance."""
    header = {'Authorization': f'Bearer {util.GetAccessToken()}'}
    url = (f'{self.GetEndPoint()}/v1/projects/'
           f'{self.project}/instances/{self.name}')
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
        'gcp_spanner_name': self.name,
        'gcp_spanner_database': self.database,
        'gcp_spanner_node_count': self.nodes,
        'gcp_spanner_ddl': self._ddl,
        'gcp_spanner_config': self._config,
        'gcp_spanner_endpoint': self.GetEndPoint()
    }

  def GetAverageCpuUsage(self, duration_minutes: int) -> float:
    """Gets the average high priority CPU usage through the time duration."""
    client = monitoring_v3.MetricServiceClient()
    # It takes up to 3 minutes for CPU metrics to appear.
    end_timestamp = time.time() - CPU_API_DELAY_SECONDS
    cpu_query = query.Query(
        client, project=self.project,
        metric_type='spanner.googleapis.com/instance/cpu/utilization_by_priority',
        end_time=datetime.datetime.utcfromtimestamp(end_timestamp),
        minutes=duration_minutes)
    # Filter by high priority
    cpu_query = cpu_query.select_metrics(
        database=self.database, priority='high')
    # Filter by the Spanner instance
    cpu_query = cpu_query.select_resources(
        instance_id=self.name, project_id=self.project)
    # Aggregate user and system high priority by the minute
    time_series = list(cpu_query)
    # Expect 2 metrics: user and system high-priority CPU
    if len(time_series) != 2:
      raise errors.Benchmarks.RunError(
          'Expected 2 metrics (user and system) for Spanner high-priority CPU '
          f'utilization query, got {len(time_series)}')
    cpu_aggregated = [
        user.value.double_value + system.value.double_value
        for user, system in zip(time_series[0].points, time_series[1].points)
    ]
    average_cpu = statistics.mean(cpu_aggregated)
    logging.info('CPU aggregated: %s', cpu_aggregated)
    logging.info('Average CPU for the %s minutes ending at %s: %s',
                 duration_minutes,
                 datetime.datetime.fromtimestamp(end_timestamp), average_cpu)
    return average_cpu

  def CalculateRecommendedThroughput(self, read_proportion: float,
                                     write_proportion: float) -> int:
    """Returns the recommended throughput based on the workload and nodes."""
    if read_proportion + write_proportion != 1:
      raise errors.Benchmarks.RunError(
          'Unrecognized workload, read + write proportion must be equal to 1, '
          f'got {read_proportion} + {write_proportion}.')
    # Calculates the starting throughput based off of each node being able to
    # handle 10k QPS of reads or 2k QPS of writes. For example, for a 50/50
    # workload, run at a QPS target of 1666 reads + 1666 writes = 3333 (round).
    a = np.array([[1 / _READ_OPS_PER_NODE, 1 / _WRITE_OPS_PER_NODE],
                  [write_proportion, -(1 - write_proportion)]])
    b = np.array([1, 0])
    result = np.linalg.solve(a, b)
    return int(sum(result) * self.nodes)


def GetSpannerClass(
    service_type: str) -> Optional[resource.AutoRegisterResourceMeta]:
  """Return the Spanner class associated with service_type."""
  return resource.GetResourceClass(
      GcpSpannerInstance, SERVICE_TYPE=service_type)
