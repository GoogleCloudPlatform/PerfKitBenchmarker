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

import json
import logging
from typing import Dict, Optional

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.gcp import util

DEFAULT_REGION = 'us-central1'

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


class SpannerSpec(spec.BaseSpec):
  """Configurable options of a Spanner instance."""
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
               flag_values: Dict[str, flags.FlagValues] = None,
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

    # Get values needed for defaults
    run_uri = FLAGS.run_uri

    result.update({
        'service_type': (
            option_decoders.EnumDecoder,
            {
                'valid_values': [
                    DEFAULT_SPANNER_TYPE,
                ],
                'default': DEFAULT_SPANNER_TYPE
            }),
        'name': (option_decoders.StringDecoder, {
            'default': f'pkb-{run_uri}'
        }),
        'description': (option_decoders.StringDecoder, {
            'default': 'Spanner instance created by PKB.',
        }),
        'database': (option_decoders.StringDecoder, {
            'default': f'pkb-db-{run_uri}'
        }),
        'ddl': (option_decoders.StringDecoder, {
            'default': None,
            'none_ok': True,
        }),
        'config': (option_decoders.StringDecoder, {
            'default': f'regional-{cls._GetDefaultRegion()}'
        }),
        'nodes': (option_decoders.IntDecoder, {
            'default': 1,
        }),
        'project': (option_decoders.StringDecoder, {
            'default': None
        }),
    })

    return result

  @classmethod
  def _GetDefaultRegion(cls):
    """Defaults to the region used for the test."""
    try:
      region = util.GetRegionFromZone(
          FLAGS.zones[0] if FLAGS.zones else FLAGS.zone[0])
    except IndexError:
      region = DEFAULT_REGION
    return region

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

  def __repr__(self):
    return (f'SpannerSpec (service_type: {self.service_type}, '
            f'name: {self.name} '
            f'description: {self.description}, '
            f'database: {self.database}, '
            f'ddl: {self.ddl}, '
            f'config: {self.config}, '
            f'nodes: {self.nodes}, '
            f'project: {self.project})')


class GcpSpannerInstance(resource.BaseResource):
  """Object representing a GCP Spanner Instance.

  The project and Cloud Spanner config must already exist. Instance and database
  will be created and torn down before and after the test.

  The following parameters are set by corresponding FLAGs.
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
               name: str,
               description: str,
               database: str,
               ddl: str,
               config: str = None,
               nodes: int = None,
               project: str = None,
               **kwargs):
    super(GcpSpannerInstance, self).__init__(**kwargs)
    self._name = name
    self._description = description
    self._database = database
    self._ddl = ddl
    self._config = config or FLAGS.cloud_spanner_config
    self._nodes = nodes or FLAGS.cloud_spanner_nodes
    self._end_point = None

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = (
        project or FLAGS.project or util.GetDefaultProject())
    self.zone = None

  @classmethod
  def FromSpec(cls, spanner_spec: SpannerSpec) -> 'GcpSpannerInstance':
    """Initialize Spanner from the provided spec."""
    return cls(spanner_spec.name,
               spanner_spec.description,
               spanner_spec.database,
               spanner_spec.ddl,
               spanner_spec.config,
               spanner_spec.nodes,
               spanner_spec.project)

  def _Create(self):
    """Creates the instance, the database, and update the schema."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'create', self._name)
    cmd.flags['description'] = self._description
    cmd.flags['nodes'] = self._nodes
    cmd.flags['config'] = self._config
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner instance failed.')
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'create',
                             self._database)
    cmd.flags['instance'] = self._name
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Create GCP Spanner database failed.')
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'ddl', 'update',
                             self._database)
    cmd.flags['instance'] = self._name
    cmd.flags['ddl'] = self._ddl
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Update GCP Spanner database schema failed.')
    else:
      logging.info('Created GCP Spanner instance and database.')

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'delete',
                             self._name)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Delete GCP Spanner instance failed.')
    else:
      logging.info('Deleted GCP Spanner instance.')

  def _Exists(self, instance_only=False):
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self._name)

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not found GCP Spanner instances %s.', self._name)
      return False

    if instance_only:
      return True

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'describe',
                             self._database)
    cmd.flags['instance'] = self._name

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not found GCP Spanner database %s.', self._database)
      return False

    return True

  def GetEndPoint(self):
    """Returns the end point for Cloud Spanner."""
    if self._end_point:
      return self._end_point

    cmd = util.GcloudCommand(self, 'config', 'get-value',
                             'api_endpoint_overrides/spanner')
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.warning('Fail to retrieve cloud spanner end point.')
      return None
    self._end_point = json.loads(stdout)
    return self._end_point


def GetSpannerClass(
    service_type: str) -> Optional[resource.AutoRegisterResourceMeta]:
  """Return the Spanner class associated with service_type."""
  return resource.GetResourceClass(
      GcpSpannerInstance, SERVICE_TYPE=service_type)
