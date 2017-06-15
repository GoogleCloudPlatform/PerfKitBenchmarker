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

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

DEFAULT_CLOUD_SPANNER_CONFIG = 'regional-us-central1'

FLAGS = flags.FLAGS
flags.DEFINE_string('cloud_spanner_config',
                    DEFAULT_CLOUD_SPANNER_CONFIG,
                    'The config for the Cloud Spanner instance.')
flags.DEFINE_integer('cloud_spanner_nodes',
                     1,
                     'The number of nodes for the Cloud Spanner instance.')
flags.DEFINE_string('cloud_spanner_project',
                    None,
                    'The project for the Cloud Spanner instance. Use default '
                    'project if unset.')


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

  def __init__(self, name, description, database, ddl):
    super(GcpSpannerInstance, self).__init__()
    self._name = name
    self._description = description
    self._database = database
    self._ddl = ddl

    self._config = FLAGS.cloud_spanner_config
    self._nodes = FLAGS.cloud_spanner_nodes

    self._end_point = None

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = FLAGS.cloud_spanner_project or util.GetDefaultProject()
    self.zone = None

  def _Create(self):
    """Creates the instance, the database, and update the schema."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'create', self._name)
    cmd.flags['description'] = self._description
    cmd.flags['nodes'] = self._nodes
    cmd.flags['config'] = self._config
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP Spanner instance failed.')
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'create',
                             self._database)
    cmd.flags['instance'] = self._name
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP Spanner database failed.')
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'ddl', 'update',
                             self._database)
    cmd.flags['instance'] = self._name
    cmd.flags['ddl'] = self._ddl
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Update GCP Spanner database schema failed.')
    else:
      logging.info('Created GCP Spanner instance and database.')

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'delete',
                             self._name)
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Delete GCP Spanner instance failed.')
    else:
      logging.info('Deleted GCP Spanner instance.')

  def _Exists(self, instance_only=False):
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self._name)

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      logging.info('Could not found GCP Spanner instances %s.' % self._name)
      return False

    if instance_only:
      return True

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'describe',
                             self._database)
    cmd.flags['instance'] = self._name

    # Do not log error or warning when checking existence.
    _, _, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      logging.info('Could not found GCP Spanner database %s.' % self._database)
      return False

    return True

  def GetEndPoint(self):
    """Returns the end point for Cloud Spanner."""
    if self._end_point:
      return self._end_point

    cmd = util.GcloudCommand(self, 'config', 'get-value',
                             'api_endpoint_overrides/spanner')
    stdout, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.warning('Fail to retrieve cloud spanner end point.')
      return None
    self._end_point = json.loads(stdout)
    return self._end_point
