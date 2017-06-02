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

import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


DEFAULT_CLOUD_SPANNER_HOST = 'https://spanner.googleapis.com/'


class GcpSpannerInstance(resource.BaseResource):
  """Object representing a GCP Spanner Instance.

  Attributes:
    project: The gcloud project. If None, use default project.
    name: Instance name.
    description: Description of the instance.
    nodes: Number of nodes in the instance.
    config: Config of the instance.
    database: The name of the database.
    ddl: The schema of the database.
    host: The host of the database. If None, use default host.
  """

  def __init__(self, project, name, description, nodes, config, database, ddl,
               host):
    super(GcpSpannerInstance, self).__init__()
    self._nodes = nodes
    self._name = name
    self._description = description
    self._config = config
    self._database = database
    self._ddl = ddl
    self._host = host

    # Cloud Spanner may not explicitly set the following common flags.
    self.project = project if project is not None else util.GetDefaultProject()
    self.zone = None

    self._OverrideEndPoint()

  def __del__(self):
    self._ResetEndPoint()

  def _OverrideEndPoint(self):
    """Override Cloud Spanner end point if self._host is not None."""
    if self._host is not None:
      cmd = util.GcloudCommand(self, 'config', 'set',
                               'api_endpoint_overrides/spanner', self._host)
      _, _, retcode = cmd.Issue()
      if retcode != 0:
        logging.error('Override Cloud Spanner end point failed.')

  def _ResetEndPoint(self):
    """Reset Cloud Spanner end point."""
    if self._host is not None and self._host != DEFAULT_CLOUD_SPANNER_HOST:
      cmd = util.GcloudCommand(self, 'config', 'set',
                               'api_endpoint_overrides/spanner',
                               DEFAULT_CLOUD_SPANNER_HOST)
      _, _, retcode = cmd.Issue()
      if retcode != 0:
        logging.error('Reset Cloud Spanner end point failed.')

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

  def _Exists(self):
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'describe',
                             self._name)
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Could not found GCP Spanner instances %s.' % self._name)
      return False

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'describe',
                             self._database)
    cmd.flags['instance'] = self._name
    _, _, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Could not found GCP Spanner database %s.' % self._database)
      return False

    return True
