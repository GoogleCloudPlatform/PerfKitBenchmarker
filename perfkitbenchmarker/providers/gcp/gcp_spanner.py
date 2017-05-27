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

FLAGS = flags.FLAGS


class GcpSpannerInstance(resource.BaseResource):
  """Object representing a GCP Spanner Instance.

  Attributes:
    name: Instance name.
    desp: Description of the instance.
    nodes: Number of nodes in the instance.
    config: config the instance.
  """

  def __init__(self, name, desp, nodes, config, database, ddl):
    super(GcpSpannerInstance, self).__init__()
    self._nodes = nodes
    self._name = name
    self._desp = desp
    self._config = config
    self._database = database
    self._ddl = ddl

    # Cloud Spanner is not using the following common flags.
    self.project = None
    self.zone = None

  def _Create(self):
    """Creates the instance, the database, and update the schema."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'create', self._name)
    cmd.flags['description'] = self._desp
    cmd.flags['nodes'] = self._nodes
    cmd.flags['config'] = self._config
    stdout, stderr, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP Spanner instance failed. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'create',
                             self._database)
    cmd.flags['instance'] = self._name
    stdout, stderr, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Create GCP Spanner database failed. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'ddl', 'update',
                             self._database)
    cmd.flags['instance'] = self._name
    cmd.flags['ddl'] = self._ddl
    stdout, stderr, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Update GCP Spanner database schema failed. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'delete',
                             self._name)
    stdout, stderr, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Delete GCP Spanner instances failed. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)

  def _Exists(self):
    """Returns true if the instance and the database exists."""
    cmd = util.GcloudCommand(self, 'spanner', 'instances', 'list')
    cmd.flags['filter'] = self._name
    stdout, stderr, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      # This is not ideal, as we're returning false not because we know
      # the instance isn't there, but because we can't figure out whether
      # it is there.  This behavior is consistent without other
      # _Exists methods.
      logging.error('Unable to list GCP Spanner instances. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return False
    result = json.loads(stdout)
    instances = {instance['name'].split('/')[-1] for instance in result}
    if self._name not in instances:
      return False

    cmd = util.GcloudCommand(self, 'spanner', 'databases', 'list')
    cmd.flags['filter'] = self._database
    cmd.flags['instance'] = self._name
    stdout, stderr, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      # This is not ideal, as we're returning false not because we know
      # the database isn't there, but because we can't figure out whether
      # it is there.  This behavior is consistent without other
      # _Exists methods.
      logging.error('Unable to list GCP Spanner databases. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return False
    result = json.loads(stdout)
    databases = {database['name'].split('/')[-1] for database in result}
    if self._database not in databases:
      return False

    return True
