# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's bigtable instances.

Clusters can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GcpBigtableInstance(resource.BaseResource):
  """Object representing a GCP Bigtable Instance.

  Attributes:
    name: Instance and cluster name.
    num_nodes: Number of nodes in the instance's cluster.
    project: Enclosing project for the instance.
    zone: zone of the instance's cluster.
  """

  def __init__(self, name, num_nodes, project, zone):
    super(GcpBigtableInstance, self).__init__()
    self.num_nodes = num_nodes
    self.name = name
    self.zone = zone
    self.project = project

  def _Create(self):
    """Creates the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'create',
                             self.name)
    cmd.flags['description'] = 'PkbCreatedCluster'
    cmd.flags['cluster'] = self.name
    cmd.flags['cluster-num-nodes'] = str(self.num_nodes)
    cmd.flags['cluster-zone'] = self.zone
    cmd.flags['project'] = self.project
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    cmd.Issue()

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'delete',
                             self.name)
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    cmd.Issue()

  def _Exists(self):
    """Returns true if the instance exists."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'list')
    cmd.flags['format'] = 'json'
    cmd.flags['project'] = self.project
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    stdout, stderr, retcode = cmd.Issue(suppress_warning=True)
    if retcode != 0:
      # This is not ideal, as we're returning false not because we know
      # the table isn't there, but because we can't figure out whether
      # it is there.  This behavior is consistent without other
      # _Exists methods.
      logging.error('Unable to list GCP Bigtable instances. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return False
    result = json.loads(stdout)
    instances = {instance['name'] for instance in result}
    full_name = 'projects/{}/instances/{}'.format(self.project, self.name)
    return full_name in instances
