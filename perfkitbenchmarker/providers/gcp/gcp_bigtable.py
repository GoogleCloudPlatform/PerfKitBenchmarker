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
"""Module containing class for GCP's bigtable clusters.

Clusters can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GcpBigtableCluster(resource.BaseResource):
  """Object representing a GCP Bigtable Cluster.

  Attributes:
    name: Cluster name.
    num_nodes: Number of nodes in the cluster.
    project: Enclosing project for the cluster.
    zone: zone of the cluster.
  """

  def __init__(self, name, num_nodes, project, zone):
    super(GcpBigtableCluster, self).__init__()
    self.num_nodes = num_nodes
    self.name = name
    self.zone = zone
    self.project = project

  def _Create(self):
    """Creates the cluster."""
    cmd = util.GcloudCommand(self, 'alpha', 'bigtable', 'clusters', 'create',
                             self.name)
    cmd.flags['description'] = 'PkbCreatedCluster'
    cmd.flags['nodes'] = str(self.num_nodes)
    cmd.flags['zone'] = self.zone
    cmd.flags['project'] = self.project
    cmd.Issue()

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(self, 'alpha', 'bigtable', 'clusters', 'delete',
                             self.name)
    cmd.Issue()

  def _Exists(self):
    """Returns true if the cluster exists."""
    cmd = util.GcloudCommand(self, 'alpha', 'bigtable', 'clusters', 'list')
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
      logging.error('Unable to list GCP Bigtable clusters. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return False
    result = json.loads(stdout)
    clusters = {cluster['clusterId'] for cluster in result}
    return self.name in clusters
