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

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


class GceBigtableCluster(resource.BaseResource):
  """Object representing a GCE Bigtable Cluster.

  Attributes:
    name: Cluster name.
    num_nodes: Number of nodes in the cluster.
    project: Enclosing project for the cluster.
    zone: zone of the cluster.
  """

  def __init__(self, name, num_nodes, project, zone):
    super(GceBigtableCluster, self).__init__()
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
    stdout, stderr, _ = cmd.Issue(suppress_warning=True)
    try:
      result = json.loads(stdout)
      clusters = {cluster['name']: cluster for cluster in result['clusters']}
      expected_cluster_name = 'projects/{0}/zones/{1}/clusters/{2}'.format(
          self.project, self.zone, self.name)
      try:
        clusters[expected_cluster_name]
        return True
      except KeyError:
        return False
    except ValueError:
      return False
    return True
