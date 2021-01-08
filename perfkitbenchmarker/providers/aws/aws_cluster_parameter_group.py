# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's Redshift Cluster Parameter Group."""

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


class RedshiftClusterParameterGroup(resource.BaseResource):
  """Cluster Parameter Group associated with a Redshift cluster.

  Attributes:
    name: A string name of the cluster parameter group.
  """

  def __init__(self, cmd_prefix):
    super(RedshiftClusterParameterGroup, self).__init__(user_managed=False)
    self.cmd_prefix = cmd_prefix
    self.name = 'pkb-' + FLAGS.run_uri

  def _Create(self):
    cmd = self.cmd_prefix + [
        'redshift', 'create-cluster-parameter-group', '--parameter-group-name',
        self.name, '--parameter-group-family', 'redshift-1.0', '--description',
        'Cluster Parameter group for run uri {}'.format(FLAGS.run_uri)
    ]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Delete a redshift cluster parameter group."""
    cmd = self.cmd_prefix + [
        'redshift', 'delete-cluster-parameter-group', '--parameter-group-name',
        self.name
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)
