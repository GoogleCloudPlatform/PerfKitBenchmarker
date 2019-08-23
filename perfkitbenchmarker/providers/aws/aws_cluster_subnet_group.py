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
"""Module containing class for AWS's Redshift Cluster Subnet Group."""

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


class RedshiftClusterSubnetGroup(resource.BaseResource):
  """Cluster Subnet Group associated with a Redshift cluster launched in a vpc.

  A cluster subnet group allows you to specify a set of subnets in your VPC.


  Attributes:
    name: A string name of the cluster subnet group.
    subnet_id: A string name of the subnet id associated with the group.
  """

  def __init__(self, cmd_prefix):
    super(RedshiftClusterSubnetGroup, self).__init__(user_managed=False)
    self.cmd_prefix = cmd_prefix
    self.name = 'pkb-' + FLAGS.run_uri
    self.subnet_id = ''

  def _Create(self):
    cmd = self.cmd_prefix + [
        'redshift', 'create-cluster-subnet-group',
        '--cluster-subnet-group-name', self.name, '--description',
        'Cluster Subnet Group for run uri {}'.format(
            FLAGS.run_uri), '--subnet-ids', self.subnet_id
    ]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Delete a redshift cluster subnet group."""
    cmd = self.cmd_prefix + [
        'redshift', 'delete-cluster-subnet-group',
        '--cluster-subnet-group-name', self.name
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)
