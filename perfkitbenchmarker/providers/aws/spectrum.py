# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS's Spectrum EDW service.

Clusters can be created (based on new configuration or restored from a snapshot)
and deleted.
"""

import json
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import redshift
from perfkitbenchmarker.providers.aws import util


FLAGS = flags.FLAGS


READY_STATUSES = ['available']
SNAPSHOT_READY_STATUSES = ['completed']


def AddTags(resource_arn, region):
  """Adds tags to a Redshift cluster created by PerfKitBenchmarker.

  Args:
    resource_arn: The arn of AWS resource to operate on.
    region: The AWS region resource was created in.
  """
  cmd_prefix = util.AWS_PREFIX
  tag_cmd = cmd_prefix + ['redshift', 'create-tags', '--region=%s' % region,
                          '--resource-name', resource_arn, '--tags']
  tag_cmd += util.MakeFormattedDefaultTags()
  vm_util.IssueCommand(tag_cmd)


class AddingIAMRole(object):
  """IAM Role to associate with the cluster.

  IAM Role can be associated with the cluster to access to other services such
  as S3.

  Attributes:
    cluster_identifier: Identifier of the cluster
    iam_role_name: Role name of the IAM
  """

  def __init__(self, cluster_identifier, iam_role_name, cmd_prefix):
    self.cmd_prefix = cmd_prefix
    self.cluster_identifier = cluster_identifier
    self.iam_role_name = iam_role_name
    cmd = self.cmd_prefix + ['redshift',
                             'modify-cluster-iam-roles',
                             '--cluster-identifier',
                             self.cluster_identifier,
                             '--add-iam-roles',
                             self.iam_role_name]
    vm_util.IssueCommand(cmd)


class Spectrum(redshift.Redshift):
  """Object representing a Spectrum cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project.
  """

  SERVICE_TYPE = 'spectrum'

  def __init__(self, edw_service_spec):
    super(Spectrum, self).__init__(edw_service_spec)
    # Cluster setup attributes
    self.iam_role = edw_service_spec.iam_role

  def _IsReady(self):
    """Method to return if the cluster is ready to handle queries."""
    return self._IsClusterReady() and self._IsSnapshotRestored()

  def _IsClusterReady(self):
    """Method to return if the cluster is ready."""
    stdout, _, _ = self.__DescribeCluster()
    return json.loads(stdout)['Clusters'][0]['ClusterStatus'] in READY_STATUSES

  def __DescribeCluster(self):
    """Describe a spectrum cluster."""
    cmd = self.cmd_prefix + ['redshift', 'describe-clusters',
                             '--cluster-identifier', self.cluster_identifier]
    return vm_util.IssueCommand(cmd)

  def _IsSnapshotRestored(self):
    """Method to return if the cluster snapshot is completed restoring."""
    stdout, _, _, = self.__DescribeCluster()
    return (json.loads(stdout)['Clusters'][0]['RestoreStatus']['Status'] in
            SNAPSHOT_READY_STATUSES)

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    Get the endpoint to be used for interacting with the cluster and apply
    tags on the cluster.
    """
    @vm_util.Retry(poll_interval=self.POLL_INTERVAL, fuzz=0,
                   timeout=self.READY_TIMEOUT,
                   retryable_exceptions=(
                       errors.Resource.RetryableCreationError,))
    def WaitUntilReady():
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Adding IAM Role')

    stdout, _, _ = self.__DescribeCluster()
    self.adding_iam_role = None
    if self.iam_role is not None:
      self.adding_iam_role = AddingIAMRole(self.cluster_identifier,
                                           self.iam_role,
                                           self.cmd_prefix)
      WaitUntilReady()

    stdout, _, _ = self.__DescribeCluster()
    self.endpoint = json.loads(stdout)['Clusters'][0]['Endpoint']['Address']
    account = util.GetAccount()
    self.arn = 'arn:aws:redshift:{}:{}:cluster:{}'.format(self.region, account,
                                                          self.
                                                          cluster_identifier)
    AddTags(self.arn, self.region)
