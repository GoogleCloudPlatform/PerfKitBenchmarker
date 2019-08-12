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
"""Module containing class for AWS's Redshift EDW service.

Clusters can be created (based on new configuration or restored from a snapshot)
and deleted.
"""

import json

from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_cluster_parameter_group
from perfkitbenchmarker.providers.aws import aws_cluster_subnet_group
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

VALID_EXIST_STATUSES = ['creating', 'available']
DELETION_STATUSES = ['deleting']
READY_STATUSES = ['available']
ELIMINATE_AUTOMATED_SNAPSHOT_RETENTION = '--automated-snapshot-retention-period=0'


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


def GetDefaultRegion():
  """Utility method to supply the default region."""
  cmd_prefix = util.AWS_PREFIX
  default_region_cmd = cmd_prefix + ['configure', 'get', 'region']
  stdout, _, _ = vm_util.IssueCommand(default_region_cmd)
  return stdout


class Redshift(edw_service.EdwService):
  """Object representing a Redshift cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project.
  """

  CLOUD = providers.AWS
  SERVICE_TYPE = 'redshift'

  READY_TIMEOUT = 7200

  def __init__(self, edw_service_spec):
    super(Redshift, self).__init__(edw_service_spec)
    # pkb setup attribute
    self.project = None
    self.cmd_prefix = list(util.AWS_PREFIX)
    if FLAGS.zones:
      self.zone = FLAGS.zones[0]
      self.region = util.GetRegionFromZone(self.zone)
    else:
      self.region = GetDefaultRegion()
    self.cmd_prefix += ['--region', self.region]

    # Redshift specific attribute (see if they can be set)
    self.cluster_subnet_group = None
    self.cluster_parameter_group = None
    self.arn = ''
    self.cluster_subnet_group = aws_cluster_subnet_group.RedshiftClusterSubnetGroup(
        self.cmd_prefix)
    self.cluster_parameter_group = aws_cluster_parameter_group.RedshiftClusterParameterGroup(
        edw_service_spec.concurrency, self.cmd_prefix)

  def _CreateDependencies(self):
    self.cluster_subnet_group.Create()
    self.cluster_parameter_group.Create()

  def _Create(self):
    """Create the redshift cluster resource."""
    if self.snapshot:
      self.Restore(self.snapshot, self.cluster_identifier)
    else:
      self.Initialize(self.cluster_identifier, self.node_type, self.node_count,
                      self.user, self.password, self.cluster_parameter_group,
                      self.cluster_subnet_group)

  def Initialize(self, cluster_identifier, node_type, node_count, user,
                 password, cluster_parameter_group, cluster_subnet_group):
    """Method to initialize a Redshift cluster from an configuration parameters.

    The cluster is initialized in the EC2-VPC platform, that runs it in a
    virtual private cloud (VPC). This allows control access to the cluster by
    associating one or more VPC security groups with the cluster.

    To create a cluster in a VPC, first create an Amazon Redshift cluster subnet
    group by providing subnet information of the VPC, and then provide the
    subnet group when launching the cluster.


    Args:
      cluster_identifier: A unique identifier for the cluster.
      node_type: The node type to be provisioned for the cluster.
       Valid Values: ds2.xlarge | ds2.8xlarge | ds2.xlarge | ds2.8xlarge |
         dc1.large | dc1.8xlarge | dc2.large | dc2.8xlarge
      node_count: The number of compute nodes in the cluster.
      user: The user name associated with the master user account for the
        cluster that is being created.
      password: The password associated with the master user account for the
        cluster that is being created.
      cluster_parameter_group: Cluster Parameter Group associated with the
        cluster.
      cluster_subnet_group: Cluster Subnet Group associated with the cluster.

    Returns:
      None


    Raises:
      MissingOption: If any of the required parameters is missing.
    """
    if not (cluster_identifier and node_type and user and password):
      raise errors.MissingOption('Need cluster_identifier, user and password '
                                 'set for creating a cluster.')

    prefix = [
        'redshift', 'create-cluster', '--cluster-identifier', cluster_identifier
    ]

    if node_count == 1:
      worker_count_cmd = ['--cluster-type', 'single-node']
    else:
      worker_count_cmd = ['--number-of-nodes', str(node_count)]

    postfix = [
        '--node-type', node_type, '--master-username', user,
        '--master-user-password', password, '--cluster-parameter-group-name',
        cluster_parameter_group.name, '--cluster-subnet-group-name',
        cluster_subnet_group.name, '--publicly-accessible',
        ELIMINATE_AUTOMATED_SNAPSHOT_RETENTION
    ]

    cmd = self.cmd_prefix + prefix + worker_count_cmd + postfix
    stdout, stderr, _ = vm_util.IssueCommand(cmd)
    if not stdout:
      raise errors.Resource.CreationError('Cluster creation failure: '
                                          '{}'.format(stderr))

  def _ValidateSnapshot(self, snapshot_identifier):
    """Validate the presence of a cluster snapshot based on its metadata."""
    cmd = self.cmd_prefix + ['redshift', 'describe-cluster-snapshots',
                             '--snapshot-identifier', snapshot_identifier]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    if not stdout:
      raise errors.Config.InvalidValue('Cluster snapshot indicated by '
                                       'edw_service_cluster_snapshot does not'
                                       ' exist: {}.'
                                       .format(snapshot_identifier))
    result = json.loads(stdout)
    return result['Snapshots'][0]['Status'] == 'available'

  def _SnapshotDetails(self, snapshot_identifier):
    """Delete a redshift cluster and disallow creation of a snapshot."""
    cmd = self.cmd_prefix + ['redshift', 'describe-cluster-snapshots',
                             '--snapshot-identifier', snapshot_identifier]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    node_type = result['Snapshots'][0]['NodeType']
    node_count = result['Snapshots'][0]['NumberOfNodes']
    return node_type, node_count

  def Restore(self, snapshot_identifier, cluster_identifier):
    """Method to restore a Redshift cluster from an existing snapshot.

    A snapshot of cluster in VPC can be restored only in VPC. Therefore, subnet
    group name where the cluster is to be restored must be provided.

    vpc-security-group-ids are not specified at the time of restoration, and it
    is expected that the default VPC security group which gets associated with
    the cluster has appropriate ingress and egress rules.

    Ref: http://docs.aws.amazon.com/cli/latest/reference/
    redshift/restore-from-cluster-snapshot.html

    Args:
      snapshot_identifier: Identifier of the snapshot to restore
      cluster_identifier:  Identifier of the restored cluster
    Returns:
      None
    """

    if not (self.user and self.password and self.db):
      raise errors.MissingOption('Need the db, user and password set for '
                                 'restoring a cluster')

    if self._ValidateSnapshot(snapshot_identifier):
      node_type, node_count = self._SnapshotDetails(snapshot_identifier)
      # For a restored cluster update the cluster shape and size based on the
      # snapshot's configuration
      self.node_type = node_type
      self.node_count = node_count
      cmd = self.cmd_prefix + ['redshift', 'restore-from-cluster-snapshot',
                               '--cluster-identifier', cluster_identifier,
                               '--snapshot-identifier', snapshot_identifier,
                               '--cluster-subnet-group-name',
                               self.cluster_subnet_group.name,
                               '--cluster-parameter-group-name',
                               self.cluster_parameter_group.name,
                               '--publicly-accessible',
                               '--automated-snapshot-retention-period=0']
      stdout, stderr, _ = vm_util.IssueCommand(cmd)
      if not stdout:
        raise errors.Resource.CreationError('Cluster creation failure: '
                                            '{}'.format(stderr))

  def __DescribeCluster(self):
    """Describe a redshift cluster."""
    cmd = self.cmd_prefix + ['redshift', 'describe-clusters',
                             '--cluster-identifier', self.cluster_identifier]
    return vm_util.IssueCommand(cmd)

  def _Exists(self):
    """Method to validate the existence of a redshift cluster.

    Provision pipeline: returns True during the provisioning (status in
    'creating', 'available') to prevent retry of creation

    Deletion pipeline: returns True, during the deletion (status in
    'deleting') which causes a retry of deletion, an idempotent operation.
    TODO(saksena): handle the deletion step more cleanly, and spin till deletion

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    stdout, _, _ = self.__DescribeCluster()
    if (not stdout or (json.loads(stdout)['Clusters'][0]['ClusterStatus'] not in
                       VALID_EXIST_STATUSES)):
      return False
    else:
      return True

  def _IsReady(self):
    """Method to return if the cluster is ready to handle queries."""
    stdout, _, _ = self.__DescribeCluster()
    return json.loads(stdout)['Clusters'][0]['ClusterStatus'] in READY_STATUSES

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    Get the endpoint to be used for interacting with the cluster and apply
    tags on the cluster.
    """
    stdout, _, _ = self.__DescribeCluster()
    self.endpoint = json.loads(stdout)['Clusters'][0]['Endpoint']['Address']
    account = util.GetAccount()
    self.arn = 'arn:aws:redshift:{}:{}:cluster:{}'.format(self.region, account,
                                                          self.
                                                          cluster_identifier)
    AddTags(self.arn, self.region)

  def _Delete(self):
    """Delete a redshift cluster and disallow creation of a snapshot."""
    cmd = self.cmd_prefix + ['redshift', 'delete-cluster',
                             '--cluster-identifier', self.cluster_identifier,
                             '--skip-final-cluster-snapshot']
    vm_util.IssueCommand(cmd)

  def _IsDeleting(self):
    """Method to check if the cluster is being deleting."""
    stdout, _, _ = self.__DescribeCluster()
    if not stdout:
      return False
    else:
      return (json.loads(stdout)['Clusters'][0]['ClusterStatus'] in
              DELETION_STATUSES)

  def _DeleteDependencies(self):
    """Delete dependencies of a redshift cluster."""
    self.cluster_subnet_group.Delete()
    self.cluster_parameter_group.Delete()

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = super(Redshift, self).GetMetadata()
    basic_data['edw_cluster_concurrency'] = self.concurrency
    basic_data['region'] = self.region
    if self.snapshot is not None:
      basic_data['snapshot'] = self.snapshot
    return basic_data

  def RunCommandHelper(self):
    """Redshift specific run script command components."""
    return '--host={} --database={} --user={} --password={}'.format(
        self.endpoint, self.db, self.user, self.password)

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of redshift runner.

    psql, a terminal-based front end from PostgreSQL, used as client
    https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-from-psql.html

    Args:
      vm: Client vm on which the script will be run.
    """
    vm.Install('pgbench')
