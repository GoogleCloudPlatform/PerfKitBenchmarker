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
import util


FLAGS = flags.FLAGS


VALID_EXIST_STATUSES = ['creating', 'available']
DELETION_STATUSES = ['deleting']
READY_STATUSES = ['available']


def AddTags(resource_arn, region, **kwargs):
  """Adds tags to a Redshift cluster created by PerfKitBenchmarker.

  Args:
    resource_arn: The arn of AWS resource to operate on.
    region: The AWS region resource was created in.
    **kwargs: dict. Key-value pairs to set on the instance.
  """

  if not kwargs:
    return
  cmd_prefix = util.AWS_PREFIX
  tag_cmd = cmd_prefix + ['redshift', 'create-tags', '--region=%s' % region,
                          '--resource-name', resource_arn, '--tags']
  for key, value in kwargs.iteritems():
    tag_cmd.append('Key={0},Value={1}'.format(key, value))
  vm_util.IssueCommand(tag_cmd)


def GetDefaultRegion():
  """Utility method to supply the default region."""
  cmd_prefix = util.AWS_PREFIX
  default_region_cmd = cmd_prefix + ['configure', 'get', 'region']
  stdout, _, _ = vm_util.IssueCommand(default_region_cmd)
  return stdout


class RedshiftClusterSubnetGroup(object):
  """Cluster Subnet Group associated with a Redshift cluster launched in a vpc.

  A cluster subnet group allows you to specify a set of subnets in your VPC.


  Attributes:
    name: A string name of the cluster subnet group.
    subnet_id: A string name of the subnet id associated with the group.
  """

  def __init__(self, subnet_id, cmd_prefix):
    self.cmd_prefix = cmd_prefix
    self.name = 'pkb-' + FLAGS.run_uri
    self.subnet_id = subnet_id
    cmd = self.cmd_prefix + ['redshift',
                             'create-cluster-subnet-group',
                             '--cluster-subnet-group-name',
                             self.name,
                             '--description',
                             'Cluster Subnet Group for run uri {}'
                             .format(FLAGS.run_uri),
                             '--subnet-ids',
                             self.subnet_id]
    vm_util.IssueCommand(cmd)

  def Delete(self):
    """Delete a redshift cluster subnet group."""
    cmd = self.cmd_prefix + ['redshift',
                             'delete-cluster-subnet-group',
                             '--cluster-subnet-group-name',
                             self.name]
    vm_util.IssueCommand(cmd)


class RedshiftClusterParameterGroup(object):
  """Cluster Paramemter Group associated with a Redshift cluster.

  A cluster parameter group allows you to specify concurrency for the cluster.


  Attributes:
    name: A string name of the cluster parameter group.
    concurrency: An integer concurrency value for the cluster.
  """

  def __init__(self, concurrency, cmd_prefix):
    self.cmd_prefix = cmd_prefix
    self.name = 'pkb-' + FLAGS.run_uri
    cmd = self.cmd_prefix + ['redshift',
                             'create-cluster-parameter-group',
                             '--parameter-group-name',
                             self.name,
                             '--parameter-group-family',
                             'redshift-1.0',
                             '--description',
                             'Cluster Parameter group for run uri {}'
                             .format(FLAGS.run_uri)]
    vm_util.IssueCommand(cmd)
    wlm_concurrency_parameter_prefix = ('[{"ParameterName":"wlm_json_configurat'
                                        'ion","ParameterValue":"[{\\\"query_con'
                                        'currency\\\":')
    wlm_concurrency_parameter_postfix = '}]","ApplyType":"dynamic"}]'
    cmd = self.cmd_prefix + ['redshift',
                             'modify-cluster-parameter-group',
                             '--parameter-group-name',
                             self.name,
                             '--parameters',
                             '{}{}{}'.format(wlm_concurrency_parameter_prefix,
                                             str(concurrency),
                                             wlm_concurrency_parameter_postfix)]
    vm_util.IssueCommand(cmd)

  def Delete(self):
    """Delete a redshift cluster parameter group."""
    cmd = self.cmd_prefix + ['redshift',
                             'delete-cluster-parameter-group',
                             '--parameter-group-name',
                             self.name]
    vm_util.IssueCommand(cmd)


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

  def _Create(self):
    """Create the redshift cluster resource."""
    self.cluster_subnet_group = RedshiftClusterSubnetGroup(self.spec.subnet_id,
                                                           self.cmd_prefix)
    self.cluster_parameter_group = RedshiftClusterParameterGroup(
        self.concurrency, self.cmd_prefix)
    if self.snapshot:
      self.Restore(self.snapshot, self.cluster_identifier)
    else:
      # TODO(saksena@): Implmement the new Redshift cluster creation
      raise NotImplementedError()

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
    tags = {'owner': FLAGS.owner, 'perfkitbenchmarker-run': FLAGS.run_uri}
    AddTags(self.arn, self.region, **tags)

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
