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

import copy
import json
import os
from typing import Dict, List, Text, Tuple

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import data
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
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
DEFAULT_DATABASE_NAME = 'dev'
BOOTSTRAP_DB = 'sample'
REDSHIFT_JDBC_JAR = 'redshift-jdbc-client-1.0.jar'


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


def GetRedshiftClientInterface(database: str, user: str,
                               password: str) -> edw_service.EdwClientInterface:
  """Builds and Returns the requested Redshift client Interface.

  Args:
    database: Name of the database to run queries against.
    user: Redshift username for authentication.
    password: Redshift password for authentication.

  Returns:
    A concrete Client Interface object.

  Raises:
    RuntimeError: if an unsupported redshift_client_interface is requested
  """
  if FLAGS.redshift_client_interface == 'CLI':
    return CliClientInterface(database, user, password)
  if FLAGS.redshift_client_interface == 'JDBC':
    return JdbcClientInterface(database, user, password)
  raise RuntimeError('Unknown Redshift Client Interface requested.')


class CliClientInterface(edw_service.EdwClientInterface):
  """Command Line Client Interface class for Redshift.

  Uses the native Redshift client that ships with pgbench.
  https://docs.aws.amazon.com/cli/latest/reference/redshift/index.html

  Attributes:
    host: Host endpoint to be used for interacting with the cluster.
    database: Name of the database to run queries against.
    user: Redshift username for authentication.
    password: Redshift password for authentication.
  """

  def __init__(self, database: str, user: str, password: str):
    self.database = database
    self.user = user
    self.password = password

    # set by SetProvisionedAttributes()
    self.host = None

  def SetProvisionedAttributes(self, bm_spec: benchmark_spec.BenchmarkSpec):
    """Sets any attributes that were unknown during initialization."""
    super(CliClientInterface, self).SetProvisionedAttributes(bm_spec)
    self.host = bm_spec.edw_service.endpoint

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the redshift tool dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand('sudo pip install absl-py')
    self.client_vm.Install('pgbench')

    # Push the framework to execute a sql query and gather performance details
    service_specific_dir = os.path.join('edw', Redshift.SERVICE_TYPE)
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(service_specific_dir, 'script_runner.sh')))
    runner_permission_update_cmd = 'chmod 755 {}'.format('script_runner.sh')
    self.client_vm.RemoteCommand(runner_permission_update_cmd)
    self.client_vm.PushFile(
        data.ResourcePath(os.path.join('edw', 'script_driver.py')))
    self.client_vm.PushFile(
        data.ResourcePath(
            os.path.join(service_specific_dir,
                         'provider_specific_script_driver.py')))

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = ('python script_driver.py --script={} --host={} '
                     '--database={} --user={} --password={}').format(
                         query_name, self.host, self.database, self.user,
                         self.password)
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    performance = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    details['job_id'] = performance[query_name]['job_id']
    return float(performance[query_name]['execution_time']), details

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.redshift_client_interface}


class JdbcClientInterface(edw_service.EdwClientInterface):
  """Native JDBC Client Interface class for Redshift.

  https://docs.aws.amazon.com/redshift/latest/mgmt/jdbc20-install.html

  Attributes:
    host: Host endpoint to be used for interacting with the cluster.
    database: Name of the database to run queries against.
    user: Redshift username for authentication.
    password: Redshift password for authentication.
  """

  def __init__(self, database: str, user: str, password: str):
    self.database = database
    # Use the default port.
    self.port = '5439'
    self.user = user
    self.password = password

    # set in SetProvisionedAttributes()
    self.host = None

  def SetProvisionedAttributes(self, bm_spec: benchmark_spec.BenchmarkSpec):
    """Sets any attributes that were unknown during initialization."""
    super(JdbcClientInterface, self).SetProvisionedAttributes(bm_spec)
    endpoint = bm_spec.edw_service.endpoint
    self.host = f'jdbc:redshift://{endpoint}:{self.port}/{self.database}'

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query.

    Installs the redshift tool dependencies.

    Args:
      package_name: String name of the package defining the preprovisioned data
        (certificates, etc.) to extract and use during client vm preparation.
    """
    self.client_vm.Install('openjdk')

    # Push the executable jar to the working directory on client vm
    self.client_vm.InstallPreprovisionedPackageData(package_name,
                                                    [REDSHIFT_JDBC_JAR], '')

  def ExecuteQuery(self, query_name: Text) -> Tuple[float, Dict[str, str]]:
    """Executes a query and returns performance details.

    Args:
      query_name: String name of the query to execute.

    Returns:
      A tuple of (execution_time, execution details)
      execution_time: A Float variable set to the query's completion time in
        secs. -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      performance_details: A dictionary of query execution attributes eg. job_id
    """
    query_command = ('java -cp {} com.google.cloud.performance.edw.Single '
                     '--endpoint {} --query_file {}').format(
                         REDSHIFT_JDBC_JAR, self.host, query_name)
    stdout, _ = self.client_vm.RemoteCommand(query_command)
    performance = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    if 'failure_reason' in performance:
      details.update({'failure_reason': performance['failure_reason']})
    else:
      details.update(performance['details'])
    return performance['query_wall_time_in_secs'], details

  def ExecuteSimultaneous(self, submission_interval: int,
                          queries: List[str]) -> str:
    """Executes queries simultaneously on client and return performance details.

    Simultaneous app expects queries as white space separated query file names.

    Args:
      submission_interval: Simultaneous query submission interval in
        milliseconds.
      queries: List of strings (names) of queries to execute.

    Returns:
      A serialized dictionary of execution details.
    """
    cmd = ('java -cp {} com.google.cloud.performance.edw.Simultaneous '
           '--endpoint {} --submission_interval {} --query_files {}'.format(
               REDSHIFT_JDBC_JAR, self.host, submission_interval,
               ' '.join(queries)))
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout

  def ExecuteThroughput(self, concurrency_streams: List[List[str]]) -> str:
    """Executes a throughput test and returns performance details.

    Args:
      concurrency_streams: List of streams to execute simultaneously, each of
        which is a list of string names of queries.

    Returns:
      A serialized dictionary of execution details.
    """
    cmd = ('java -cp {} com.google.cloud.performance.edw.Throughput '
           '--endpoint {} --query_streams {}'.format(
               REDSHIFT_JDBC_JAR, self.host,
               ' '.join([','.join(stream) for stream in concurrency_streams])))
    stdout, _ = self.client_vm.RemoteCommand(cmd)
    return stdout

  def GetMetadata(self) -> Dict[str, str]:
    """Gets the Metadata attributes for the Client Interface."""
    return {'client': FLAGS.redshift_client_interface}


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
        self.cmd_prefix)

    if self.db is None:
      self.db = DEFAULT_DATABASE_NAME
    self.client_interface = GetRedshiftClientInterface(self.db, self.user,
                                                       self.password)

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
      raise errors.Config.MissingOption('Need cluster_identifier, user and '
                                        'password set for creating a cluster.')

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
    stdout, stderr, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
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
      raise errors.Config.MissingOption(
          'Need the db, user and password set for restoring a cluster')

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
                               '--automated-snapshot-retention-period=1']
      stdout, stderr, _ = vm_util.IssueCommand(cmd)
      if not stdout:
        raise errors.Resource.CreationError('Cluster creation failure: '
                                            '{}'.format(stderr))

  def __DescribeCluster(self):
    """Describe a redshift cluster."""
    cmd = self.cmd_prefix + ['redshift', 'describe-clusters',
                             '--cluster-identifier', self.cluster_identifier]
    return vm_util.IssueCommand(cmd, raise_on_failure=False)

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
    vm_util.IssueCommand(cmd, raise_on_failure=False)

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
    basic_data['region'] = self.region
    if self.snapshot is not None:
      basic_data['snapshot'] = self.snapshot
    basic_data.update(self.client_interface.GetMetadata())
    return basic_data
