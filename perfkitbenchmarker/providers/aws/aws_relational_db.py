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
"""Managed relational database provisioning and teardown for AWS RDS."""


import datetime
import json
import logging
import time
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
FLAGS = flags.FLAGS


DEFAULT_MYSQL_VERSION = '5.7.16'
DEFAULT_POSTGRES_VERSION = '9.6.9'

DEFAULT_MYSQL_AURORA_VERSION = '5.7.12'
DEFAULT_MYSQL56_AURORA_VERSION = '5.6.10a'
DEFAULT_POSTGRES_AURORA_VERSION = '9.6.9'

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (RDS HA takes a long time to prepare)

_MAP_ENGINE_TO_DEFAULT_VERSION = {
    relational_db.MYSQL: DEFAULT_MYSQL_VERSION,
    relational_db.AURORA_MYSQL: DEFAULT_MYSQL_AURORA_VERSION,
    relational_db.AURORA_MYSQL56: DEFAULT_MYSQL56_AURORA_VERSION,
    relational_db.POSTGRES: DEFAULT_POSTGRES_VERSION,
    relational_db.AURORA_POSTGRES: DEFAULT_POSTGRES_AURORA_VERSION,
}

_AURORA_ENGINES = set([
    relational_db.AURORA_MYSQL56, relational_db.AURORA_MYSQL,
    relational_db.AURORA_POSTGRES
])

_RDS_ENGINES = set([relational_db.MYSQL, relational_db.POSTGRES])


class AwsRelationalDbCrossRegionException(Exception):
  pass


class AwsRelationalDb(relational_db.BaseRelationalDb):
  """An object representing an AWS RDS managed relational database.

  Currenty MySQL and Postgres are supported. This class requires that a
  client vm be available as an attribute on the instance before Create() is
  called, which is the current behavior of PKB. This is necessary to setup the
  networking correctly. The following steps are performed to provision the
  database:
    1. get the client's VPC
    2. get the client's zone
    3. create a new subnet in the VPC's region that is different from the
        client's zone
    4. create a new db subnet group using the client's zone, and the newly
        created zone
    5. authorize Postgres traffic on the VPC's default security group
    6. create the RDS instance in the requested region using the new db
        subnet group and VPC security group.

  On teardown, all resources are deleted.

  Note that the client VM's region and the region requested for the database
  must be the same.

  At the moment there is no way to specify the primary zone when creating a
  high availability instance, which means that the client and server may
  be launched in different zones, which hurts network performance.
  In other words, the 'zone' attribute on the relational_db db_spec
  has no effect, and is only used to specify the region.

  To filter out runs that cross zones, be sure to check the sample metadata for
  'zone' (client's zone), 'relational_db_zone' (primary RDS zone),
  and 'relational_db_secondary_zone' (secondary RDS zone).

  If the instance was NOT launched in the high availability configuration, the
  server will be launched in the zone requested, and
  relational_db_secondary_zone will not exist in the metadata.
  """
  CLOUD = providers.AWS

  def __init__(self, relational_db_spec):
    super(AwsRelationalDb, self).__init__(relational_db_spec)
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri
    self.cluster_id = None
    self.all_instance_ids = []
    self.primary_zone = None
    self.secondary_zone = None

    if hasattr(self.spec, 'zones') and self.spec.zones is not None:
      self.zones = self.spec.zones
    else:
      self.zones = [self.spec.db_spec.zone]

    self.region = util.GetRegionFromZones(self.zones)
    self.subnets_owned_by_db = []
    self.subnets_used_by_db = []

    self.unmanaged_db_exists = None if self.is_managed_db else False

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of AWS Managed DB metadata.
    """
    metadata = super(AwsRelationalDb, self).GetResourceMetadata()
    metadata.update({
        'zone': self.primary_zone,
    })

    if self.spec.high_availability:
      metadata.update({
          'secondary_zone': self.secondary_zone,
      })

    if hasattr(self.spec.db_disk_spec, 'iops'):
      metadata.update({
          'disk_iops': self.spec.db_disk_spec.iops,
      })

    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).
    Returns:
      (string): Default engine version.
    Raises:
      Exception: If unrecognized engine is specified.
    """
    if engine not in _MAP_ENGINE_TO_DEFAULT_VERSION:
      raise Exception('Unspecified default version for {0}'.format(engine))
    return _MAP_ENGINE_TO_DEFAULT_VERSION[engine]

  def _GetNewZones(self):
    """Returns a list of zones, excluding the one that the client VM is in."""
    all_zones = util.GetZonesInRegion(self.region)
    for zone in self.zones:
      all_zones.remove(zone)
    return all_zones

  def _CreateSubnetInZone(self, new_subnet_zone):
    """Creates a new subnet in the same region as the client VM.

    Args:
      new_subnet_zone: The zone for the subnet to be created.
                       Must be in the same region as the client

    Returns:
      the new subnet resource
    """
    cidr = self.client_vm.network.regional_network.vpc.NextSubnetCidrBlock()
    logging.info('Attempting to create a subnet in zone %s' % new_subnet_zone)
    new_subnet = (
        aws_network.AwsSubnet(
            new_subnet_zone,
            self.client_vm.network.regional_network.vpc.id,
            cidr))
    new_subnet.Create()
    logging.info('Successfully created a new subnet, subnet id is: %s',
                 new_subnet.id)

    # save for cleanup
    self.subnets_used_by_db.append(new_subnet)
    self.subnets_owned_by_db.append(new_subnet)
    return new_subnet

  def _CreateSubnetInAllZonesAssumeClientZoneExists(self):
    client_zone = self.client_vm.network.subnet.zone
    for zone in self.zones:
      if zone != client_zone:
        self._CreateSubnetInZone(zone)
      else:
        self.subnets_used_by_db.append(self.client_vm.network.subnet)

  def _CreateSubnetInAdditionalZone(self):
    """Creates a new subnet in the same region as the client VM.

    The zone will be different from the client's zone (but in the same region).

    Returns:
      the new subnet resource

    Raises:
      Exception: if unable to create a subnet in any zones in the region.
    """
    new_subnet_zones = self._GetNewZones()
    while len(new_subnet_zones) >= 1:
      try:
        new_subnet_zone = new_subnet_zones.pop()
        new_subnet = self._CreateSubnetInZone(new_subnet_zone)
        return new_subnet
      except:
        logging.info('Unable to create subnet in zone %s', new_subnet_zone)
    raise Exception('Unable to create subnet in any availability zones')

  def _CreateDbSubnetGroup(self, subnets):
    """Creates a new db subnet group.

    Args:
      subnets: a list of strings.
               The db subnet group will consit of all subnets in this list.
    """
    db_subnet_group_name = 'pkb-db-subnet-group-{0}'.format(FLAGS.run_uri)

    create_db_subnet_group_cmd = util.AWS_PREFIX + (
        ['rds',
         'create-db-subnet-group',
         '--db-subnet-group-name', db_subnet_group_name,
         '--db-subnet-group-description', 'pkb_subnet_group_for_db',
         '--region', self.region,
         '--subnet-ids'] + [subnet.id for subnet in subnets] +
        ['--tags'] + util.MakeFormattedDefaultTags())

    vm_util.IssueCommand(create_db_subnet_group_cmd)

    # save for cleanup
    self.db_subnet_group_name = db_subnet_group_name
    self.security_group_id = (self.client_vm.network.regional_network.
                              vpc.default_security_group_id)

  def _SetupNetworking(self):
    """Sets up the networking required for the RDS database."""
    if self.spec.engine in _RDS_ENGINES:
      self.subnets_used_by_db.append(self.client_vm.network.subnet)
      self._CreateSubnetInAdditionalZone()
    elif self.spec.engine in _AURORA_ENGINES:
      self._CreateSubnetInAllZonesAssumeClientZoneExists()
    else:
      raise Exception('Unknown how to create network for {0}'.format(
          self.spec.engine))

    self._CreateDbSubnetGroup(self.subnets_used_by_db)

    open_port_cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id', self.security_group_id,
        '--source-group', self.security_group_id,
        '--protocol', 'tcp',
        '--port={0}'.format(DEFAULT_POSTGRES_PORT),
        '--region', self.region]
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info('Granted DB port ingress, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)

  def _TeardownNetworking(self):
    """Tears down all network resources that were created for the database."""
    if hasattr(self, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name', self.db_subnet_group_name,
          '--region', self.region]
      vm_util.IssueCommand(delete_db_subnet_group_cmd, raise_on_failure=False)

    for subnet_for_db in self.subnets_owned_by_db:
      subnet_for_db.Delete()

  def _CreateAwsSqlInstance(self):
    if self.spec.engine in _RDS_ENGINES:
      instance_identifier = self.instance_id
      self.all_instance_ids.append(instance_identifier)
      cmd = util.AWS_PREFIX + [
          'rds', 'create-db-instance',
          '--db-instance-identifier=%s' % instance_identifier,
          '--engine=%s' % self.spec.engine,
          '--master-username=%s' % self.spec.database_username,
          '--master-user-password=%s' % self.spec.database_password,
          '--allocated-storage=%s' % self.spec.db_disk_spec.disk_size,
          '--storage-type=%s' % self.spec.db_disk_spec.disk_type,
          '--db-instance-class=%s' % self.spec.db_spec.machine_type,
          '--no-auto-minor-version-upgrade',
          '--region=%s' % self.region,
          '--engine-version=%s' % self.spec.engine_version,
          '--db-subnet-group-name=%s' % self.db_subnet_group_name,
          '--vpc-security-group-ids=%s' % self.security_group_id,
          '--availability-zone=%s' % self.spec.db_spec.zone, '--tags'
      ] + util.MakeFormattedDefaultTags()

      if self.spec.db_disk_spec.disk_type == aws_disk.IO1:
        cmd.append('--iops=%s' % self.spec.db_disk_spec.iops)
      # TODO(ferneyhough): add backup_enabled and backup_window

      vm_util.IssueCommand(cmd)

    elif self.spec.engine in _AURORA_ENGINES:
      zones_needed_for_high_availability = len(self.zones) > 1
      if zones_needed_for_high_availability != self.spec.high_availability:
        raise Exception('When db_high_availability is true, multiple '
                        'zones must be specified.  When '
                        'db_high_availability is false, one zone '
                        'should be specified.   '
                        'db_high_availability: {0}  '
                        'zone count: {1} '.format(
                            zones_needed_for_high_availability,
                            len(self.zones)))

      cluster_identifier = 'pkb-db-cluster-' + FLAGS.run_uri
      # Create the cluster.
      cmd = util.AWS_PREFIX + [
          'rds', 'create-db-cluster',
          '--db-cluster-identifier=%s' % cluster_identifier,
          '--engine=%s' % self.spec.engine,
          '--engine-version=%s' % self.spec.engine_version,
          '--master-username=%s' % self.spec.database_username,
          '--master-user-password=%s' % self.spec.database_password,
          '--region=%s' % self.region,
          '--db-subnet-group-name=%s' % self.db_subnet_group_name,
          '--vpc-security-group-ids=%s' % self.security_group_id,
          '--availability-zones=%s' % self.spec.zones[0],
          '--tags'] + util.MakeFormattedDefaultTags()

      self.cluster_id = cluster_identifier
      vm_util.IssueCommand(cmd)

      for zone in self.zones:

        # The first instance is assumed to be writer -
        # and so use the instance_id  for that id.
        if zone == self.zones[0]:
          instance_identifier = self.instance_id
        else:
          instance_identifier = self.instance_id + '-' + zone

        self.all_instance_ids.append(instance_identifier)

        cmd = util.AWS_PREFIX + [
            'rds', 'create-db-instance',
            '--db-instance-identifier=%s' % instance_identifier,
            '--db-cluster-identifier=%s' % cluster_identifier,
            '--engine=%s' % self.spec.engine,
            '--engine-version=%s' % self.spec.engine_version,
            '--no-auto-minor-version-upgrade',
            '--db-instance-class=%s' % self.spec.db_spec.machine_type,
            '--region=%s' % self.region,
            '--availability-zone=%s' % zone, '--tags'
        ] + util.MakeFormattedDefaultTags()
        vm_util.IssueCommand(cmd)

    else:
      raise Exception('Unknown how to create AWS data base engine {0}'.format(
          self.spec.engine))

  def _Create(self):
    """Creates the AWS RDS instance.

    Raises:
      Exception: if unknown how to create self.spec.engine.

    """
    if self.spec.engine in [
        relational_db.AURORA_MYSQL56, relational_db.AURORA_MYSQL,
        relational_db.MYSQL
    ]:
      self._InstallMySQLClient()
    if self.is_managed_db:
      self._CreateAwsSqlInstance()
    else:
      self.endpoint = self.server_vm.ip_address
      if self.spec.engine == relational_db.MYSQL:
        self._InstallMySQLServer()
      else:
        raise Exception(
            'Engine {0} not supported for unmanaged databases.'.format(
                self.spec.engine))
      self.firewall = aws_network.AwsFirewall()
      self.firewall.AllowPortInSecurityGroup(
          self.server_vm.region,
          self.server_vm.network.regional_network.vpc.default_security_group_id,
          '3306', '3306', ['%s/32' % self.client_vm.ip_address])
      self.unmanaged_db_exists = True

  def _IsDeleting(self):
    """See Base class BaseResource in perfkitbenchmarker.resource.py."""

    for instance_id in self.all_instance_ids:
      json_output = self._DescribeInstance(instance_id)
      if json_output:
        state = json_output['DBInstances'][0]['DBInstanceStatus']
        if state == 'deleting':
          return True

    return False

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    if not self.is_managed_db:
      if hasattr(self, 'firewall'):
        self.firewall.DisallowAllPorts()
      self.unmanaged_db_exists = False
      return

    for current_instance_id in self.all_instance_ids:
      cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-instance',
          '--db-instance-identifier=%s' % current_instance_id,
          '--skip-final-snapshot',
          '--region', self.region,
      ]
      vm_util.IssueCommand(cmd, raise_on_failure=False)

    if self.cluster_id is not None:
      cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-cluster',
          '--db-cluster-identifier=%s' % self.cluster_id,
          '--skip-final-snapshot',
          '--region', self.region,
      ]
      vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    if not self.is_managed_db:
      return self.unmanaged_db_exists
    for current_instance_id in self.all_instance_ids:
      json_output = self._DescribeInstance(current_instance_id)
      if not json_output:
        return False

    return True

  def _ParseEndpointFromInstance(self, describe_instance_json):
    """Parses the json output from the CLI and returns the endpoint.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'

    Returns:
      endpoint of the server as a string
    """
    return describe_instance_json['DBInstances'][0]['Endpoint']['Address']

  def _ParsePortFromInstance(self, describe_instance_json):
    """Parses the json output from the CLI and returns the port.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'

    Returns:
      port on which the server is listening, as an int
    """
    if describe_instance_json is None:
      return None
    return int(describe_instance_json['DBInstances'][0]['Endpoint']['Port'])

  def _ParseEndpointFromCluster(self, describe_cluster_json):
    """Parses the json output from the CLI and returns the endpoint.

    Args:
      describe_cluster_json: output in json format from calling
        'aws rds describe-db-clusters'

    Returns:
      endpoint of the server as a string
    """
    return describe_cluster_json['DBClusters'][0]['Endpoint']

  def _ParsePortFromCluster(self, describe_cluster_json):
    """Parses the json output from the CLI and returns the port.

    Args:
      describe_cluster_json: output in json format from calling
        'aws rds describe-db-instances'

    Returns:
      port on which the server is listening, as an int
    """
    if describe_cluster_json is None:
      return None
    return int(describe_cluster_json['DBClusters'][0]['Port'])

  def _SavePrimaryAndSecondaryZones(self, describe_instance_json):
    """Saves the primary, and secondary (only if HA) zone of the server.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'
    """

    if self.spec.engine in _AURORA_ENGINES:
      self.primary_zone = self.zones[0]
      if len(self.zones) > 1:
        self.secondary_zone = ','.join(self.zones[1:])
    else:
      db_instance = describe_instance_json['DBInstances'][0]
      self.primary_zone = (
          db_instance['AvailabilityZone'])
      if self.spec.high_availability:
        if 'SecondaryAvailabilityZone' in db_instance:
          self.secondary_zone = db_instance['SecondaryAvailabilityZone']
        else:
          # the secondary DB for RDS is in the second subnet.
          self.secondary_zone = self.subnets_used_by_db[1].zone

  def _IsReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the underlying resource is ready.

    This method will query all of the instance every 5 seconds until
    its instance state is 'available', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """
    if not self.is_managed_db:
      return self._IsReadyUnmanaged()

    if not self.all_instance_ids:
      return False

    for instance_id in self.all_instance_ids:
      if not self._IsInstanceReady(instance_id, timeout):
        return False

    return True

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    Raises:
       Exception:  If could not ready the instance after modification to
                   multi-az.
    """
    self._ApplyMySqlFlags()

    if not self.is_managed_db:
      return

    need_ha_modification = self.spec.engine in _RDS_ENGINES

    if self.spec.high_availability and need_ha_modification:
      # When extending the database to be multi-az, the second region
      # is picked by where the second subnet has been created.
      cmd = util.AWS_PREFIX + [
          'rds',
          'modify-db-instance',
          '--db-instance-identifier=%s' % self.instance_id,
          '--multi-az',
          '--apply-immediately',
          '--region=%s' % self.region
      ]
      vm_util.IssueCommand(cmd)

      if not self._IsInstanceReady(self.instance_id, timeout=IS_READY_TIMEOUT):
        raise Exception('Instance could not be set to ready after '
                        'modification for high availability')

    json_output = self._DescribeInstance(self.instance_id)
    self._SavePrimaryAndSecondaryZones(json_output)
    if self.cluster_id:
      self._GetPortsForClusterInstance(self.cluster_id)
    else:
      self._GetPortsForWriterInstance(self.all_instance_ids[0])

  def _IsInstanceReady(self, instance_id, timeout=IS_READY_TIMEOUT):
    """Return true if the instance is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'available', or until a timeout occurs.

    Args:
      instance_id: string of the instance to check is ready
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.exception('Timeout waiting for sql instance to be ready')
        return False
      json_output = self._DescribeInstance(instance_id)
      if json_output:
        try:
          state = json_output['DBInstances'][0]['DBInstanceStatus']
          pending_values = (
              json_output['DBInstances'][0]['PendingModifiedValues'])
          logging.info('Instance state: %s', state)
          if pending_values:
            logging.info('Pending values: %s', (str(pending_values)))

          if state == 'available' and not pending_values:
            break
        except:
          logging.exception(
              'Error attempting to read stdout. Creation failure.')
          return False
      time.sleep(5)

    return True

  def _DescribeInstance(self, instance_id):
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % instance_id,
        '--region=%s' % self.region
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, suppress_warning=True,
                                              raise_on_failure=False)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

  def _DescribeCluster(self, cluster_id):
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-clusters',
        '--db-cluster-identifier=%s' % cluster_id,
        '--region=%s' % self.region
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd, suppress_warning=True)
    json_output = json.loads(stdout)
    return json_output

  def _GetPortsForWriterInstance(self, instance_id):
    """Assigns the ports and endpoints from the instance_id to self.

    These will be used to communicate with the data base.
    """
    json_output = self._DescribeInstance(instance_id)
    self.endpoint = self._ParseEndpointFromInstance(json_output)
    self.port = self._ParsePortFromInstance(json_output)

  def _GetPortsForClusterInstance(self, cluster_id):
    """Assigns the ports and endpoints from the cluster_id to self.

    These will be used to communicate with the data base.
    """
    json_output = self._DescribeCluster(cluster_id)
    self.endpoint = self._ParseEndpointFromCluster(json_output)
    self.port = self._ParsePortFromCluster(json_output)

  def _AssertClientAndDbInSameRegion(self):
    """Asserts that the client vm is in the same region requested by the server.

    Raises:
      AwsRelationalDbCrossRegionException: if the client vm is in a
        different region that is requested by the server.
    """
    if self.client_vm.region != self.region:
      raise AwsRelationalDbCrossRegionException(
          ('client_vm and relational_db server '
           'must be in the same region'))

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    self._AssertClientAndDbInSameRegion()
    self._SetupNetworking()

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    self._TeardownNetworking()

  def _FailoverHA(self):
    """Fail over from master to replica."""

    if self.spec.engine in _RDS_ENGINES:
      cmd = util.AWS_PREFIX + [
          'rds',
          'reboot-db-instance',
          '--db-instance-identifier=%s' % self.instance_id,
          '--force-failover',
          '--region=%s' % self.region
      ]
      vm_util.IssueCommand(cmd)
    elif self.spec.engine in _AURORA_ENGINES:
      new_primary_id = self.all_instance_ids[1]
      cmd = util.AWS_PREFIX + [
          'rds',
          'failover-db-cluster',
          '--db-cluster-identifier=%s' % self.cluster_id,
          '--target-db-instance-identifier=%s' % new_primary_id,
          '--region=%s' % self.region
      ]
      vm_util.IssueCommand(cmd)
    else:
      raise Exception('Unknown how to failover {0}'.format(
          self.spec.engine))
