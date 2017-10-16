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
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network

FLAGS = flags.FLAGS


DEFAULT_MYSQL_VERSION = '5.7.11'
DEFAULT_POSTGRES_VERSION = '9.6.2'

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (RDS HA takes a long time to prepare)


class AwsManagedRelationalDbCrossRegionException(Exception):
  pass


class AwsManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
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
  In other words, the 'zone' attribute on the managed_relational_db vm_spec
  has no effect, and is only used to specify the region.

  To filter out runs that cross zones, be sure to check the sample metadata for
  'zone' (client's zone), 'managed_relational_db_zone' (primary RDS zone),
  and 'managed_relational_db_secondary_zone' (secondary RDS zone).

  If the instance was NOT launched in the high availability configuration, the
  server will be launched in the zone requested, and
  managed_relational_db_secondary_zone will not exist in the metadata.
  """
  CLOUD = providers.AWS

  def __init__(self, managed_relational_db_spec):
    super(AwsManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri
    self.zone = self.spec.vm_spec.zone
    self.region = util.GetRegionFromZone(self.zone)

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with managed_relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of AWS Managed DB metadata.
    """
    metadata = super(AwsManagedRelationalDb, self).GetResourceMetadata()
    metadata.update({
        'zone': self.primary_zone,
    })

    if self.spec.high_availability:
      metadata.update({
          'secondary_zone': self.secondary_zone,
      })

    if hasattr(self.spec.disk_spec, 'iops'):
      metadata.update({
          'disk_iops': self.spec.disk_spec.iops,
      })

    return metadata

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).
    Returns:
      (string): Default engine version.
    """
    if engine == managed_relational_db.MYSQL:
      return DEFAULT_MYSQL_VERSION
    elif engine == managed_relational_db.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif engine == managed_relational_db.AURORA_POSTGRES:
      return DEFAULT_POSTGRES_VERSION

  def _GetNewZones(self):
    """Returns a list of zones, excluding the one that the client VM is in."""
    zone = self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    get_zones_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-availability-zones',
        '--region={0}'.format(region)
    ]
    stdout, _, _ = vm_util.IssueCommand(get_zones_cmd)
    response = json.loads(stdout)
    all_zones = [item['ZoneName'] for item in response['AvailabilityZones']
                 if item['State'] == 'available']
    all_zones.remove(zone)
    return all_zones

  def _CreateSubnetInAdditionalZone(self):
    """Creates a new subnet in the same region as the client VM.

    The zone will be different from the client's zone (but in the same region).

    Returns:
      the new subnet resource

    Raises:
      Exception if unable to create a subnet in any zones in the region.
    """
    new_subnet_zones = self._GetNewZones()
    while len(new_subnet_zones) >= 1:
      try:
        new_subnet_zone = new_subnet_zones.pop()
        logging.info('Attempting to create a second subnet in zone %s',
                     new_subnet_zone)
        new_subnet = (
            aws_network.AwsSubnet(
                new_subnet_zone,
                self.client_vm.network.regional_network.vpc.id,
                '10.0.1.0/24'))
        new_subnet.Create()
        logging.info('Successfully created a new subnet, subnet id is: %s',
                     new_subnet.id)

        # save for cleanup
        self.extra_subnet_for_db = new_subnet
        return new_subnet
      except:
        logging.info('Unable to create subnet in zone %s', new_subnet_zone)
    raise Exception('Unable to create subnet in any availability zones')

  def _CreateDbSubnetGroup(self, new_subnet):
    """Creates a new db subnet group.

    The db subnet group will consit of two zones: the client vm zone,
    and another zone in the same region.

    Args:
      new_subnet: a subnet in the same region as the client VM's subnet
    """
    zone = self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    db_subnet_group_name = 'pkb-db-subnet-group-{0}'.format(FLAGS.run_uri)
    create_db_subnet_group_cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-subnet-group',
        '--db-subnet-group-name', db_subnet_group_name,
        '--db-subnet-group-description', 'pkb_subnet_group_for_db',
        '--subnet-ids', self.client_vm.network.subnet.id, new_subnet.id,
        '--region', region]
    stdout, stderr, _ = vm_util.IssueCommand(create_db_subnet_group_cmd)

    # save for cleanup
    self.db_subnet_group_name = db_subnet_group_name
    self.security_group_id = (self.client_vm.network.regional_network.
                              vpc.default_security_group_id)

  def _SetupNetworking(self):
    """Sets up the networking required for the RDS database."""
    zone = self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    new_subnet = self._CreateSubnetInAdditionalZone()
    self._CreateDbSubnetGroup(new_subnet)

    open_port_cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id', self.security_group_id,
        '--source-group', self.security_group_id,
        '--protocol', 'tcp',
        '--port={0}'.format(DEFAULT_POSTGRES_PORT),
        '--region', region]
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info('Granted DB port ingress, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)

  def _TeardownNetworking(self):
    """Tears down all network resources that were created for the database."""
    zone = self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    if hasattr(self, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name', self.db_subnet_group_name,
          '--region', region]
      stdout, stderr, _ = vm_util.IssueCommand(delete_db_subnet_group_cmd)

    if hasattr(self, 'extra_subnet_for_db'):
      self.extra_subnet_for_db.Delete()

  def _Create(self):
    """Creates the AWS RDS instance."""
    cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-instance',
        '--db-instance-identifier=%s' % self.instance_id,
        '--engine=%s' % self.spec.engine,
        '--master-username=%s' % self.spec.database_username,
        '--master-user-password=%s' % self.spec.database_password,
        '--allocated-storage=%s' % self.spec.disk_spec.disk_size,
        '--storage-type=%s' % self.spec.disk_spec.disk_type,
        '--db-instance-class=%s' % self.spec.vm_spec.machine_type,
        '--no-auto-minor-version-upgrade',
        '--region=%s' % self.region,
        '--engine-version=%s' % self.spec.engine_version,
        '--db-subnet-group-name=%s' % self.db_subnet_group_name,
        '--vpc-security-group-ids=%s' % self.security_group_id,
    ]

    if self.spec.disk_spec.disk_type == aws_disk.IO1:
      cmd.append('--iops=%s' % self.spec.disk_spec.iops)

    if self.spec.high_availability:
      cmd.append('--multi-az')
    else:
      cmd.append('--availability-zone=%s' % self.spec.vm_spec.zone)

    # TODO(ferneyhough): add backup_enabled and backup_window

    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-instance',
        '--db-instance-identifier=%s' % self.instance_id,
        '--skip-final-snapshot',
        '--region', self.region,
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % self.instance_id,
        '--region=%s' % self.region
    ]
    _, _, retcode = vm_util.IssueCommand(cmd)
    return retcode == 0

  def _ParseEndpoint(self, describe_instance_json):
    """Parses the json output from the CLI and returns the endpoint.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'

    Returns:
      endpoint of the server as a string
    """
    return describe_instance_json['DBInstances'][0]['Endpoint']['Address']

  def _ParsePort(self, describe_instance_json):
    """Parses the json output from the CLI and returns the port.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'

    Returns:
      port on which the server is listening, as an int
    """
    return int(describe_instance_json['DBInstances'][0]['Endpoint']['Port'])

  def _SavePrimaryAndSecondaryZones(self, describe_instance_json):
    """Saves the primary, and secondary (only if HA) zone of the server.

    Args:
      describe_instance_json: output in json format from calling
        'aws rds describe-db-instances'
    """
    self.primary_zone = (
        describe_instance_json['DBInstances'][0]['AvailabilityZone'])
    if self.spec.high_availability:
      self.secondary_zone = (describe_instance_json['DBInstances'][0]
                             ['SecondaryAvailabilityZone'])

  def _IsReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the underlying resource is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'available', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occured.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % self.instance_id,
        '--region=%s' % self.region
    ]
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.exception('Timeout waiting for sql instance to be ready')
        return False
      stdout, _, _ = vm_util.IssueCommand(cmd, suppress_warning=True)

      try:
        json_output = json.loads(stdout)
        state = json_output['DBInstances'][0]['DBInstanceStatus']
        logging.info('Instance state: {0}'.format(state))
        if state == 'available':
          self._SavePrimaryAndSecondaryZones(json_output)
          break
      except:
        logging.exception('Error attempting to read stdout. Creation failure.')
        return False
      time.sleep(5)

    self.endpoint = self._ParseEndpoint(json_output)
    self.port = self._ParsePort(json_output)
    return True

  def _AssertClientAndDbInSameRegion(self):
    """Asserts that the client vm is in the same region requested by the server.

    Raises:
      AwsManagedRelationalDbCrossRegionException if the client vm is in a
        different region that is requested by the server.
    """
    client_region = self.client_vm.region
    db_region = util.GetRegionFromZone(self.zone)
    if client_region != db_region:
      raise AwsManagedRelationalDbCrossRegionException((
          'client_vm and managed_relational_db server '
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
