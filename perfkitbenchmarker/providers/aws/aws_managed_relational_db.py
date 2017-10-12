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
DEFAULT_POSTGRES_PORT = 5432

class AwsManagedRelationalDb(managed_relational_db.BaseManagedRelationalDb):
  """An object representing an AWS managed relational database.

  Attributes:
    created: True if the resource has been created.
    pkb_managed: Whether the resource is managed (created and deleted) by PKB.
  """

  CLOUD = providers.AWS
  SERVICE_NAME = 'managed_relational_db'

  def __init__(self, managed_relational_db_spec):
    super(AwsManagedRelationalDb, self).__init__(managed_relational_db_spec)
    self.spec = managed_relational_db_spec
    self.instance_id = 'pkb-db-instance-' + FLAGS.run_uri
    self.zone = self.spec.vm_spec.zone
    self.region = util.GetRegionFromZone(self.zone)

  def GetResourceMetadata(self):
    metadata = super(AwsManagedRelationalDb, self).GetMetadata()
    metadata.update({
      'zone': self.primary_zone,
    })
    if self.spec.high_availability:
      metadata.update({
          'secondary_zone': self.secondary_zone,
      })
    return metadata

  @staticmethod
  def GetDefaultDatabaseVersion(database):
    """Returns the default version of a given database.

    Args:
      database (string): type of database (my_sql or postgres).
    Returns:
      (string): Default database version.
    """
    if database == managed_relational_db.MYSQL:
      return DEFAULT_MYSQL_VERSION
    elif database == managed_relational_db.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif database == managed_relational_db.AURORA_POSTGRES:
      return DEFAULT_POSTGRES_VERSION

  def GetEndpoint(self):
    return self.endpoint

  def GetPort(self):
    return self.port

  def _GetNewZones(self):
    # Get a list of zones, excluding the one that the VM is in.
    zone =  self.spec.vm_spec.zone
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
    # Now create a new subnet in the zone that's different from where the VM is
    new_subnet_zones = self._GetNewZones()
    while len(new_subnet_zones) >= 1:
      try:
        new_subnet_zone = new_subnet_zones.pop()
        logging.info('Attempting to create a second subnet in zone %s',
                     new_subnet_zone)
        new_subnet = aws_network.AwsSubnet(new_subnet_zone,
                                           self.network.regional_network.vpc.id,
                                           '10.0.1.0/24')
        new_subnet.Create()
        logging.info('Successfully created a new subnet, subnet id is: %s',
                     new_subnet.id)

        # save for cleanup
        self.spec.extra_subnet_for_db = new_subnet
        return new_subnet
      except:
        logging.info('Unable to create subnet in zone %s', new_subnet_zone)
    raise Exception('Unable to create subnet in any availability zones')

  def _CreateDbSubnetGroup(self, new_subnet):
    # Now we can create a new DB subnet group that has two subnets in it.
    zone =  self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    db_subnet_group_name = 'pkb-db-subnet-group-{0}'.format(FLAGS.run_uri)
    create_db_subnet_group_cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-subnet-group',
        '--db-subnet-group-name', db_subnet_group_name,
        '--db-subnet-group-description', 'pkb_subnet_group_for_db',
        '--subnet-ids', self.network.subnet.id, new_subnet.id,
        '--region', region]
    stdout, stderr, _ = vm_util.IssueCommand(create_db_subnet_group_cmd)
    # save for cleanup
    self.spec.db_subnet_group_name = db_subnet_group_name

  def _SetupNetworking(self):
    zone =  self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    new_subnet = self._CreateSubnetInAdditionalZone()
    self._CreateDbSubnetGroup(new_subnet)
    group_id = self.network.regional_network.vpc.default_security_group_id

    open_port_cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id', group_id,
        '--source-group', group_id,
        '--protocol', 'tcp',
        '--port={0}'.format(DEFAULT_POSTGRES_PORT),
        '--region', region]
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info('Granted DB port ingress, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)

  def _TeardownNetworking(self):
    zone =  self.spec.vm_spec.zone
    region = util.GetRegionFromZone(zone)
    if hasattr(self.spec, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name', self.spec.db_subnet_group_name,
          '--region', region]
      stdout, stderr, _ = vm_util.IssueCommand(delete_db_subnet_group_cmd)

    if hasattr(self.spec, 'extra_subnet_for_db'):
      self.spec.extra_subnet_for_db.Delete()

  def _Create(self):
    """Creates the AWS RDS instance"""
    group_id = self.network.regional_network.vpc.default_security_group_id
    cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-instance',
        '--db-instance-identifier=%s' % self.instance_id,
        '--engine=%s' % self.spec.database,
        '--master-username=%s' % self.spec.database_username,
        '--master-user-password=%s' % self.spec.database_password,
        '--allocated-storage=%s' % self.spec.disk_spec.disk_size,
        '--storage-type=%s' % self.spec.disk_spec.disk_type,
        '--db-instance-class=%s' % self.spec.vm_spec.machine_type,
        '--no-auto-minor-version-upgrade',
        '--region=%s' % self.region,
        '--engine-version=%s' % self.spec.database_version,
        '--db-subnet-group-name=%s' % self.spec.db_subnet_group_name,
        '--vpc-security-group-ids=%s' % group_id,
    ]

    if self.spec.disk_spec.disk_type == aws_disk.IO1:
      cmd.append('--iops=%s' % self.spec.disk_spec.iops)

    if self.spec.high_availability:
      cmd.append('--multi-az')
    else:
      cmd.append('--availability-zone=%s' % self.spec.vm_spec.zone)

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
    return describe_instance_json['DBInstances'][0]['Endpoint']['Address']

  def _ParsePort(self, describe_instance_json):
    return describe_instance_json['DBInstances'][0]['Endpoint']['Port']

  def _SavePrimaryAndSecondaryZones(self, json_output):
      self.primary_zone = json_output['DBInstances'][0]['AvailabilityZone']
      if self.spec.high_availability:
        self.secondary_zone = (
            json_output['DBInstances'][0]['SecondaryAvailabilityZone'])

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    timeout = 60 * 60 * 3 # 3 hours. (RDS HA takes a long time to prepare)
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % self.instance_id,
        '--region=%s' % self.region
    ]
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds > timeout:
        loggine.exception('Timeout waiting for sql instance to be ready')
        return False
      time.sleep(5)
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
    self.endpoint = self._ParseEndpoint(json_output)
    self.port = self._ParsePort(json_output)
    return True

  def _PostCreate(self):
    """Method that will be called once after _CreateReource is called.

    Supplying this method is optional. If it is supplied, it will be called
    once, after the resource is confirmed to exist. It is intended to allow
    data about the resource to be collected or for the resource to be tagged.
    """
    pass

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    self._SetupNetworking()

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    self._TeardownNetworking()
