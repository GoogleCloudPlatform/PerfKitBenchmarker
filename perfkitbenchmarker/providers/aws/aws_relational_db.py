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

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import mysql_iaas_relational_db
from perfkitbenchmarker import postgres_iaas_relational_db
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sqlserver_iaas_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '8'
DEFAULT_POSTGRES_VERSION = '13'
DEFAULT_SQLSERVER_VERSION = '14'
IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (RDS HA takes a long time to prepare)

MYSQL_SUPPORTED_MAJOR_VERSIONS = ['5.7', '8.0']
POSTGRES_SUPPORTED_MAJOR_VERSIONS = ['9.6', '10', '11', '12', '13', '14', '15']


class AWSSQLServerIAASRelationalDb(
    sqlserver_iaas_relational_db.SQLServerIAASRelationalDb
):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AWS


class AWSPostgresIAASRelationalDb(
    postgres_iaas_relational_db.PostgresIAASRelationalDb
):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AWS


class AWSMysqlIAASRelationalDb(mysql_iaas_relational_db.MysqlIAASRelationalDb):
  """A AWS IAAS database resource."""

  CLOUD = provider_info.AWS


class AwsRelationalDbParameterError(Exception):
  """Exceptions for invalid Db parameters."""

  pass


class BaseAwsRelationalDb(relational_db.BaseRelationalDb):
  """Base class for aurora and rds relational db.

  RDS and Aurora have similar cli commands. This base class provides
  the common methods across both offerings.
  """

  def __init__(self, relational_db_spec):
    super(BaseAwsRelationalDb, self).__init__(relational_db_spec)
    self.all_instance_ids = []
    self.primary_zone = None
    self.secondary_zone = None
    self.parameter_group = None

    if hasattr(self.spec, 'zones') and self.spec.zones is not None:
      self.zones = self.spec.zones
    else:
      self.zones = [self.spec.db_spec.zone]

    self.region = util.GetRegionFromZones(self.zones)
    self.subnets_created = []
    self.subnets_used_by_db = []

    # dependencies which will be created
    self.db_subnet_group_name: str = None
    self.security_group_id: str = None

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
    if not self.all_instance_ids:
      return False

    for instance_id in self.all_instance_ids:
      if not self._IsInstanceReady(instance_id, timeout):
        return False

    return True

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    # Database subnets requires at least one ore more subnets
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_VPC.WorkingWithRDSInstanceinaVPC.html
    # The first subnet is defined by the client vm
    self.subnets_used_by_db.append(self.client_vm.network.subnet)

    # Get the extra subnet if it is defined in the zone parameter
    # Note that the client subnet is already created and can be ignored
    subnet_zones_to_create = []

    for zone in self.zones:
      if zone != self.client_vm.network.subnet.zone:
        subnet_zones_to_create.append(zone)

    if not subnet_zones_to_create:
      # Choose a zone from cli
      all_zones = util.GetZonesInRegion(self.region)
      all_zones.remove(self.client_vm.network.subnet.zone)
      subnet_zones_to_create.append(sorted(list(all_zones))[0])

    for subnet in subnet_zones_to_create:
      new_subnet = self._CreateSubnetInZone(subnet)
      self.subnets_created.append(new_subnet)
      self.subnets_used_by_db.append(new_subnet)

    self._CreateDbSubnetGroup(self.subnets_used_by_db)

    open_port_cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id',
        self.security_group_id,
        '--source-group',
        self.security_group_id,
        '--protocol',
        'tcp',
        '--port={0}'.format(self.port),
        '--region',
        self.region,
    ]
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info(
        'Granted DB port ingress, stdout is:\n%s\nstderr is:\n%s',
        stdout,
        stderr,
    )

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    self._TeardownNetworking()
    self._TeardownParameterGroup()

  def _CreateSubnetInZone(self, new_subnet_zone):
    """Creates a new subnet in the same region as the client VM.

    Args:
      new_subnet_zone: The zone for the subnet to be created. Must be in the
        same region as the client

    Returns:
      the new subnet resource
    """
    cidr = self.client_vm.network.regional_network.vpc.NextSubnetCidrBlock()
    logging.info('Attempting to create a subnet in zone %s', new_subnet_zone)
    new_subnet = aws_network.AwsSubnet(
        new_subnet_zone, self.client_vm.network.regional_network.vpc.id, cidr
    )
    new_subnet.Create()
    logging.info(
        'Successfully created a new subnet, subnet id is: %s', new_subnet.id
    )
    return new_subnet

  def _CreateDbSubnetGroup(self, subnets):
    """Creates a new db subnet group.

    Args:
      subnets: a list of strings. The db subnet group will consit of all subnets
        in this list.
    """
    db_subnet_group_name = 'pkb-db-subnet-group-{0}'.format(FLAGS.run_uri)

    create_db_subnet_group_cmd = util.AWS_PREFIX + (
        [
            'rds',
            'create-db-subnet-group',
            '--db-subnet-group-name',
            db_subnet_group_name,
            '--db-subnet-group-description',
            'pkb_subnet_group_for_db',
            '--region',
            self.region,
            '--subnet-ids',
        ]
        + [subnet.id for subnet in subnets]
        + ['--tags']
        + util.MakeFormattedDefaultTags()
    )

    vm_util.IssueCommand(create_db_subnet_group_cmd)

    # save for cleanup
    self.db_subnet_group_name = db_subnet_group_name
    self.security_group_id = (
        self.client_vm.network.regional_network.vpc.default_security_group_id
    )

  def _ApplyDbFlags(self):
    """Apply managed flags on RDS."""
    if self.spec.db_flags:
      self.parameter_group = 'pkb-parameter-group-' + FLAGS.run_uri
      cmd = util.AWS_PREFIX + [
          'rds',
          'create-db-parameter-group',
          '--db-parameter-group-name=%s' % self.parameter_group,
          '--db-parameter-group-family=%s' % self._GetParameterGroupFamily(),
          '--region=%s' % self.region,
          '--description="AWS pkb option group"',
      ]

      vm_util.IssueCommand(cmd)

      cmd = util.AWS_PREFIX + [
          'rds',
          'modify-db-instance',
          '--db-instance-identifier=%s' % self.instance_id,
          '--db-parameter-group-name=%s' % self.parameter_group,
          '--region=%s' % self.region,
          '--apply-immediately',
      ]

      vm_util.IssueCommand(cmd)

      for flag in self.spec.db_flags:
        key_value_pair = flag.split('=')
        if len(key_value_pair) != 2:
          raise AwsRelationalDbParameterError('Malformed parameter %s' % flag)
        cmd = util.AWS_PREFIX + [
            'rds',
            'modify-db-parameter-group',
            '--db-parameter-group-name=%s' % self.parameter_group,
            '--parameters=ParameterName=%s,ParameterValue=%s,ApplyMethod=pending-reboot'
            % (key_value_pair[0], key_value_pair[1]),
            '--region=%s' % self.region,
        ]

        vm_util.IssueCommand(cmd)

      self._Reboot()

  def _GetParameterGroupFamily(self):
    """Get the parameter group family string.

    Parameter group family is formatted as engine type plus version.

    Returns:
      ParameterGroupFamiliy name of rds resources.

    Raises:
      NotImplementedError: If there is no supported ParameterGroupFamiliy.
    """
    all_supported_versions = (
        MYSQL_SUPPORTED_MAJOR_VERSIONS + POSTGRES_SUPPORTED_MAJOR_VERSIONS
    )
    for version in all_supported_versions:
      if self.spec.engine_version.startswith(version):
        return self.spec.engine + version

    raise NotImplementedError(
        'The parameter group of engine %s, version %s is not supported'
        % (self.spec.engine, self.spec.engine_version)
    )

  def _TeardownNetworking(self):
    """Tears down all network resources that were created for the database."""
    for subnet_for_db in self.subnets_created:
      subnet_for_db.Delete()
    if hasattr(self, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name',
          self.db_subnet_group_name,
          '--region',
          self.region,
      ]
      vm_util.IssueCommand(delete_db_subnet_group_cmd, raise_on_failure=False)

  def _TeardownParameterGroup(self):
    """Tears down all parameter group that were created for the database."""
    if self.parameter_group:
      delete_db_parameter_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-parameter-group',
          '--db-parameter-group-name',
          self.parameter_group,
          '--region',
          self.region,
      ]
      vm_util.IssueCommand(
          delete_db_parameter_group_cmd, raise_on_failure=False
      )

  def _DescribeInstance(self, instance_id):
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % instance_id,
        '--region=%s' % self.region,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

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
          pending_values = json_output['DBInstances'][0][
              'PendingModifiedValues'
          ]
          waiting_param = (
              json_output['DBInstances'][0]['DBParameterGroups'][0][
                  'ParameterApplyStatus'
              ]
              == 'applying'
          )
          logging.info('Instance state: %s', state)
          if pending_values:
            logging.info('Pending values: %s', (str(pending_values)))

          if waiting_param:
            logging.info('Applying parameter')

          if state == 'insufficient-capacity':
            raise errors.Benchmarks.InsufficientCapacityCloudFailure(
                'Insufficient capacity to provision this db.'
            )
          if state == 'available' and not pending_values and not waiting_param:
            break

        except:  # pylint: disable=bare-except
          logging.exception(
              'Error attempting to read stdout. Creation failure.'
          )
          return False
      time.sleep(5)

    return True

  def _Reboot(self):
    """Reboot the database and wait until the database is in ready state."""
    # Can only reboot when the instance is in ready state
    if not self._IsInstanceReady(self.instance_id, timeout=IS_READY_TIMEOUT):
      raise RuntimeError('Instance is not in a state that can reboot')

    cmd = util.AWS_PREFIX + [
        'rds',
        'reboot-db-instance',
        '--db-instance-identifier=%s' % self.instance_id,
        '--region=%s' % self.region,
    ]

    vm_util.IssueCommand(cmd)

    if not self._IsInstanceReady(self.instance_id, timeout=IS_READY_TIMEOUT):
      raise RuntimeError('Instance could not be set to ready after reboot')

  def _SetPrimaryAndSecondaryZones(self):
    self.primary_zone = self.subnets_used_by_db[0].zone
    if self.spec.high_availability:
      self.secondary_zone = ','.join(
          [x.zone for x in self.subnets_used_by_db[1:]]
      )

  def _DeleteInstance(self, instance_id):
    cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-instance',
        '--db-instance-identifier=%s' % instance_id,
        '--skip-final-snapshot',
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of AWS Managed DB metadata.
    """
    metadata = super(BaseAwsRelationalDb, self).GetResourceMetadata()
    metadata.update({
        'zone': self.primary_zone,
    })

    if self.spec.high_availability:
      metadata.update({
          'secondary_zone': self.secondary_zone,
      })

    return metadata

  def _InstanceExists(self, instance_id) -> bool:
    """Returns true if the underlying instance."""
    json_output = self._DescribeInstance(instance_id)
    if not json_output:
      return False
    return True

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    for current_instance_id in self.all_instance_ids:
      exists = self._InstanceExists(current_instance_id)
      if not exists:
        return False
    return True

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """

    @vm_util.Retry(
        poll_interval=60,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableDeletionError,),
    )
    def WaitUntilInstanceDeleted(instance_id):
      if self._InstanceExists(instance_id):
        raise errors.Resource.RetryableDeletionError('Not yet deleted')

    # Delete both instance concurrently
    for current_instance_id in self.all_instance_ids:
      self._DeleteInstance(current_instance_id)

    for current_instance_id in self.all_instance_ids:
      WaitUntilInstanceDeleted(current_instance_id)
