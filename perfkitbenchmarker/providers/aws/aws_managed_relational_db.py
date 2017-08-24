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

import json
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import managed_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util
from perfkitbenchmarker.providers.aws import aws_disk

FLAGS = flags.FLAGS

DEFAULT_MYSQL_VERSION = '5.7.11'


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
      raise NotImplementedError('Postgres not supported on AWS')

  def GetEndpoint(self):
    return self.endpoint

  def GetPort(self):
    return self.port

  def _AuthorizeDbSecurityGroup(self):
    cidr_to_authorize = '0.0.0.0/0'
    cmd = util.AWS_PREFIX + [
        'rds',
        'authorize-db-security-group-ingress',
        '--db-security-group-name=%s' % self.security_group_name,
        '--cidrip=%s' % cidr_to_authorize
    ]
    vm_util.IssueCommand(cmd)

  def _CreateDbSecurityGroup(self):
    self.security_group_name = 'pkb-db-security-group-%s' % FLAGS.run_uri
    cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-security-group',
        '--db-security-group-name=%s' % self.security_group_name,
        '--db-security-group-description=%s' % 'created by PKB'
    ]
    vm_util.IssueCommand(cmd)

  def _SetupNetworking(self):
    self._CreateDbSecurityGroup()
    self._AuthorizeDbSecurityGroup()
    # vpc = self.spec.vm_spec.network.regional_network.vpc
    # next_cidr_block = self.vpc.NextCidrBlock()

  def _TeardownNetworking(self):
    cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-security-group',
        '--db-security-group-name=%s' % self.security_group_name
    ]
    vm_util.IssueCommand(cmd)

  def _Create(self):
    """Creates the AWS RDS instance"""
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
        '--db-security-groups=%s' % self.security_group_name,
        '--region=%s' % self.region,
        '--engine-version=%s' % self.spec.database_version
    ]

    if self.spec.disk_spec.disk_type == aws_disk.IO1:
      cmd.append('--iops=%s' % self.spec.disk_spec.iops)

    if self.spec.high_availability:
      cmd.append('--multi-az')

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
        '--skip-final-snapshot'
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

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-identifier=%s' % self.instance_id,
        '--region=%s' % self.region
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    json_output = json.loads(stdout)
    is_ready = (json_output['DBInstances'][0]['DBInstanceStatus']
                == 'available')
    if not is_ready:
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
