# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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

from absl import flags
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_relational_db
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
DEFAULT_MYSQL_VERSION = '5.7.16'
DEFAULT_POSTGRES_VERSION = '9.6.9'
DEFAULT_SQLSERVER_VERSION = '14.00.3223.3.v1'

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (HA takes a long time to prepare)

_MAP_ENGINE_TO_DEFAULT_VERSION = {
    sql_engine_utils.MYSQL: DEFAULT_MYSQL_VERSION,
    sql_engine_utils.POSTGRES: DEFAULT_POSTGRES_VERSION,
    sql_engine_utils.SQLSERVER_EXPRESS: DEFAULT_SQLSERVER_VERSION,
    sql_engine_utils.SQLSERVER_STANDARD: DEFAULT_SQLSERVER_VERSION,
    sql_engine_utils.SQLSERVER_ENTERPRISE: DEFAULT_SQLSERVER_VERSION,
}

_SQL_SERVER_ENGINES = (
    sql_engine_utils.SQLSERVER_EXPRESS,
    sql_engine_utils.SQLSERVER_STANDARD,
    sql_engine_utils.SQLSERVER_ENTERPRISE,
)

_RDS_ENGINES = [
    sql_engine_utils.MYSQL,
    sql_engine_utils.POSTGRES,
    sql_engine_utils.SQLSERVER_EXPRESS,
    sql_engine_utils.SQLSERVER_STANDARD,
    sql_engine_utils.SQLSERVER_ENTERPRISE,
]


class AwsRDSRelationalDb(aws_relational_db.BaseAwsRelationalDb):
  """Implements the RDS database for AWS."""

  CLOUD = 'AWS'
  IS_MANAGED = True
  ENGINE = _RDS_ENGINES
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']

  def _Create(self):
    """Creates the AWS RDS instance.

    Raises:
      Exception: if unknown how to create self.spec.engine.
    """
    self.all_instance_ids.append(self.instance_id)
    multi_az = [
        '--no-multi-az',
        '--availability-zone=%s' % self.spec.db_spec.zone,
    ]
    if self.spec.high_availability:
      multi_az = ['--multi-az']

    cmd = (
        util.AWS_PREFIX
        + [
            'rds',
            'create-db-instance',
            '--db-instance-identifier=%s' % self.instance_id,
            '--engine=%s' % self.spec.engine,
        ]
        + multi_az
        + [
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
            '--tags',
        ]
        + util.MakeFormattedDefaultTags()
    )

    if self.spec.engine in _SQL_SERVER_ENGINES:
      cmd = cmd + ['--license-model=license-included']

    if (
        self.spec.db_disk_spec.disk_type == aws_disk.IO1
        or self.spec.db_disk_spec.disk_type == aws_disk.GP3
    ):
      if self.spec.db_disk_spec.provisioned_iops:
        cmd.append('--iops=%s' % self.spec.db_disk_spec.provisioned_iops)
      if self.spec.db_disk_spec.provisioned_throughput:
        cmd.append(
            '--storage-throughput=%s'
            % self.spec.db_disk_spec.provisioned_throughput
        )

    if self.spec.backup_enabled:
      cmd.append('--backup-retention-period=1')
    else:
      cmd.append('--backup-retention-period=0')

    vm_util.IssueCommand(cmd)

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    Raises:
       Exception:  If could not ready the instance after modification to
                   multi-az.
    """
    super()._PostCreate()

    self._SetPrimaryAndSecondaryZones()

  def GetResourceMetadata(self):
    """Returns the metadata associated with the resource.

    All keys will be prefaced with relational_db before
    being published (done in publisher.py).

    Returns:
      metadata: dict of AWS Managed DB metadata.
    """
    metadata = super().GetResourceMetadata()

    if hasattr(self.spec.db_disk_spec, 'provisioned_iops'):
      metadata.update({
          'disk_iops': self.spec.db_disk_spec.provisioned_iops,
      })

    return metadata

  def _SetEndpoint(self):
    """Assigns the ports and endpoints from the instance_id to self.

    These will be used to communicate with the data base.
    """
    json_output = self._DescribeInstance(self.instance_id)
    self.endpoint = json_output['DBInstances'][0]['Endpoint']['Address']

  def _FailoverHA(self):
    """Fail over from master to replica."""
    cmd = util.AWS_PREFIX + [
        'rds',
        'reboot-db-instance',
        '--db-instance-identifier=%s' % self.instance_id,
        '--force-failover',
        '--region=%s' % self.region,
    ]
    vm_util.IssueCommand(cmd)

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
      # pylint: disable-next=broad-exception-raised
      raise Exception('Unspecified default version for {}'.format(engine))
    return _MAP_ENGINE_TO_DEFAULT_VERSION[engine]
