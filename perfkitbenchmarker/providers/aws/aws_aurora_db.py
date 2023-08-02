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
"""Managed relational database provisioning and teardown for AWS Aurora."""

import json
import time
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_relational_db
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

DEFAULT_MYSQL_AURORA_VERSION = '8.0'
DEFAULT_POSTGRES_AURORA_VERSION = '13'

_MAP_ENGINE_TO_DEFAULT_VERSION = {
    sql_engine_utils.AURORA_MYSQL: DEFAULT_MYSQL_AURORA_VERSION,
    sql_engine_utils.AURORA_POSTGRES: DEFAULT_POSTGRES_AURORA_VERSION,
}

_AURORA_ENGINES = [
    sql_engine_utils.AURORA_MYSQL,
    sql_engine_utils.AURORA_POSTGRES]


class AwsAuroraRelationalDb(aws_relational_db.BaseAwsRelationalDb):
  """Implements the aurora database for AWS."""
  CLOUD = 'AWS'
  IS_MANAGED = True
  ENGINE = _AURORA_ENGINES
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']

  def __init__(self, relational_db_spec):
    super(AwsAuroraRelationalDb, self).__init__(relational_db_spec)
    self.cluster_id = 'pkb-db-cluster-' + FLAGS.run_uri
    self.storage_type = aws_flags.AURORA_STORAGE_TYPE.value
    self._load_machine_type = self.spec.db_spec.machine_type
    if self.spec.load_machine_type is not None:
      self._load_machine_type = self.spec.load_machine_type

  def _Create(self):
    """Creates the AWS RDS instance.

    Raises:
      Exception: if unknown how to create self.spec.engine.
    """
    zones_needed_for_high_availability = len(self.zones) > 1
    if zones_needed_for_high_availability != self.spec.high_availability:
      raise Exception('When db_high_availability is true, multiple '
                      'zones must be specified.  When '
                      'db_high_availability is false, one zone '
                      'should be specified.   '
                      'db_high_availability: {0}  '
                      'zone count: {1} '.format(
                          zones_needed_for_high_availability, len(self.zones)))

    # Create the cluster.
    cmd = (
        util.AWS_PREFIX
        + [
            'rds',
            'create-db-cluster',
            '--db-cluster-identifier=%s' % self.cluster_id,
            '--engine=%s' % self.spec.engine,
            '--engine-version=%s' % self.spec.engine_version,
            '--master-username=%s' % self.spec.database_username,
            '--master-user-password=%s' % self.spec.database_password,
            '--region=%s' % self.region,
            '--db-subnet-group-name=%s' % self.db_subnet_group_name,
            '--vpc-security-group-ids=%s' % self.security_group_id,
            '--availability-zones=%s' % self.spec.zones[0],
            '--storage-type=%s' % self.storage_type,
            '--tags',
        ]
        + util.MakeFormattedDefaultTags()
    )

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
          '--db-cluster-identifier=%s' % self.cluster_id,
          '--engine=%s' % self.spec.engine,
          '--engine-version=%s' % self.spec.engine_version,
          '--no-auto-minor-version-upgrade',
          '--db-instance-class=%s' % self.spec.db_spec.machine_type,
          '--region=%s' % self.region,
          '--availability-zone=%s' % zone, '--tags'
      ] + util.MakeFormattedDefaultTags()
      vm_util.IssueCommand(cmd)

  def _PostCreate(self):
    """Perform general post create operations on the cluster.

    Raises:
       Exception:  If could not ready the instance after modification to
                   multi-az.
    """
    super()._PostCreate()
    self._SetPrimaryAndSecondaryZones()

  def _UpdateWriterInstanceClass(self, instance_class: str) -> None:
    """Updates DBInstanceClass for the writer instance."""
    writer_instance = self.all_instance_ids[0]
    current_instance_class = self._DescribeInstance(writer_instance)[
        'DBInstances'
    ][0]['DBInstanceClass']
    if current_instance_class != instance_class:
      logging.info(
          'Updating capacity from %s to %s',
          current_instance_class,
          instance_class,
      )
      cmd = util.AWS_PREFIX + [
          'rds',
          'modify-db-instance',
          '--db-instance-identifier=%s' % writer_instance,
          '--region=%s' % self.region,
          '--db-instance-class=%s' % instance_class,
          '--apply-immediately',
      ]
      vm_util.IssueCommand(cmd, raise_on_failure=True)
      while not self._IsInstanceReady(instance_id=writer_instance):
        time.sleep(5)

  def UpdateCapacityForLoad(self) -> None:
    """See base class."""
    self._UpdateWriterInstanceClass(self._load_machine_type)

  def UpdateCapacityForRun(self) -> None:
    """See base class."""
    self._UpdateWriterInstanceClass(self.spec.db_spec.machine_type)

  def _DescribeCluster(self):
    cmd = util.AWS_PREFIX + [
        'rds', 'describe-db-clusters',
        '--db-cluster-identifier=%s' % self.cluster_id,
        '--region=%s' % self.region
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output = json.loads(stdout)
    return json_output

  def _ClusterExists(self):
    """Returns true if the underlying cluster exists."""
    json_output = self._DescribeCluster()
    if json_output:
      return True
    return False

  def _SetEndpoint(self):
    """Assigns the ports and endpoints from the cluster_id to self.

    These will be used to communicate with the data base.
    """
    json_output = self._DescribeCluster()
    self.endpoint = json_output['DBClusters'][0]['Endpoint']
    if 'ReaderEndpoint' in json_output['DBClusters'][0]:
      self.replica_endpoint = json_output['DBClusters'][0]['ReaderEndpoint']

  def _FailoverHA(self):
    """Fail over from master to replica."""
    new_primary_id = self.all_instance_ids[1]
    cmd = util.AWS_PREFIX + [
        'rds', 'failover-db-cluster',
        '--db-cluster-identifier=%s' % self.cluster_id,
        '--target-db-instance-identifier=%s' % new_primary_id,
        '--region=%s' % self.region
    ]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the aurora cluster."""
    super()._Delete()

    cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-cluster',
        '--db-cluster-identifier=%s' % self.cluster_id,
        '--skip-final-snapshot',
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

    @vm_util.Retry(
        poll_interval=60,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableDeletionError,))
    def WaitUntilClusterDeleted():
      if self._ClusterExists():
        raise errors.Resource.RetryableDeletionError('Not yet deleted')
    WaitUntilClusterDeleted()

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

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata['aurora_storage_type'] = self.storage_type
    return metadata
