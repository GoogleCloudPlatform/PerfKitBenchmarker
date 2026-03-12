# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Managed relational database provisioning and teardown for AWS Aurora DSQL."""

import functools
import json
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.aws import aws_relational_db
from perfkitbenchmarker.providers.aws import util


FLAGS = flags.FLAGS

AWS_AURORA_DSQL_RECOVERY_POINT_ARN = flags.DEFINE_string(
    'aws_aurora_dsql_recovery_point_arn',
    None,
    'The ARN of the recovery point to restore AWS Aurora DSQL cluster from. If '
    'not provided, a new cluster is created from scratch.',
)
DEFAULT_AURORA_DSQL_POSTGRES_VERSION = '16.2'

_MAP_ENGINE_TO_DEFAULT_VERSION = {
    sql_engine_utils.AURORA_DSQL_POSTGRES: DEFAULT_AURORA_DSQL_POSTGRES_VERSION,
}

_AURORA_DSQL_ENGINES = [
    sql_engine_utils.AURORA_DSQL_POSTGRES,
]

_NONE_OK = {'default': None, 'none_ok': True}


class AwsAuroraDsqlSpec(relational_db_spec.RelationalDbSpec):
  """Configurable options for AWS Aurora DSQL."""

  SERVICE_TYPE = 'aurora-dsql'

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'db_disk_spec': (spec.PerCloudConfigDecoder, _NONE_OK),
    })
    return result


class AwsAuroraDsqlRelationalDb(aws_relational_db.BaseAwsRelationalDb):
  """Implements the aurora DSQL database for AWS."""

  CLOUD = 'AWS'
  IS_MANAGED = True
  ENGINE = _AURORA_DSQL_ENGINES
  READY_TIMEOUT = 60 * 60

  def __init__(self, dsql_spec: AwsAuroraDsqlSpec):
    super().__init__(dsql_spec)
    self.cluster_id = None
    self.cluster_arn = None
    self.assigned_name = f'pkb-{FLAGS.run_uri}'
    self.use_backup = bool(AWS_AURORA_DSQL_RECOVERY_POINT_ARN.value)
    self.restore_job_id = None

  @functools.cached_property
  def account_id(self) -> str:
    """Returns the AWS account ID."""
    return util.GetAccount()

  # DSQL has different format for tags:
  # https://docs.aws.amazon.com/cli/v1/reference/rds/create-db-cluster.html
  def _MakeDsqlTags(self) -> list[str]:
    """Returns the tags to be used for the DSQL cluster."""
    tags: dict[str, Any] = util.MakeDefaultTags()
    formatted_tags_list: list[str] = [
        '%s=%s' % (k, v) for k, v in sorted(tags.items())
    ] + ['Name=%s' % self.assigned_name]
    formatted_tags_str = ','.join(formatted_tags_list)
    return [formatted_tags_str]

  def _Create(self) -> None:
    """Creates AWS Aurora DSQL cluster, from backup if recovery point ARN is provided."""
    if not self.use_backup:
      self._CreateRawCluster()
      return
    if self.restore_job_id:
      logging.info(
          'Restore job %s already exists. Skipping creation.',
          self.restore_job_id,
      )
      return
    cmd = util.AWS_PREFIX + [
        'backup',
        'start-restore-job',
        '--recovery-point-arn',
        AWS_AURORA_DSQL_RECOVERY_POINT_ARN.value,
        '--region',
        self.region,
        '--iam-role-arn',
        (
            f'arn:aws:iam::{self.account_id}:role/service-role/'
            'AWSBackupDefaultServiceRole'
        ),
        '--metadata',
        '{"regionalConfig": "[{\\"region\\": \\"%s\\",'
        ' \\"isDeletionProtectionEnabled\\": false}]"}'
        % self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    response = json.loads(stdout)
    self.restore_job_id = response['RestoreJobId']
    if self.restore_job_id:
      # Mark created so we don't try to create it again on a retry.
      self.created = True

  def _DescribeRestoreJob(self, job_id: str) -> dict[str, Any]:
    """Describes the restore job."""
    cmd = util.AWS_PREFIX + [
        'backup',
        'describe-restore-job',
        '--region',
        self.region,
        '--restore-job-id',
        job_id,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(stdout)

  def _AddTagsToCluster(self, cluster_arn: str) -> None:
    """Adds tags to the DSQL cluster."""
    cmd = (
        util.AWS_PREFIX
        + [
            'dsql',
            'tag-resource',
            '--region',
            self.region,
            '--resource-arn=%s' % cluster_arn,
            '--tags',
        ]
        + self._MakeDsqlTags()
    )
    vm_util.IssueCommand(cmd)

  def _CreateRawCluster(self) -> None:
    """Creates the AWS Aurora DSQL instance.

    Raises:
      Exception: if unknown how to create self.spec.engine.
    """
    cmd = (
        util.AWS_PREFIX
        + [
            'dsql',
            'create-cluster',
            '--region',
            self.region,
            # Make it easier for deletion/reaping
            '--no-deletion-protection-enabled',
            '--tags',
        ]
        + self._MakeDsqlTags()
    )

    stdout, _, _ = vm_util.IssueCommand(cmd)
    response = json.loads(stdout)
    # We are not allowed to define cluster id ourselves, so we have to use the
    # one returned by the create-cluster command.
    self.cluster_id = response['identifier']

  def _DescribeCluster(self) -> dict[str, Any] | None:
    if not self.cluster_id:
      logging.info('Cluster id is not set.')
      return None
    cmd = util.AWS_PREFIX + [
        'dsql',
        'get-cluster',
        '--identifier=%s' % self.cluster_id,
        '--region',
        self.region,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output: dict[str, Any] = json.loads(stdout)
    return json_output

  def _IsReady(self, timeout=aws_relational_db.IS_READY_TIMEOUT) -> bool:
    """Returns true if the cluster is ready."""
    if self.use_backup:
      if not self.restore_job_id:
        return False
      job_description = self._DescribeRestoreJob(self.restore_job_id)
      status = job_description['Status']
      if status == 'COMPLETED':
        self.cluster_id = job_description['CreatedResourceArn'].split('/')[-1]
        self.cluster_arn = job_description['CreatedResourceArn']
        return True
      if status in ['ABORTED', 'FAILED']:
        raise errors.Resource.CreationError(
            f'Restore job {self.restore_job_id} failed with status {status}'
        )
      return False
    else:
      json_output = self._DescribeCluster()
      return bool(json_output and json_output['status'] == 'ACTIVE')

  def _PostCreate(self) -> None:
    """Add tags if we are restoring from backup."""
    super()._PostCreate()
    if self.use_backup:
      self._AddTagsToCluster(self.cluster_arn)

  def _Exists(self) -> bool:
    """Returns true if the underlying cluster exists."""
    json_output = self._DescribeCluster()
    if json_output:
      return True
    return False

  def _Delete(self) -> None:
    """Deletes the DSQL cluster."""

    logging.info(
        'Deleting DSQL cluster %s in region %s',
        self.cluster_id,
        self.region,
    )

    cmd = util.AWS_PREFIX + [
        'dsql',
        'delete-cluster',
        '--identifier=%s' % self.cluster_id,
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

    @vm_util.Retry(
        poll_interval=60,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableDeletionError,),
    )
    def WaitUntilClusterDeleted():
      if self._Exists():
        raise errors.Resource.RetryableDeletionError('Not yet deleted')

    WaitUntilClusterDeleted()

  def _GetHostname(self) -> str:
    """Returns endpoint of DSQL cluster."""
    return f'{self.cluster_id}.dsql.{self.region}.on.aws'

# TODO(shuninglin): Extend PostgresCliQueryTools for DSQL.
  def RunSqlQuery(self, sql_query: str) -> None:
    """Runs a SQL query on the database."""
    # Local import to avoid dependency not found issue.
    import boto3  # pylint: disable=g-import-not-at-top
    hostname = self._GetHostname()
    # Since we are using aws cli v1 where the token generation is not supported,
    # we are using sdk here to generate the token.
    client = boto3.client('dsql', region_name=self.region)
    token = client.generate_db_connect_admin_auth_token(hostname, self.region)
    connection_properties = sql_engine_utils.DbConnectionProperties(
        engine=self.spec.engine,
        engine_version=self.spec.engine_version,
        endpoint=hostname,
        port=5432,
        database_username='admin',
        database_password=token,
        instance_name=self.cluster_id,
        database_name='postgres',
    )
    query_tools = sql_engine_utils.PostgresCliQueryTools(
        self.client_vm, connection_properties
    )
    cmd = query_tools.MakeSqlCommand(sql_query, database_name='postgres')
    self.client_vm.RemoteCommand(f'PGSSLMODE=require {cmd}')

  @staticmethod
  def GetDefaultEngineVersion(engine) -> str:
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default engine version.
    Raises:
      errors.Config.InvalidValue: If unrecognized engine is specified.
    """
    if engine not in _MAP_ENGINE_TO_DEFAULT_VERSION:
      raise errors.Config.InvalidValue(
          'Unspecified default version for {}'.format(engine)
      )
    return _MAP_ENGINE_TO_DEFAULT_VERSION[engine]

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns useful metadata about the instance."""
    metadata = {
        'dsql_cluster_id': self.cluster_id,
    }
    return metadata
