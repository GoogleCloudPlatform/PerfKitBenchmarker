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


# TODO(shuninglin): Add cluster creation from a backup.
# TODO(shuninglin): Add reaper for this new resource.

FLAGS = flags.FLAGS

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

  def __init__(self, dsql_spec: AwsAuroraDsqlSpec):
    super().__init__(dsql_spec)
    self.cluster_id = None

  # DSQL has different format for tags:
  # https://docs.aws.amazon.com/cli/v1/reference/rds/create-db-cluster.html
  def _MakeDsqlTags(self) -> list[str]:
    """Returns the tags to be used for the DSQL cluster."""
    tags: dict[str, Any] = util.MakeDefaultTags()
    formatted_tags_list: list[str] = [
        '%s=%s' % (k, v) for k, v in sorted(tags.items())
    ]
    formatted_tags_str = ','.join(formatted_tags_list)
    return [formatted_tags_str]

  def _Create(self) -> None:
    """Creates the AWS Aurora DSQL instance.

    Raises:
      Exception: if unknown how to create self.spec.engine.
    """
    cmd = (
        util.AWS_PREFIX
        + [
            'dsql',
            'create-cluster',
            '--region=%s' % self.region,
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
    cmd = util.AWS_PREFIX + [
        'dsql',
        'get-cluster',
        '--identifier=%s' % self.cluster_id,
        '--region=%s' % self.region,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return None
    json_output: dict[str, Any] = json.loads(stdout)
    return json_output

  def _IsReady(self, timeout=aws_relational_db.IS_READY_TIMEOUT) -> bool:
    """Returns true if the cluster is ready."""
    json_output = self._DescribeCluster()
    return bool(json_output and json_output['status'] == 'ACTIVE')

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
