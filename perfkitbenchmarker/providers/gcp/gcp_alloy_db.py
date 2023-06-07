"""Managed relational database provisioning for AlloyDb.

AlloyDb is currently a postgres engine GCP supporting to achieve better
performance with postgres.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

from absl import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

# https://cloud.google.com/alloydb/docs/columnar-engine/enable
_COLUMNAR_ENGINE = flags.DEFINE_bool('alloydb_columnar_engine', False,
                                     'Set to true to enable columnar engine.')

_COLUMNAR_ENGINE_SIZE = flags.DEFINE_integer(
    'alloydb_columnar_engine_size_mb', 1024,
    'Columnar engine is set to 1GB by default.')

_ENABLE_AUTO_COLUMNARIZATION = flags.DEFINE_enum(
    'alloydb_enable_auto_columnarization', 'on', ['on', 'off'],
    'Set alloydb_enable_auto_columnarization to On or off.')

_ENABLE_COLUMNAR_RECOMMENDATION = flags.DEFINE_bool(
    'alloydb_enable_columnar_recommendation', False,
    'Set alloydb_enable_columnar_recommendation to On if true.')


_READ_POOL_NODE_COUNT = flags.DEFINE_integer(
    'alloydb_read_pool_node_count', 0, 'Create read replica for alloydb.',
    upper_bound=20)

SUPPORTED_ALLOYDB_ENGINE_VERSIONS = ['13']

DEFAULT_ENGINE_VERSION = '13'
CREATION_TIMEOUT = 30 * 60
IS_READY_TIMEOUT = 600  # 10 minutes


class GCPAlloyRelationalDb(relational_db.BaseRelationalDb):
  """A GCP AlloyDB database resource.

  This class contains logic required to provision and teardown the database.
  """
  CLOUD = 'GCP'
  IS_MANAGED = True
  ENGINE = 'alloydb-postgresql'
  REQUIRED_ATTRS = ['CLOUD', 'IS_MANAGED', 'ENGINE']

  def __init__(self, db_spec: relational_db_spec.RelationalDbSpec):
    super(GCPAlloyRelationalDb, self).__init__(db_spec)
    self.cluster_id = self.instance_id + '-cluster'
    self.zone = self.spec.db_spec.zone
    self.replica_instance_id = None
    self.spec.database_username = 'postgres'
    self.region = util.GetRegionFromZone(self.zone)
    self.project = FLAGS.project or util.GetDefaultProject()
    self.enable_columnar_engine = _COLUMNAR_ENGINE.value
    self.enable_columnar_engine_recommendation = (
        _ENABLE_COLUMNAR_RECOMMENDATION.value)

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    if engine not in SUPPORTED_ALLOYDB_ENGINE_VERSIONS:
      raise NotImplementedError('Default engine not specified for '
                                'engine {0}'.format(engine))
    return DEFAULT_ENGINE_VERSION

  def GetResourceMetadata(self) -> Dict[str, Any]:
    metadata = super().GetResourceMetadata()
    metadata.update({
        'alloydb_columnar_engine': _COLUMNAR_ENGINE.value,
        'alloydb_columnar_engine_size': _COLUMNAR_ENGINE_SIZE.value,
        'alloydb_enable_columnar_recommendation': (
            _ENABLE_COLUMNAR_RECOMMENDATION.value
        ),
        'alloydb_enable_auto_columnarization': (
            _ENABLE_AUTO_COLUMNARIZATION.value
        ),
    })
    return metadata

  def _Create(self) -> None:
    """Creates the Cloud SQL instance and authorizes traffic from anywhere.

    Raises:
      UnsupportedDatabaseEngineError:
        if the database is unmanaged and the engine isn't MYSQL.
      Exception: if an invalid MySQL flag was used.
    """
    # Create a database cluster
    cmd_string = [
        'clusters', 'create', self.cluster_id,
        '--password=%s' % self.spec.database_password
    ]

    cmd = self._GetAlloyDbCommand(cmd_string)
    _, _, _ = cmd.Issue(timeout=CREATION_TIMEOUT)

    # Create a primary instance
    cmd_string = [
        'instances', 'create', self.instance_id,
        '--cluster=%s' % self.cluster_id,
        '--cpu-count=%s' % self.spec.db_spec.cpus, '--instance-type=PRIMARY'
    ]

    cmd = self._GetAlloyDbCommand(cmd_string)
    cmd.Issue(timeout=CREATION_TIMEOUT)

    cmd_string = [
        'instances', 'describe', self.instance_id,
        '--cluster=%s' % self.cluster_id
    ]

    # Assign the endpoint from the describe command
    cmd = self._GetAlloyDbCommand(cmd_string)
    stdout, _, _ = cmd.Issue()
    json_output = json.loads(stdout)
    self.endpoint = json_output['ipAddress']

    if _READ_POOL_NODE_COUNT.value:
      self.replica_instance_id = self.instance_id + '-read-replica'
      # Create read replica
      cmd_string = [
          'instances',
          'create',
          self.replica_instance_id,
          f'--cluster={self.cluster_id}',
          f'--cpu-count={self.spec.db_spec.cpus}',
          f'--read-pool-node-count={_READ_POOL_NODE_COUNT.value}',
          '--instance-type=READ_POOL',
      ]
      cmd = self._GetAlloyDbCommand(cmd_string)
      cmd.Issue(timeout=CREATION_TIMEOUT)

      # Assign the endpoint for the read replica
      cmd_string = [
          'instances', 'describe', self.replica_instance_id,
          f'--cluster={self.cluster_id}'
      ]
      cmd = self._GetAlloyDbCommand(cmd_string)
      stdout, _, _ = cmd.Issue()
      json_output = json.loads(stdout)
      self.replica_endpoint = json_output['ipAddress']

  def _PostCreate(self) -> None:
    """Creates the PKB user and sets the password."""
    super()._PostCreate()
    self.client_vm_query_tools.InstallPackages()
    columnar_engine_size = None
    if _COLUMNAR_ENGINE.value:
      columnar_engine_size = _COLUMNAR_ENGINE_SIZE.value
    self.UpdateAlloyDBFlags(columnar_engine_size,
                            _ENABLE_COLUMNAR_RECOMMENDATION.value,
                            _ENABLE_AUTO_COLUMNARIZATION.value)

  def AddTableToColumnarEngine(self, table: str, database_name: str) -> None:
    self.client_vm_query_tools.IssueSqlCommand(
        f'SELECT google_columnar_engine_add(\'{table}\');',
        database_name=database_name)

  def CreateColumnarEngineExtension(self, database_name: str) -> None:
    self.client_vm_query_tools.IssueSqlCommand(
        'CREATE EXTENSION IF NOT EXISTS google_columnar_engine;',
        database_name=database_name)

  def RunColumnarEngineRecommendation(self, database_name: str) -> None:
    self.client_vm_query_tools.IssueSqlCommand(
        'SELECT '
        'google_columnar_engine_run_recommendation'
        '(0, \'FIXED_SIZE\', true)',
        database_name=database_name)

  def WaitColumnarEnginePopulates(self, database_name: str):
    self.client_vm_query_tools.IssueSqlCommand(
        'SELECT google_columnar_engine_jobs_wait(14400000)', database_name)

  def UpdateAlloyDBFlags(self,
                         columnar_engine_size: Optional[int],
                         enable_columnar_recommendation: bool,
                         enable_auto_columnarization: str,
                         relation: Optional[str] = None):
    database_flags = []
    if FLAGS.db_flags:
      database_flags += [':'.join(FLAGS.db_flags)]

    if columnar_engine_size:
      database_flags += [
          'google_columnar_engine.enabled=on',
          'google_columnar_engine.memory_size_in_mb'
          f'={columnar_engine_size}',
          'google_columnar_engine.enable_auto_columnarization='
          f'{enable_auto_columnarization}'
      ]

      if enable_columnar_recommendation:
        database_flags += [
            'google_columnar_engine.enable_columnar_recommendation=on',
        ]
      if relation:
        database_flags += [f'google_columnar_engine.relations={relation}']
    if database_flags:
      database_flags_str = ':'.join(database_flags)
      cmd_string = [
          'instances', 'update', self.instance_id,
          f'--database-flags=^:^{database_flags_str}',
          f'--cluster={self.cluster_id}'
      ]
      cmd = self._GetAlloyDbCommand(cmd_string)
      cmd.Issue(timeout=CREATION_TIMEOUT)

  def GetColumnarEngineRecommendation(self,
                                      database_name: str) -> Tuple[int, str]:
    result, _ = self.client_vm_query_tools.IssueSqlCommand(
        'SELECT google_columnar_engine_run_recommendation'
        '(65536, \'PERFORMANCE_OPTIMAL\');', database_name)
    regex = r'\((\d*),"(.*)"\)'
    return (regex_util.ExtractInt(regex, result),
            regex_util.ExtractGroup(regex, result, group=2))

  def _GetAlloyDbCommand(self,
                         cmd_string: List[str],
                         timeout: Optional[int] = None) -> util.GcloudCommand:
    """Used to issue alloydb command."""
    cmd_string = [self, 'alpha', 'alloydb'] + cmd_string
    cmd = util.GcloudCommand(*cmd_string)
    cmd.flags['project'] = self.project
    cmd.flags['zone'] = []
    cmd.flags['region'] = self.region
    return cmd

  def _IsReady(self, timeout: int = IS_READY_TIMEOUT) -> bool:
    """Return true if the underlying resource is ready.

    Alloydb Creation is synchronous, therefore is ready is not needed.

    Args:
      timeout: how long to wait when checking if the DB is ready.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    return True

  def _ApplyDbFlags(self):
    # Database flags is applied during creation.
    pass

  def _Delete(self) -> None:
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    cmd_string = ['clusters', 'delete', self.cluster_id, '--force', '--async']
    cmd = self._GetAlloyDbCommand(cmd_string)
    cmd.Issue()
