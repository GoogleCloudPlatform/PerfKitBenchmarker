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
"""Module containing class for AWS' documentdb clusters.

clusters can be created and deleted.
"""

import json
import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util


# Billing Modes
_ON_DEMAND = 'PAY_PER_REQUEST'
_PROVISIONED = 'PROVISIONED'

FLAGS = flags.FLAGS
_CLUSTER_NAME = flags.DEFINE_string(
    'aws_documentdb_cluster_name',
    None,
    'The name of the documentdb cluster. This makes the resource user managed'
    ' and assumes the correct zone is passed in.',
)
_INSTANCE_CLASS = flags.DEFINE_string(
    'aws_documentdb_instance_class',
    None,
    'The instance class to use for the documentdb cluster. Corresponds to the'
    ' --db-instance-class parameter for the create command.',
)
_REPLICA_COUNT = flags.DEFINE_integer(
    'aws_documentdb_replica_count',
    0,
    'The number of replicas to use for the documentdb cluster.',
)
_ZONES = flags.DEFINE_list(
    'aws_documentdb_zones',
    None,
    'The zones to use for the documentdb cluster.',
)
_TLS = flags.DEFINE_bool(
    'aws_documentdb_tls',
    False,
    'Whether to enable TLS for the documentdb cluster.',
)
_SNAPSHOT = flags.DEFINE_string(
    'aws_documentdb_snapshot',
    None,
    'If supplied, creates the DocumentDB instance from the snapshot.',
)

_DEFAULT_ZONES = ['us-east-1a', 'us-east-1b', 'us-east-1c']
_DEFAULT_STORAGE_TYPE = 'iopt1'
_DEFAULT_INSTANCE_CLASS = 'db.t3.medium'


class DocumentDbSpec(non_relational_db.BaseNonRelationalDbSpec):
  """Configurable options of a DocumentDB instance."""

  SERVICE_TYPE = non_relational_db.DOCUMENTDB

  name: str
  zones: list[str]
  storage_type: str
  db_instance_class: str
  replica_count: int
  tls: bool
  snapshot: str

  def __init__(self, component_full_name, flag_values, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes / constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    none_ok = {'default': None, 'none_ok': False}
    result.update({
        'name': (option_decoders.StringDecoder, none_ok),
        'storage_type': (option_decoders.StringDecoder, none_ok),
        'zones': (
            option_decoders.ListDecoder,
            {'item_decoder': option_decoders.StringDecoder(), 'default': []},
        ),
        'db_instance_class': (option_decoders.StringDecoder, none_ok),
        'replica_count': (option_decoders.IntDecoder, none_ok),
        'tls': (option_decoders.BooleanDecoder, none_ok),
        'snapshot': (option_decoders.StringDecoder, none_ok),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    option_name_from_flag = {
        'aws_documentdb_cluster_name': 'name',
        'aws_documentdb_instance_class': 'db_instance_class',
        'aws_documentdb_replica_count': 'replica_count',
        'aws_documentdb_zones': 'zones',
        'aws_documentdb_tls': 'tls',
        'aws_documentdb_snapshot': 'snapshot',
    }
    for flag_name, option_name in option_name_from_flag.items():
      if flag_values[flag_name].present:
        config_values[option_name] = flag_values[flag_name].value

  def __repr__(self) -> str:
    return str(self.__dict__)


class DocumentDb(non_relational_db.BaseManagedMongoDb):
  """Class for working with DocumentDB."""

  SERVICE_TYPE = non_relational_db.DOCUMENTDB
  READY_TIMEOUT = 30 * 60

  def __init__(
      self,
      name: str | None = None,
      zones: list[str] | None = None,
      storage_type: str | None = None,
      db_instance_class: str | None = None,
      replica_count: int | None = None,
      tls: bool | None = None,
      snapshot: str | None = None,
      **kwargs,
  ):
    super().__init__(**kwargs)
    self.name = name or f'pkb-{FLAGS.run_uri}'
    self.zones = zones or _DEFAULT_ZONES
    self.db_instance_class = db_instance_class or _DEFAULT_INSTANCE_CLASS
    self.replica_count = replica_count or 0
    self.region = util.GetRegionFromZone(self.zones[0])
    self.resource_arn: str = None  # Set during the _Exists() call.
    self.subnet_group_name: str = self.name + '-subnet-group'
    self.subnets: list[aws_network.AwsSubnet] = []
    self.storage_type = storage_type or _DEFAULT_STORAGE_TYPE
    self.storage_encryption = True
    self.parameter_group = None
    self.tls_enabled = tls or False
    self.snapshot = snapshot

    if name:
      # consider logging warning if spec args were passed
      self.user_managed = True

  @classmethod
  def FromSpec(cls, spec: DocumentDbSpec) -> 'DocumentDb':
    return cls(
        name=spec.name,
        zones=spec.zones,
        storage_type=spec.storage_type,
        db_instance_class=spec.db_instance_class,
        replica_count=spec.replica_count,
        tls=spec.tls,
        snapshot=spec.snapshot,
        enable_freeze_restore=spec.enable_freeze_restore,
        create_on_restore_error=spec.create_on_restore_error,
        delete_on_freeze_error=spec.delete_on_freeze_error,
    )

  def _UserManagedSetup(self):
    """See base class."""
    describe_cluster = self._DescribeCluster()
    self.replica_count = len(describe_cluster['DBClusterMembers']) - 1
    for instance in describe_cluster['DBClusterMembers']:
      instance_id = instance['DBInstanceIdentifier']
      describe_instance = self._DescribeInstance(instance_id)
      self.db_instance_class = describe_instance['DBInstanceClass']

    if not self.parameter_group:
      self.parameter_group = describe_cluster['DBClusterParameterGroup']
      cmd = util.AWS_PREFIX + [
          'docdb',
          'describe-db-cluster-parameters',
          '--db-cluster-parameter-group-name',
          self.parameter_group,
          '--region',
          self.region,
      ]
      stdout, _, _ = vm_util.IssueCommand(cmd)
      for param in json.loads(stdout)['Parameters']:
        if (
            param['ParameterName'] == 'tls'
            and param['ParameterValue'] == 'enabled'
        ):
          self.tls_enabled = True
          return
      self.tls_enabled = False

  def _CreateDependencies(self):
    """See base class."""
    subnet_id = self._GetClientVm().network.subnet.id
    cmd = util.AWS_PREFIX + [
        'docdb',
        'create-db-subnet-group',
        '--region',
        self.region,
        '--db-subnet-group-name',
        self.subnet_group_name,
        '--db-subnet-group-description',
        '"PKB subnet"',
        '--subnet-ids',
        subnet_id,
    ]
    # Subnets determine where instances can be placed.
    regional_network = self._GetClientVm().network.regional_network
    vpc_id = regional_network.vpc.id
    for zone in self.zones:
      cidr = regional_network.vpc.NextSubnetCidrBlock()
      subnet = aws_network.AwsSubnet(zone, vpc_id, cidr_block=cidr)
      subnet.Create()
      cmd += [subnet.id]
      self.subnets.append(subnet)

    cmd += ['--tags']
    cmd += util.MakeFormattedDefaultTags()

    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'docdb',
        'delete-db-subnet-group',
        '--region=%s' % self.region,
        '--db-subnet-group-name=%s' % self.subnet_group_name,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

    for subnet in self.subnets:
      subnet.Delete()

  def _GetClientVm(self):
    """Conveniently returns the client VM to use for the instance."""
    return self._client_vms[0]

  def AddNode(self) -> None:
    """Adds a node to the documentdb cluster."""
    self.replica_count += 1
    cmd = util.AWS_PREFIX + [
        'docdb',
        'create-db-instance',
        '--db-instance-identifier', f'{self.name}-instance-{self.replica_count}',
        '--db-cluster-identifier', self.name,
        '--db-instance-class', self.db_instance_class,
        '--engine', 'docdb',
        '--availability-zone', self.zones[0],
        '--region', self.region,
        '--tags',
    ] + util.MakeFormattedDefaultTags()  # pyformat: disable
    vm_util.IssueCommand(cmd)

  def _Create(self) -> None:
    common_params = [
        '--db-cluster-identifier', self.name,
        '--db-subnet-group-name', self.subnet_group_name,
        '--engine', 'docdb',
        '--storage-type', self.storage_type,
        '--no-deletion-protection',
        '--region', self.region,
    ]  # pyformat: disable
    if self.snapshot:
      cmd = util.AWS_PREFIX + [
          'docdb',
          'restore-db-cluster-from-snapshot',
          '--snapshot-identifier', self.snapshot,
      ] + common_params  # pyformat: disable
    else:
      cmd = util.AWS_PREFIX + [
          'docdb',
          'create-db-cluster',
          '--master-username', 'test',
          '--master-user-password', 'testtest',
          '--storage-encrypted',
      ] + common_params  # pyformat: disable
    if self.tls_enabled:
      cmd += [
          '--db-cluster-parameter-group-name',
          'default.docdb5.0',
      ]
    else:
      cmd += ['--db-cluster-parameter-group-name', 'nontls']
    cmd += [
        '--tags'
    ] + util.MakeFormattedDefaultTags()  # pyformat: disable
    vm_util.IssueCommand(cmd)

    for replica_index in range(self.replica_count + 1):
      cmd = util.AWS_PREFIX + [
          'docdb',
          'create-db-instance',
          '--db-instance-identifier',
          f'{self.name}-instance-{replica_index}',
          '--db-cluster-identifier',
          self.name,
          '--db-instance-class',
          self.db_instance_class,
          '--engine',
          'docdb',
          '--availability-zone',
          self.zones[replica_index % len(self.zones)],
          '--region',
          self.region,
          '--tags',
      ] + util.MakeFormattedDefaultTags()  # pyformat: disable
      vm_util.IssueCommand(cmd)

  def SetupClientTls(self):
    background_tasks.RunThreaded(SetupClientTls, self._client_vms)

  def _Delete(self) -> None:
    """Deletes the documentdb cluster."""
    describe_cluster = self._DescribeCluster()
    if not describe_cluster:
      return
    instances = [
        instance['DBInstanceIdentifier']
        for instance in describe_cluster['DBClusterMembers']
    ]
    for instance in instances:
      cmd = util.AWS_PREFIX + [
          'docdb',
          'delete-db-instance',
          '--db-instance-identifier',
          instance,
          '--region',
          self.region,
      ]
      vm_util.IssueCommand(cmd, raise_on_failure=False)
    cmd = util.AWS_PREFIX + [
        'docdb',
        'delete-db-cluster',
        '--region',
        self.region,
        '--db-cluster-identifier',
        self.name,
        '--skip-final-snapshot',
    ]
    logging.info('Attempting deletion: ')
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _DescribeInstance(self, instance_id: str) -> dict[str, Any]:
    """Calls describe on DocumentDB instance."""
    cmd = util.AWS_PREFIX + [
        'docdb',
        'describe-db-instances',
        '--db-instance-identifier',
        instance_id,
        '--region',
        self.region,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find instance %s, %s', instance_id, stderr)
      return {}
    return json.loads(stdout)['DBInstances'][0]

  def _IsReady(self) -> bool:
    """Check if documentdb cluster is ready."""
    logging.info('Getting cluster ready status for %s', self.name)
    describe_cluster = self._DescribeCluster()
    if describe_cluster['Status'] != 'available':
      return False
    instances = [
        instance['DBInstanceIdentifier']
        for instance in describe_cluster['DBClusterMembers']
    ]
    for instance in instances:
      if self._DescribeInstance(instance)['DBInstanceStatus'] != 'available':
        return False
    return True

  def _Exists(self) -> bool:
    """Returns true if the documentdb cluster exists."""
    logging.info('Checking if cluster %s exists', self.name)
    result = self._DescribeCluster()
    if not result:
      return False
    if result['Status'] == 'deleting':
      return False
    if not self.resource_arn:
      self.resource_arn = result['DBClusterArn']
    return True

  def _DescribeCluster(self) -> dict[Any, Any]:
    """Calls describe on DocumentDB cluster."""
    cmd = util.AWS_PREFIX + [
        'docdb',
        'describe-db-clusters',
        '--region',
        self.region,
        '--db-cluster-identifier',
        self.name,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find cluster %s, %s', self.name, stderr)
      return {}
    output = json.loads(stdout)['DBClusters'][0]
    self.endpoint = output['Endpoint']
    self.port = output['Port']
    return output

  def GetResourceMetadata(self) -> dict[Any, Any]:
    """Returns a dict containing metadata about the documentdb instance.

    Returns:
      dict mapping string property key to value.
    """
    metadata = super().GetResourceMetadata()
    metadata.update({
        'aws_documentdb_name': self.name,
        'aws_documentdb_zones': self.zones,
        'aws_documentdb_db_instance_class': self.db_instance_class,
        'aws_documentdb_region': self.region,
        'aws_documentdb_storage_encryption': self.storage_encryption,
        'aws_documentdb_storage_type': self.storage_type,
        'aws_documentdb_replica_count': self.replica_count,
    })
    return metadata

  def GetConnectionString(self) -> str:
    """Returns the connection string used to connect to the instance."""
    return (
        f'mongodb://test:testtest@{self.endpoint}:{self.port}/ycsb'
        '?replicaSet=rs0'
        '&retryWrites=false'
        '&maxPoolSize=500'
        f'&tls={str(self.tls_enabled).lower()}'
    )

  def GetJvmTrustStoreArgs(self) -> str:
    return (
        '-Djavax.net.ssl.trustStore=/tmp/certs/rds-truststore.jks '
        '-Djavax.net.ssl.trustStorePassword=testtest'
    )


def SetupClientTls(vm: virtual_machine.BaseVirtualMachine):
  """See base class."""
  vm.Install('openjdk')
  vm.PushDataFile('tls_setup.sh')
  vm.RemoteCommand('chmod +x ~/tls_setup.sh')
  vm.RemoteCommand('~/tls_setup.sh')
