# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS' dynamodb tables.

Tables can be created and deleted.
"""

import json
import logging
from typing import Any, Collection, Dict, List, Optional, Sequence, Tuple

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import util


# Billing Modes
_ON_DEMAND = 'PAY_PER_REQUEST'
_PROVISIONED = 'PROVISIONED'

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'aws_dynamodb_primarykey',
    None,
    'The primaryKey of dynamodb table. This switches to sortkey if using sort.'
    'If testing GSI/LSI, use the range keyname of the index you want to test.'
    'Defaults to primary_key',
)
flags.DEFINE_boolean(
    'aws_dynamodb_use_sort',
    None,
    'Determine whether to use sort key or not. Defaults to False.',
)
flags.DEFINE_string(
    'aws_dynamodb_sortkey',
    None,
    'The sortkey of dynamodb table. This switches to primarykey if using sort.'
    'If testing GSI/LSI, use the primary keyname of the index you want to test.'
    'Defaults to sort_key.',
)
flags.DEFINE_enum(
    'aws_dynamodb_attributetype',
    None,
    ['S', 'N', 'B'],
    'The type of attribute, default to S (String).'
    'Alternates are N (Number) and B (Binary).'
    'Defaults to S.',
)
_BILLING_MODE = flags.DEFINE_enum(
    'aws_dynamodb_billing_mode',
    None,
    [_ON_DEMAND, _PROVISIONED],
    (
        'DynamoDB billing mode. "provisioned": Uses'
        ' --aws_dynamodb_read_capacity and --aws_dynamodb_write_capacity to'
        ' determine provisioned throughput. "ondemand": DynamoDB will autoscale'
        ' capacity and disregard provisioned capacity flags for RCU and WCU.'
        ' Note that this is different from using provisioned capacity with an'
        ' autoscaling policy.'
    ),
)
flags.DEFINE_integer(
    'aws_dynamodb_read_capacity',
    None,
    'Set RCU for dynamodb table. Defaults to 25.',
)
flags.DEFINE_integer(
    'aws_dynamodb_write_capacity',
    None,
    'Set WCU for dynamodb table. Defaults to 25.',
)
flags.DEFINE_integer(
    'aws_dynamodb_lsi_count',
    None,
    'Set amount of Local Secondary Indexes. Only set 0-5.Defaults to 0.',
)
flags.DEFINE_integer(
    'aws_dynamodb_gsi_count',
    None,
    'Set amount of Global Secondary Indexes. Only set 0-5.Defaults to 0.',
)
# For info on autoscaling parameters, see
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.html
_AUTOSCALING_MAX_WCU = flags.DEFINE_integer(
    'aws_dynamodb_autoscaling_wcu_max', None, 'Maximum WCU for autoscaling.'
)
_AUTOSCALING_MAX_RCU = flags.DEFINE_integer(
    'aws_dynamodb_autoscaling_rcu_max', None, 'Maximum RCU for autoscaling.'
)
_AUTOSCALING_CPU_TARGET = flags.DEFINE_integer(
    'aws_dynamodb_autoscaling_target',
    None,
    'The target utilization percent for autoscaling.',
)

# Throughput constants
_FREE_TIER_RCU = 25
_FREE_TIER_WCU = 25

_DEFAULT_ZONE = 'us-east-1b'

# For autoscaling CLI parameters, see
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AutoScaling.CLI.html
_WCU_SCALABLE_DIMENSION = 'dynamodb:table:WriteCapacityUnits'
_RCU_SCALABLE_DIMENSION = 'dynamodb:table:ReadCapacityUnits'

_WCU_SCALING_METRIC = 'DynamoDBWriteCapacityUtilization'
_RCU_SCALING_METRIC = 'DynamoDBReadCapacityUtilization'

_DEFAULT_SCALE_OUT_COOLDOWN = 0
_DEFAULT_SCALE_IN_COOLDOWN = 0
_DEFAULT_AUTOSCALING_TARGET = 65.0


class DynamoDbSpec(non_relational_db.BaseNonRelationalDbSpec):
  """Configurable options of a DynamoDB instance."""

  SERVICE_TYPE = non_relational_db.DYNAMODB

  table_name: str
  billing_mode: str
  zone: str
  rcu: int
  wcu: int
  primary_key: str
  sort_key: str
  attribute_type: str
  lsi_count: int
  gsi_count: int
  use_sort: bool

  def __init__(self, component_full_name, flag_values, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes / constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    none_ok = {'default': None, 'none_ok': False}
    result.update({
        'table_name': (option_decoders.StringDecoder, none_ok),
        'billing_mode': (option_decoders.StringDecoder, none_ok),
        'zone': (option_decoders.StringDecoder, none_ok),
        'rcu': (option_decoders.IntDecoder, none_ok),
        'wcu': (option_decoders.IntDecoder, none_ok),
        'primary_key': (option_decoders.StringDecoder, none_ok),
        'sort_key': (option_decoders.StringDecoder, none_ok),
        'attribute_type': (option_decoders.StringDecoder, none_ok),
        'lsi_count': (option_decoders.IntDecoder, none_ok),
        'gsi_count': (option_decoders.IntDecoder, none_ok),
        'use_sort': (option_decoders.BooleanDecoder, none_ok),
    })
    return result

  @classmethod
  def _ValidateConfig(cls, config_values) -> None:
    if 'lsi_count' in config_values:
      if not -1 < config_values['lsi_count'] < 6:
        raise errors.Config.InvalidValue('lsi_count must be from 0-5')
      if (
          not config_values.get('use_sort', False)
          and config_values['lsi_count'] != 0
      ):
        raise errors.Config.InvalidValue('lsi_count requires use_sort=True')
    if not -1 < config_values.get('gsi_count', 0) < 6:
      raise errors.Config.InvalidValue('gsi_count must be from 0-5')
    if (
        any(x in config_values for x in ['rcu', 'wcu'])
        and config_values.get('billing_mode') == _ON_DEMAND
    ):
      raise errors.Config.InvalidValue(
          'Billing mode set to ON_DEMAND but read capacity and/or write'
          ' capacity were set. Please set mode to "provisioned" or unset the'
          ' provisioned capacity flags.'
      )

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
        'aws_dynamodb_billing_mode': 'billing_mode',
        'aws_dynamodb_read_capacity': 'rcu',
        'aws_dynamodb_write_capacity': 'wcu',
        'aws_dynamodb_primarykey': 'primary_key',
        'aws_dynamodb_sortkey': 'sort_key',
        'aws_dynamodb_attributetype': 'attribute_type',
        'aws_dynamodb_lsi_count': 'lsi_count',
        'aws_dynamodb_gsi_count': 'gsi_count',
        'aws_dynamodb_use_sort': 'use_sort',
    }
    for flag_name, option_name in option_name_from_flag.items():
      if flag_values[flag_name].present:
        config_values[option_name] = flag_values[flag_name].value

    # Handle the zone flag.
    for zone_flag_name in ['zone', 'zones']:
      if flag_values[zone_flag_name].present:
        config_values['zone'] = flag_values[zone_flag_name].value[0]

    cls._ValidateConfig(config_values)

  def __repr__(self) -> str:
    return str(self.__dict__)


class AwsDynamoDBInstance(non_relational_db.BaseNonRelationalDb):
  """Class for working with DynamoDB."""

  SERVICE_TYPE = non_relational_db.DYNAMODB

  def __init__(
      self,
      table_name: Optional[str] = None,
      billing_mode: Optional[str] = None,
      zone: Optional[str] = None,
      rcu: Optional[int] = None,
      wcu: Optional[int] = None,
      primary_key: Optional[str] = None,
      sort_key: Optional[str] = None,
      attribute_type: Optional[str] = None,
      lsi_count: Optional[int] = None,
      gsi_count: Optional[int] = None,
      use_sort: Optional[bool] = None,
      **kwargs,
  ):
    super(AwsDynamoDBInstance, self).__init__(**kwargs)
    self.table_name = table_name or f'pkb-{FLAGS.run_uri}'
    self._resource_id = f'table/{self.table_name}'
    self.billing_mode = billing_mode or _PROVISIONED
    self.zone = zone or _DEFAULT_ZONE
    self.region = util.GetRegionFromZone(self.zone)
    self.resource_arn: str = None  # Set during the _Exists() call.

    self.rcu = rcu or _FREE_TIER_RCU
    self.wcu = wcu or _FREE_TIER_WCU
    self.throughput = (
        f'ReadCapacityUnits={self.rcu},WriteCapacityUnits={self.wcu}'
    )

    self.primary_key = primary_key or 'primary_key'
    self.sort_key = sort_key or 'sort_key'
    self.use_sort = use_sort or False
    self.attribute_type = attribute_type or 'S'

    self.lsi_count = lsi_count or 0
    self.lsi_indexes = self._CreateLocalSecondaryIndex()
    self.gsi_count = gsi_count or 0
    self.gsi_indexes = self._CreateGlobalSecondaryIndex()

  @classmethod
  def FromSpec(cls, spec: DynamoDbSpec) -> 'AwsDynamoDBInstance':
    return cls(
        table_name=spec.table_name,
        billing_mode=spec.billing_mode,
        zone=spec.zone,
        rcu=spec.rcu,
        wcu=spec.wcu,
        primary_key=spec.primary_key,
        sort_key=spec.sort_key,
        attribute_type=spec.attribute_type,
        lsi_count=spec.lsi_count,
        gsi_count=spec.gsi_count,
        use_sort=spec.use_sort,
        enable_freeze_restore=spec.enable_freeze_restore,
        create_on_restore_error=spec.create_on_restore_error,
        delete_on_freeze_error=spec.delete_on_freeze_error,
    )

  def _CreateLocalSecondaryIndex(self) -> List[str]:
    """Used to create local secondary indexes."""
    lsi_items = []
    lsi_entry = []
    attr_list = []
    for lsi in range(0, self.lsi_count):
      lsi_item = json.dumps({
          'IndexName': f'lsiidx{str(lsi)}',
          'KeySchema': [
              {'AttributeName': self.primary_key, 'KeyType': 'HASH'},
              {'AttributeName': f'lattr{str(lsi)}', 'KeyType': 'RANGE'},
          ],
          'Projection': {'ProjectionType': 'KEYS_ONLY'},
      })
      lsi_entry.append(lsi_item)
      attr_list.append(
          json.dumps({
              'AttributeName': f'lattr{str(lsi)}',
              'AttributeType': self.attribute_type,
          })
      )
    lsi_items.append('[' + ','.join(lsi_entry) + ']')
    lsi_items.append(','.join(attr_list))
    return lsi_items

  def _CreateGlobalSecondaryIndex(self) -> List[str]:
    """Used to create global secondary indexes."""
    gsi_items = []
    gsi_entry = []
    attr_list = []
    for gsi in range(0, self.gsi_count):
      gsi_item = json.dumps({
          'IndexName': f'gsiidx{str(gsi)}',
          'KeySchema': [
              {'AttributeName': f'gsikey{str(gsi)}', 'KeyType': 'HASH'},
              {'AttributeName': f'gattr{str(gsi)}', 'KeyType': 'RANGE'},
          ],
          'Projection': {'ProjectionType': 'KEYS_ONLY'},
          'ProvisionedThroughput': {
              'ReadCapacityUnits': 5,
              'WriteCapacityUnits': 5,
          },
      })
      gsi_entry.append(gsi_item)
      attr_list.append(
          json.dumps({
              'AttributeName': f'gattr{str(gsi)}',
              'AttributeType': self.attribute_type,
          })
      )
      attr_list.append(
          json.dumps({
              'AttributeName': f'gsikey{str(gsi)}',
              'AttributeType': self.attribute_type,
          })
      )
    gsi_items.append('[' + ','.join(gsi_entry) + ']')
    gsi_items.append(','.join(attr_list))
    return gsi_items

  def _SetAttrDefnArgs(self, cmd: List[str], args: Sequence[str]) -> None:
    attr_def_args = _MakeArgs(args)
    cmd[10] = f'[{attr_def_args}]'
    logging.info('adding to --attribute-definitions')

  def _SetKeySchemaArgs(self, cmd: List[str], args: Sequence[str]) -> None:
    key_schema_args = _MakeArgs(args)
    cmd[12] = f'[{key_schema_args}]'
    logging.info('adding to --key-schema')

  def _PrimaryKeyJson(self) -> str:
    return json.dumps({'AttributeName': self.primary_key, 'KeyType': 'HASH'})

  def _PrimaryAttrsJson(self) -> str:
    return json.dumps({
        'AttributeName': self.primary_key,
        'AttributeType': self.attribute_type,
    })

  def _SortAttrsJson(self) -> str:
    return json.dumps(
        {'AttributeName': self.sort_key, 'AttributeType': self.attribute_type}
    )

  def _SortKeyJson(self) -> str:
    return json.dumps({'AttributeName': self.sort_key, 'KeyType': 'RANGE'})

  def _Create(self) -> None:
    """Creates the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'create-table',
        '--region', self.region,
        '--table-name', self.table_name,
        '--attribute-definitions', self._PrimaryAttrsJson(),
        '--key-schema', self._PrimaryKeyJson(),
        '--billing-mode', self.billing_mode,
        '--tags'
    ] + util.MakeFormattedDefaultTags()  # pyformat: disable
    if self.billing_mode == _PROVISIONED:
      cmd.extend(['--provisioned-throughput', self.throughput])
    if self.lsi_count > 0 and self.use_sort:
      self._SetAttrDefnArgs(
          cmd,
          [
              self._PrimaryAttrsJson(),
              self._SortAttrsJson(),
              self.lsi_indexes[1],
          ],
      )
      cmd.append('--local-secondary-indexes')
      cmd.append(self.lsi_indexes[0])
      self._SetKeySchemaArgs(cmd, [self._PrimaryKeyJson(), self._SortKeyJson()])
    elif self.use_sort:
      self._SetAttrDefnArgs(
          cmd, [self._PrimaryAttrsJson(), self._SortAttrsJson()]
      )
      self._SetKeySchemaArgs(cmd, [self._PrimaryKeyJson(), self._SortKeyJson()])
    if self.gsi_count > 0:
      self._SetAttrDefnArgs(
          cmd, cmd[10].strip('[]').split(',') + [self.gsi_indexes[1]]
      )
      cmd.append('--global-secondary-indexes')
      cmd.append(self.gsi_indexes[0])
    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warning('Failed to create table! %s', stderror)

  def IsAutoscaling(self) -> bool:
    """Returns whether this instance uses (or should use) an autoscaling policy."""
    return all([
        _AUTOSCALING_CPU_TARGET.value,
        _AUTOSCALING_MAX_RCU.value,
        _AUTOSCALING_MAX_WCU.value,
    ])

  def IsServerless(self) -> bool:
    """Returns whether this instance uses autoscaling or on-demand."""
    return self.IsAutoscaling() or self.billing_mode == _ON_DEMAND

  def _PostCreate(self):
    if not self.IsAutoscaling():
      return
    self._CreateAutoscalingPolicy(
        _RCU_SCALABLE_DIMENSION,
        _RCU_SCALING_METRIC,
        self.rcu,
        _AUTOSCALING_MAX_RCU.value,
    )
    self._CreateAutoscalingPolicy(
        _WCU_SCALABLE_DIMENSION,
        _WCU_SCALING_METRIC,
        self.wcu,
        _AUTOSCALING_MAX_WCU.value,
    )

  def _CreateScalableTarget(
      self, scalable_dimension: str, min_capacity: int, max_capacity: int
  ) -> None:
    """Creates a scalable target which controls QPS limits for the table."""
    logging.info('Registering scalable target.')
    cmd = util.AWS_PREFIX + [
        'application-autoscaling',
        'register-scalable-target',
        '--service-namespace',
        'dynamodb',
        '--resource-id',
        self._resource_id,
        '--scalable-dimension',
        scalable_dimension,
        '--min-capacity',
        str(min_capacity),
        '--max-capacity',
        str(max_capacity),
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=True)

  def _CreateAutoscalingPolicy(
      self,
      scalable_dimension: str,
      scaling_metric: str,
      min_capacity: int,
      max_capacity: int,
  ) -> None:
    """Creates an autoscaling policy for the table."""
    self._CreateScalableTarget(scalable_dimension, min_capacity, max_capacity)
    logging.info('Creating autoscaling policy.')
    scaling_policy = {
        'PredefinedMetricSpecification': {
            'PredefinedMetricType': scaling_metric
        },
        'ScaleOutCooldown': _DEFAULT_SCALE_OUT_COOLDOWN,
        'ScaleInCooldown': _DEFAULT_SCALE_IN_COOLDOWN,
        'TargetValue': _AUTOSCALING_CPU_TARGET.value,
    }
    cmd = util.AWS_PREFIX + [
        'application-autoscaling',
        'put-scaling-policy',
        '--service-namespace',
        'dynamodb',
        '--resource-id',
        self._resource_id,
        '--scalable-dimension',
        scalable_dimension,
        '--policy-name',
        f'{FLAGS.run_uri}-policy',
        '--policy-type',
        'TargetTrackingScaling',
        '--target-tracking-scaling-policy-configuration',
        json.dumps(scaling_policy),
        '--region',
        self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=True)

  def _HasAutoscalingPolicies(self) -> bool:
    """Returns whether autoscaling policies are in effect for this table."""
    cmd = util.AWS_PREFIX + [
        'application-autoscaling',
        'describe-scaling-policies',
        '--service-namespace',
        'dynamodb',
        '--resource-id',
        self._resource_id,
        '--region',
        self.region,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd, raise_on_failure=True)
    result = json.loads(stdout).get('ScalingPolicies', [])
    return bool(result)

  def _Delete(self) -> None:
    """Deletes the dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'delete-table',
        '--region',
        self.region,
        '--table-name',
        self.table_name,
    ]
    logging.info('Attempting deletion: ')
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsReady(self) -> bool:
    """Check if dynamodb table is ready."""
    logging.info('Getting table ready status for %s', self.table_name)
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region',
        self.region,
        '--table-name',
        self.table_name,
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    result = json.loads(stdout)
    return result['Table']['TableStatus'] == 'ACTIVE'

  def _Exists(self) -> bool:
    """Returns true if the dynamodb table exists."""
    logging.info('Checking if table %s exists', self.table_name)
    result = self._DescribeTable()
    if not result:
      return False
    if not self.resource_arn:
      self.resource_arn = result['TableArn']
    return True

  def _DescribeTable(self) -> Dict[Any, Any]:
    """Calls describe on dynamodb table."""
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'describe-table',
        '--region',
        self.region,
        '--table-name',
        self.table_name,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find table %s, %s', self.table_name, stderr)
      return {}
    return json.loads(stdout)['Table']

  def GetEndPoint(self) -> str:
    return f'http://dynamodb.{self.region}.amazonaws.com'

  def GetResourceMetadata(self) -> Dict[str, Any]:
    """Returns a dict containing metadata about the dynamodb instance.

    Returns:
      dict mapping string property key to value.
    """
    metadata = {
        'aws_dynamodb_billing_mode': self.billing_mode,
        'aws_dynamodb_primarykey': self.primary_key,
        'aws_dynamodb_use_sort': self.use_sort,
        'aws_dynamodb_sortkey': self.sort_key,
        'aws_dynamodb_attributetype': self.attribute_type,
        'aws_dynamodb_read_capacity': self.rcu,
        'aws_dynamodb_write_capacity': self.wcu,
        'aws_dynamodb_lsi_count': self.lsi_count,
        'aws_dynamodb_gsi_count': self.gsi_count,
    }
    if self.IsAutoscaling():
      metadata.update({
          'aws_dynamodb_autoscaling': True,
          'aws_dynamodb_autoscaling_rcu_min_capacity': self.rcu,
          'aws_dynamodb_autoscaling_rcu_max_capacity': (
              _AUTOSCALING_MAX_RCU.value or self.rcu
          ),
          'aws_dynamodb_autoscaling_wcu_min_capacity': self.wcu,
          'aws_dynamodb_autoscaling_wcu_max_capacity': (
              _AUTOSCALING_MAX_WCU.value or self.wcu
          ),
          'aws_dynamodb_autoscaling_scale_in_cooldown': (
              _DEFAULT_SCALE_IN_COOLDOWN
          ),
          'aws_dynamodb_autoscaling_scale_out_cooldown': (
              _DEFAULT_SCALE_OUT_COOLDOWN
          ),
          'aws_dynamodb_autoscaling_target': _DEFAULT_AUTOSCALING_TARGET,
      })
    return metadata

  def SetThroughput(
      self, rcu: Optional[int] = None, wcu: Optional[int] = None
  ) -> None:
    """Updates the table's rcu and wcu."""
    if not rcu:
      rcu = self.rcu
    if not wcu:
      wcu = self.wcu
    current_rcu, current_wcu = self._GetThroughput()
    if rcu == current_rcu and wcu == current_wcu:
      logging.info('Table already has requested rcu %s and wcu %s', rcu, wcu)
      return
    cmd = util.AWS_PREFIX + [
        'dynamodb',
        'update-table',
        '--table-name',
        self.table_name,
        '--region',
        self.region,
        '--provisioned-throughput',
        f'ReadCapacityUnits={rcu},WriteCapacityUnits={wcu}',
    ]
    logging.info(
        'Setting %s table provisioned throughput to %s rcu and %s wcu',
        self.table_name,
        rcu,
        wcu,
    )
    util.IssueRetryableCommand(cmd)
    while not self._IsReady():
      continue

  def _GetThroughput(self) -> Tuple[int, int]:
    """Returns the current (rcu, wcu) of the table."""
    output = self._DescribeTable()['ProvisionedThroughput']
    return output['ReadCapacityUnits'], output['WriteCapacityUnits']

  @vm_util.Retry(
      poll_interval=1,
      max_retries=3,
      retryable_exceptions=(errors.Resource.CreationError),
  )
  def _GetTagResourceCommand(self, tags: Sequence[str]) -> Sequence[str]:
    """Returns the tag-resource command with the provided tags.

    This function will retry up to max_retries to allow for instance creation to
    finish.

    Args:
      tags: List of formatted tags to append to the instance.

    Returns:
      A list of arguments for the 'tag-resource' command.

    Raises:
      errors.Resource.CreationError: If the current instance does not exist.
    """
    if not self._Exists():
      raise errors.Resource.CreationError(
          f'Cannot get resource arn of non-existent instance {self.table_name}'
      )
    return (
        util.AWS_PREFIX
        + [
            'dynamodb',
            'tag-resource',
            '--resource-arn',
            self.resource_arn,
            '--region',
            self.region,
            '--tags',
        ]
        + list(tags)
    )

  def UpdateWithDefaultTags(self) -> None:
    """Adds default tags to the table."""
    tags = util.MakeFormattedDefaultTags()
    cmd = self._GetTagResourceCommand(tags)
    logging.info('Setting default tags on table %s', self.table_name)
    util.IssueRetryableCommand(cmd)

  def UpdateTimeout(self, timeout_minutes: int) -> None:
    """Updates the timeout associated with the table."""
    tags = util.MakeFormattedDefaultTags(timeout_minutes)
    cmd = self._GetTagResourceCommand(tags)
    logging.info(
        'Updating timeout tags on table %s with timeout minutes %s',
        self.table_name,
        timeout_minutes,
    )
    util.IssueRetryableCommand(cmd)

  def _Freeze(self) -> None:
    """See base class.

    Lowers provisioned throughput to free-tier levels. There is a limit to how
    many times throughput on a table may by lowered per day. See:
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughput.html.
    """
    if self._HasAutoscalingPolicies():
      self._CreateScalableTarget(
          _RCU_SCALABLE_DIMENSION, _FREE_TIER_RCU, _AUTOSCALING_MAX_RCU.value
      )
      self._CreateScalableTarget(
          _WCU_SCALABLE_DIMENSION, _FREE_TIER_WCU, _AUTOSCALING_MAX_WCU.value
      )
      return
    if self.billing_mode == _ON_DEMAND:
      return
    # Check that we actually need to lower before issuing command.
    rcu, wcu = self._GetThroughput()
    if rcu > _FREE_TIER_RCU or wcu > _FREE_TIER_WCU:
      logging.info('(rcu=%s, wcu=%s) is higher than free tier.', rcu, wcu)
      self.SetThroughput(rcu=_FREE_TIER_RCU, wcu=_FREE_TIER_WCU)

  def _Restore(self) -> None:
    """See base class.

    Restores provisioned throughput back to benchmarking levels.
    """
    # Providing flag overrides the capacity used in the previous run.
    if FLAGS['aws_dynamodb_read_capacity'].present:
      self.rcu = FLAGS.aws_dynamodb_read_capacity
    if FLAGS['aws_dynamodb_write_capacity'].present:
      self.wcu = FLAGS.aws_dynamodb_write_capacity
    if self._HasAutoscalingPolicies():
      # If the flags are not provided, this should default to previous levels.
      self._CreateScalableTarget(
          _RCU_SCALABLE_DIMENSION, self.rcu, _AUTOSCALING_MAX_RCU.value
      )
      self._CreateScalableTarget(
          _WCU_SCALABLE_DIMENSION, self.wcu, _AUTOSCALING_MAX_WCU.value
      )
      return
    if self.billing_mode == _ON_DEMAND:
      return
    self.SetThroughput(self.rcu, self.wcu)


def _MakeArgs(args: Collection[str]) -> str:
  return ','.join(args)
