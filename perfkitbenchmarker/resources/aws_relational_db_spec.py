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
"""Spec for AWS Relational DB."""

from perfkitbenchmarker import errors
from perfkitbenchmarker import relational_db_spec
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.configs import option_decoders


class AwsRelationalDbSpec(relational_db_spec.RelationalDbSpec):
  """Configurable options of an AWS Relational DB."""

  ENGINE = [
      sql_engine_utils.MYSQL,
      sql_engine_utils.POSTGRES,
      sql_engine_utils.MARIADB,
      sql_engine_utils.SQLSERVER_EXPRESS,
      sql_engine_utils.SQLSERVER_STANDARD,
      sql_engine_utils.SQLSERVER_ENTERPRISE,
      sql_engine_utils.AURORA_MYSQL,
      sql_engine_utils.AURORA_POSTGRES,
  ]
  CLOUD = 'AWS'

  aws_rds_dedicated_log_volume: bool

  def __init__(self, component_full_name, flag_values=None, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    if self.aws_rds_dedicated_log_volume and self.engine not in [
        sql_engine_utils.MYSQL,
        sql_engine_utils.POSTGRES,
        sql_engine_utils.MARIADB,
    ]:
      raise errors.Config.InvalidValue(
          'Dedicated log volume is only supported for MySQL, Postgres, and '
          'MariaDB engines.'
      )
    if (
        self.aws_rds_dedicated_log_volume
        and self.db_disk_spec
        and self.db_disk_spec.disk_type not in ['io1', 'io2']
    ):
      raise errors.Config.InvalidValue(
          'Dedicated log volume is only supported with io1 or io2 disk types.'
      )

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'aws_rds_dedicated_log_volume': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
    })
    return result

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['aws_rds_dedicated_log_volume'].present:
      config_values['aws_rds_dedicated_log_volume'] = (
          flag_values.aws_rds_dedicated_log_volume
      )
