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
"""Module containing class for Snowflake EDW service resource hosted on AWS.

Setting constants and relevant variables to correctly configure Snowflake on
AWS. Relevant functionality is in providers/snowflake/Snowflake.
"""

from absl import flags
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.providers.snowflake import snowflake

FLAGS = flags.FLAGS
# Filename for JDBC client associated with this provider:
# snowflake-jdbc-client-2.5-standard.jar


class Snowflake(snowflake.Snowflake):
  """Class representing a Snowflake Data Warehouse Instance hosted on AWS."""
  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'snowflake_aws'


class Snowflakeexternal(snowflake.Snowflake):
  """Class representing Snowflake External Warehouses on AWS."""
  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'snowflakeexternal_aws'
