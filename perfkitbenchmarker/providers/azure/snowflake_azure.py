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
"""Module containing class for Snowflake EDW service resource hosted on Azure.

Setting constants and relevant variables to correctly configure Snowflake on
Azure. Relevant functionality is in providers/snowflake/Snowflake.
"""

from absl import flags
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.providers.snowflake import snowflake

FLAGS = flags.FLAGS
# Filename for JDBC client associated with this provider:
# 'snowflake-jdbc-client-azure-external-2.0.jar'


class Snowflake(snowflake.Snowflake):
  """Class representing a Snowflake Data Warehouse Instance hosted on Azure."""

  CLOUD = provider_info.AZURE
  SERVICE_TYPE = 'snowflake_azure'


class Snowflakeexternal(snowflake.Snowflake):
  """Class representing Snowflake External Warehouses on Azure."""

  CLOUD = provider_info.AZURE
  SERVICE_TYPE = 'snowflakeexternal_azure'
