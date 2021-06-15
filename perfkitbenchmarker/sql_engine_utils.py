# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities to support multiple engines."""

MYSQL = 'mysql'
POSTGRES = 'postgres'
AURORA_POSTGRES = 'aurora-postgresql'
AURORA_MYSQL = 'aurora-mysql'
AURORA_MYSQL56 = 'aurora'
SQLSERVER = 'sqlserver'
SQLSERVER_EXPRESS = 'sqlserver-ex'
SQLSERVER_ENTERPRISE = 'sqlserver-ee'
SQLSERVER_STANDARD = 'sqlserver-se'

ALL_ENGINES = [
    MYSQL,
    POSTGRES,
    AURORA_POSTGRES,
    AURORA_MYSQL,
    AURORA_MYSQL56,
    SQLSERVER,
    SQLSERVER_EXPRESS,
    SQLSERVER_ENTERPRISE,
    SQLSERVER_STANDARD
]

ENGINE_TYPES = [
    MYSQL,
    POSTGRES,
    SQLSERVER
]

AWS_SQLSERVER_ENGINES = ['sqlserver-ee', 'sqlserver-se',
                         'sqlserver-ex', 'sqlserver-web']

AWS_AURORA_POSTGRES_ENGINE = 'aurora-postgresql'
AWS_AURORA_MYSQL_ENGINE = 'aurora-mysql'


# Helper functions for this module
def GetDbEngineType(db_engine: str) -> str:
  """Converts the engine type from db_engine.

  The same engine type can have multiple names because of differences
  in cloud provider or versions.

  Args:
    db_engine: db_engine defined in the spec

  Returns:
    Engine type in string.
  """

  # AWS uses sqlserver-se and sqlserver-ex as db_egine for sql server
  if db_engine in AWS_SQLSERVER_ENGINES:
    return SQLSERVER
  elif db_engine == AWS_AURORA_POSTGRES_ENGINE:
    return POSTGRES
  elif db_engine == AWS_AURORA_MYSQL_ENGINE:
    return MYSQL

  if db_engine not in ENGINE_TYPES:
    raise TypeError('Unsupported engine type', db_engine)
  return db_engine

