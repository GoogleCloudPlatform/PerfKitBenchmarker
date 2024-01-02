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
"""Relational database provisioning and teardown for Azure RDS."""

import datetime
import logging
import time
from typing import Tuple

from absl import flags
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_relational_db

DEFAULT_DATABASE_NAME = 'database'

FLAGS = flags.FLAGS

DISABLE_HA = 'Disabled'
ENABLE_HA = 'SameZone'

DEFAULT_MYSQL_VERSION = '8.0'
DEFAULT_POSTGRES_VERSION = '13'

IS_READY_TIMEOUT = 60 * 60 * 1  # 1 hour (might take some time to prepare)

# The Azure command timeout with the following error message:
#
# Deployment failed. Correlation ID: fcdc3c76-33cc-4eb1-986c-fbc30ce7d820.
# The operation timed out and automatically rolled back.
# Please retry the operation.
CREATE_AZURE_DB_TIMEOUT = 60 * 120


class AzureFlexibleServer(azure_relational_db.AzureRelationalDb):
  """An object representing an Azure Flexible server.

  Instructions from:
  https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/

  On teardown, all resources are deleted.

  Note that the client VM's region and the region requested for the database
  must be the same.
  """

  SERVER_TYPE = 'flexible-server'
  CLOUD = provider_info.AZURE
  ENGINE = [
      sql_engine_utils.FLEXIBLE_SERVER_POSTGRES,
      sql_engine_utils.FLEXIBLE_SERVER_MYSQL,
  ]

  @staticmethod
  def GetDefaultEngineVersion(engine: str):
    """Returns the default version of a given database engine.

    Args:
      engine: type of database (my_sql or postgres).

    Returns:
      (string): Default engine version.
    Raises:
      RelationalDbEngineNotFoundError: if an unknown engine is
                                                  requested.
    """
    if engine == sql_engine_utils.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif engine == sql_engine_utils.MYSQL:
      return DEFAULT_MYSQL_VERSION
    else:
      raise relational_db.RelationalDbEngineNotFoundError(
          'Unsupported engine {0}'.format(engine)
      )

  def _Create(self):
    """Creates the Azure RDS instance."""
    ha_flag = ENABLE_HA if self.spec.high_availability else DISABLE_HA
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'create',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
        '--location',
        self.region,
        '--high-availability',
        ha_flag,
        '--admin-user',
        self.spec.database_username,
        '--admin-password',
        self.spec.database_password,
        '--storage-size',
        str(self.spec.db_disk_spec.disk_size),
        '--sku-name',
        self.spec.db_spec.machine_type,
        '--tier',
        self.spec.db_tier,
        '--version',
        self.spec.engine_version,
        '--yes',
    ]

    vm_util.IssueCommand(cmd, timeout=CREATE_AZURE_DB_TIMEOUT)

  def GetAzCommandForEngine(self):
    engine = self.spec.engine
    if engine == sql_engine_utils.FLEXIBLE_SERVER_POSTGRES:
      return 'postgres'
    elif engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL:
      return 'mysql'
    else:
      raise NotImplementedError('Unsupported engine {0}'.format(engine))
    return engine

  def _PostCreate(self):
    """Perform general post create operations on the cluster."""
    # Calling the grand parent class.
    super(azure_relational_db.AzureRelationalDb, self)._PostCreate()
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'firewall-rule',
        'create',
        '--resource-group',
        self.resource_group.name,
        '--name',
        self.instance_id,
        '--rule-name',
        'allow-all-ips',
        '--start-ip-address',
        '0.0.0.0',
        '--end-ip-address',
        '255.255.255.255',
    ]
    vm_util.IssueCommand(cmd)

  def SetDbConfiguration(self, name: str, value: str) -> Tuple[str, str, int]:
    """Set configuration for the database instance.

    Args:
        name: string, the name of the settings to change
        value: value, string the value to set

    Returns:
      Tuple of standand output and standard error
    """
    cmd = [
        azure.AZURE_PATH,
        self.GetAzCommandForEngine(),
        self.SERVER_TYPE,
        'parameter',
        'set',
        '--name',
        name,
        '--value',
        value,
        '--resource-group',
        self.resource_group.name,
        '--server',
        self.instance_id,
    ]
    return vm_util.IssueCommand(cmd)

  def _IsInstanceReady(self, timeout=IS_READY_TIMEOUT):
    """Return true if the instance is ready.

    This method will query the instance every 5 seconds until
    its instance state is 'Ready', or until a timeout occurs.

    Args:
      timeout: timeout in seconds

    Returns:
      True if the resource was ready in time, False if the wait timed out
        or an Exception occurred.
    """
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.warning('Timeout waiting for sql instance to be ready')
        return False

      server_show_json = self._AzServerShow()
      state = server_show_json['state']
      if server_show_json is not None:
        if state == 'Ready':
          break
      time.sleep(5)
    return True

  def _ApplyDbFlags(self):
    """Applies the MySqlFlags to a managed instance."""
    for flag in FLAGS.db_flags:
      name_and_value = flag.split('=')
      cmd = [
          azure.AZURE_PATH,
          self.GetAzCommandForEngine(),
          self.SERVER_TYPE,
          'parameter',
          'set',
          '--name',
          name_and_value[0],
          '--resource-group',
          self.resource_group.name,
          '--server-name',
          self.instance_id,
          '--value',
          name_and_value[1],
      ]
      _, stderr, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
      if stderr and 'WARNING' not in stderr:
        # Azure might raise warning
        # WARNING: configuration_name is not a known attribute of class
        raise NotImplementedError(
            'Invalid flags: {0}.  Error {1}'.format(name_and_value, stderr)
        )

    self._Reboot()
