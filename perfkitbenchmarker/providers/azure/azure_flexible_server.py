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
"""Azure Flexible server provisioning and teardown.

Postgres Flexible Server is created with PremiumV2_LRS storage type.

MySQL Flexible Server is created with provisioned IOPS unless omitted, in which
case it is created with auto-scale IOPS.
"""

import datetime
import json
import logging
import time
from typing import Any, Tuple

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_relational_db
from perfkitbenchmarker.providers.azure import util

DEFAULT_DATABASE_NAME = 'database'

FLAGS = flags.FLAGS

DISABLE_HA = 'Disabled'
ENABLE_HA = 'ZoneRedundant'

DEFAULT_MYSQL_VERSION = '8.0'
DEFAULT_POSTGRES_VERSION = '17'

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

  Note that the client VM's region and the region requested for the database
  must be the same.
  """

  SERVER_TYPE = 'flexible-server'
  CLOUD = provider_info.AZURE
  ENGINE = [
      sql_engine_utils.FLEXIBLE_SERVER_POSTGRES,
      sql_engine_utils.FLEXIBLE_SERVER_MYSQL,
  ]
  # Metrics are processed in 5 minute batches according to
  # https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-monitoring.
  METRICS_COLLECTION_DELAY_SECONDS = 300

  def __init__(self, relational_db_spec: Any):
    super().__init__(relational_db_spec)
    if (
        self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL
        and self.spec.db_disk_spec.provisioned_throughput
    ):
      raise errors.Config.InvalidValue(
          'Provisioned throughput is not supported for MySQL Flexible Server.'
      )
    self.storage_type = None
    if self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_POSTGRES:
      self.storage_type = (
          self.spec.db_disk_spec.disk_type or azure_disk.PREMIUM_STORAGE
      )
      if self.storage_type == azure_disk.PREMIUM_STORAGE_V2:
        raise errors.Config.InvalidValue(
            'Premium Storage v2 is not supported for Postgres Flexible Server.'
        )

  @staticmethod
  def GetDefaultEngineVersion(engine: str) -> str:
    """Returns the default version of a given database engine."""
    if engine == sql_engine_utils.POSTGRES:
      return DEFAULT_POSTGRES_VERSION
    elif engine == sql_engine_utils.MYSQL:
      return DEFAULT_MYSQL_VERSION
    else:
      raise relational_db.RelationalDbEngineNotFoundError(
          'Unsupported engine {}'.format(engine)
      )

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    if self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL:
      metadata['autoscale_iops'] = (
          'Enabled'
          if self.spec.db_disk_spec.provisioned_iops is None
          else 'Disabled'
      )
    if self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_POSTGRES:
      metadata['disk_type'] = self.storage_type
    return metadata

  def _Create(self) -> None:
    """Creates the Azure database instance."""
    ha_flag = ENABLE_HA if self.spec.high_availability else DISABLE_HA
    # Consider adding --zone and maybe --standby-zone for parity with GCP.
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
    if self.storage_type:
      cmd.extend([
          '--storage-type',
          self.storage_type,
      ])
    if (
        self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL
        and self.spec.db_disk_spec.provisioned_iops is not None
    ):
      cmd.extend([
          '--auto-scale-iops',
          'Disabled',
      ])
    if self.spec.db_disk_spec.provisioned_iops:
      cmd.extend([
          '--iops',
          str(self.spec.db_disk_spec.provisioned_iops),
      ])
    if self.spec.db_disk_spec.provisioned_throughput:
      cmd.extend([
          '--throughput',
          str(self.spec.db_disk_spec.provisioned_throughput),
      ])

    vm_util.IssueCommand(cmd, timeout=CREATE_AZURE_DB_TIMEOUT)

  def GetAzCommandForEngine(self) -> str:
    engine = self.spec.engine
    if engine == sql_engine_utils.FLEXIBLE_SERVER_POSTGRES:
      return 'postgres'
    elif engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL:
      return 'mysql'
    else:
      raise NotImplementedError('Unsupported engine {}'.format(engine))

  def _PostCreate(self) -> None:
    """Perform general post create operations on the cluster."""
    # Calling the grand parent class.
    super(azure_relational_db.AzureRelationalDb, self)._PostCreate()
    # Get the client VM's ip address
    ip = self.client_vm.ip_address
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
        ip,
        '--end-ip-address',
        ip,
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
    return vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsInstanceReady(self, timeout: int = IS_READY_TIMEOUT) -> bool:
    """See base class."""
    start_time = datetime.datetime.now()

    while True:
      if (datetime.datetime.now() - start_time).seconds >= timeout:
        logging.warning('Timeout waiting for sql instance to be ready')
        return False

      server_show_json = self._AzServerShow()
      if server_show_json is not None:
        state = server_show_json['state']
        if state == 'Ready':
          break
      time.sleep(5)
    return True

  def _ApplyDbFlags(self) -> None:
    """Apply database flags to the instance."""
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
            'Invalid flags: {}.  Error {}'.format(name_and_value, stderr)
        )

    self._Reboot()

  def _GetResourceProvider(self) -> str:
    if self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL:
      return 'Microsoft.DBforMySQL'
    elif self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_POSTGRES:
      return 'Microsoft.DBforPostgreSQL'
    else:
      raise NotImplementedError(f'Unsupported engine {self.spec.engine}')

  def _GetResourceId(self) -> str:
    return (
        f'/subscriptions/{util.GetSubscriptionId()}/resourceGroups/'
        f'{self.resource_group.name}/providers/'
        f'{self._GetResourceProvider()}/flexibleServers/{self.instance_id}'
    )

  def _GetMetricsToCollect(self) -> list[relational_db.MetricSpec]:
    """Returns a list of metrics to collect."""
    # pyformat: disable
    if self.spec.engine == sql_engine_utils.FLEXIBLE_SERVER_MYSQL:
      return [
          relational_db.MetricSpec('cpu_percent', 'cpu_utilization', '%', None),
          relational_db.MetricSpec('io_consumption_percent', 'io_consumption_percent', '%', None),
          relational_db.MetricSpec('storage_io_count', 'storage_io_count', 'iops', None),
          relational_db.MetricSpec('storage_used', 'disk_bytes_used', 'GB', lambda x: x / (1024 * 1024 * 1024)),
      ]
    else:
      return [
          relational_db.MetricSpec('cpu_percent', 'cpu_utilization', '%', None),
          relational_db.MetricSpec('read_iops', 'disk_read_iops', 'iops', None),
          relational_db.MetricSpec('write_iops', 'disk_write_iops', 'iops', None),
          relational_db.MetricSpec('read_throughput', 'disk_read_throughput', 'MB/s', lambda x: x / (1024 * 1024)),
          relational_db.MetricSpec('write_throughput', 'disk_write_throughput', 'MB/s', lambda x: x / (1024 * 1024)),
          relational_db.MetricSpec('storage_used', 'disk_bytes_used', 'GB', lambda x: x / (1024 * 1024 * 1024)),
      ]
    # pyformat: enable

  @vm_util.Retry(poll_interval=60, max_retries=5, retryable_exceptions=KeyError)
  def _CollectProviderMetric(
      self,
      metric: relational_db.MetricSpec,
      start_time: datetime.datetime,
      end_time: datetime.datetime,
      collect_percentiles: bool = False,
  ) -> list[sample.Sample]:
    """Collects metrics from Azure Monitor."""
    if end_time - start_time < datetime.timedelta(minutes=1):
      logging.warning(
          'Not collecting metrics since end time %s is within 1 minute of start'
          ' time %s.',
          end_time,
          start_time,
      )
      return []
    metric_name = metric.provider_name
    logging.info(
        'Collecting metric %s for instance %s', metric_name, self.instance_id
    )
    cmd = [
        azure.AZURE_PATH,
        'monitor',
        'metrics',
        'list',
        '--resource',
        self._GetResourceId(),
        '--metric',
        metric_name,
        '--start-time',
        start_time.astimezone(datetime.timezone.utc).strftime(
            relational_db.METRICS_TIME_FORMAT
        ),
        '--end-time',
        end_time.astimezone(datetime.timezone.utc).strftime(
            relational_db.METRICS_TIME_FORMAT
        ),
        '--interval',
        'pt1m',
        '--aggregation',
        'Average',
    ]
    try:
      stdout, _ = vm_util.IssueRetryableCommand(cmd)
    except errors.VmUtil.IssueCommandError as e:
      logging.warning(
          'Could not collect metric %s for instance %s: %s',
          metric.provider_name,
          self.instance_id,
          e,
      )
      return []
    response = json.loads(stdout)
    if (
        not response
        or not response['value']
        or not response['value'][0]['timeseries']
    ):
      logging.warning('No timeseries for metric %s', metric_name)
      return []

    datapoints = response['value'][0]['timeseries'][0]['data']
    if not datapoints:
      logging.warning('No datapoints for metric %s', metric_name)
      return []

    points = []
    for dp in datapoints:
      if dp['average'] is None:
        continue
      value = dp['average']
      if metric.conversion_func:
        value = metric.conversion_func(value)
      points.append((
          datetime.datetime.fromisoformat(dp['timeStamp']),
          value,
      ))

    return self._CreateSamples(
        points, metric.sample_name, metric.unit, collect_percentiles
    )
