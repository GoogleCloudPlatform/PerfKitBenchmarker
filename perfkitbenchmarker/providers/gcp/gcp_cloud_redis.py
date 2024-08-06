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
"""Module containing class for GCP's cloud redis instances.

Instances can be created and deleted.
"""
import json
import logging
import os
import time
from typing import Any

from absl import flags
from google.cloud import monitoring_v3
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
STANDARD_TIER = 'STANDARD'
BASIC_TIER = 'BASIC'
# TODO(user): Put timeout back to 10 minutes after experiment.
COMMAND_TIMEOUT = 1200  # 20 minutes
DEFAULT_PORT = 6379

_SHARD_SIZE_GB = 13


class CloudRedis(managed_memory_store.BaseManagedMemoryStore):
  """Object representing a GCP cloud redis instance."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'memorystore'
  MEMORY_STORE = managed_memory_store.REDIS

  def __init__(self, spec):
    super().__init__(spec)
    self.project = FLAGS.project
    self.size = gcp_flags.REDIS_GB.value
    if self._clustered:
      self.size = self.node_count * _SHARD_SIZE_GB
    self.redis_region = FLAGS.cloud_redis_region
    self.redis_version = spec.config.cloud_redis.redis_version
    self.tier = self._GetTier()
    self.network = (
        'default'
        if not gcp_flags.GCE_NETWORK_NAMES.value
        else gcp_flags.GCE_NETWORK_NAMES.value[0]
    )
    self.subnet = (
        'default'
        if not gcp_flags.GCE_SUBNET_NAMES.value
        else gcp_flags.GCE_SUBNET_NAMES.value[0]
    )
    # Update the environment for gcloud commands:
    os.environ['CLOUDSDK_API_ENDPOINT_OVERRIDES_REDIS'] = (
        gcp_flags.CLOUD_REDIS_API_OVERRIDE.value
    )

  def _GetTier(self) -> str | None:
    """Returns the tier of the instance."""
    # See https://cloud.google.com/memorystore/docs/redis/redis-tiers."""
    if self._clustered:
      return None
    if self.failover_style == managed_memory_store.Failover.FAILOVER_NONE:
      return BASIC_TIER
    return STANDARD_TIER

  def CheckPrerequisites(self):
    if (
        FLAGS.redis_failover_style
        == managed_memory_store.Failover.FAILOVER_SAME_ZONE
    ):
      raise errors.Config.InvalidValue(
          'GCP cloud redis does not support same zone failover'
      )
    if (
        FLAGS.managed_memory_store_version
        and FLAGS.managed_memory_store_version
        not in managed_memory_store.REDIS_VERSIONS
    ):
      raise errors.Config.InvalidValue('Invalid Redis version.')

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    self.metadata.update({
        'cloud_redis_failover_style': self.failover_style,
        'cloud_redis_size': self.size,
        'cloud_redis_region': self.redis_region,
        'cloud_redis_version': self.ParseReadableVersion(self.redis_version),
    })
    if self.tier is not None:
      self.metadata['cloud_redis_tier'] = self.tier
    return self.metadata

  @staticmethod
  def ParseReadableVersion(version):
    """Parses Redis major and minor version number."""
    if version.count('_') < 2:
      logging.info(
          (
              'Could not parse version string correctly, '
              'full Redis version returned: %s'
          ),
          version,
      )
      return version
    return '.'.join(version.split('_')[1:])

  def _CreateServiceConnectionPolicy(self) -> None:
    """Creates a service connection policy for the VPC."""
    cmd = util.GcloudCommand(
        self,
        'network-connectivity',
        'service-connection-policies',
        'create',
        f'pkb-{FLAGS.run_uri}-policy',
    )
    cmd.flags['service-class'] = 'gcp-memorystore-redis'
    cmd.flags['network'] = self.network
    cmd.flags['region'] = self.redis_region
    cmd.flags['subnets'] = (
        'https://www.googleapis.com/compute/v1'
        f'/projects/{self.project}'
        f'/regions/{self.redis_region}/subnetworks/{self.subnet}'
    )
    cmd.Issue(raise_on_failure=False)

  def _GetClusterCreateCommand(self) -> util.GcloudCommand:
    """Returns the command used to create the cluster."""
    cmd = util.GcloudCommand(self, 'redis', 'clusters', 'create', self.name)
    cmd.use_alpha_gcloud = True
    # Add labels when supported
    cmd.flags['shard-count'] = self.node_count
    cmd.flags['network'] = (
        f'projects/{self.project}/global/networks/{self.network}'
    )
    if self.enable_tls:
      cmd.flags['transit-encryption-mode'] = 'server-authentication'
    return cmd

  def _GetCreateCommand(self) -> util.GcloudCommand:
    """Returns the command used to create the instance."""
    cmd = util.GcloudCommand(self, 'redis', 'instances', 'create', self.name)
    cmd.flags['zone'] = FLAGS.zone[0]
    cmd.flags['network'] = self.network
    cmd.flags['tier'] = self.tier
    cmd.flags['size'] = self.size
    cmd.flags['redis-version'] = self.redis_version
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    return cmd

  def _Create(self):
    """Creates the instance."""
    cmd = self._GetCreateCommand()
    if self._clustered:
      self._CreateServiceConnectionPolicy()
      cmd = self._GetClusterCreateCommand()
    cmd.flags['region'] = self.redis_region
    cmd.Issue(timeout=COMMAND_TIMEOUT)

  def _IsReady(self):
    """Returns whether cluster is ready."""
    instance_details, _, _ = self.DescribeInstance()
    if self._clustered:
      return json.loads(instance_details).get('state') == 'ACTIVE'
    return json.loads(instance_details).get('state') == 'READY'

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'redis', 'instances', 'delete', self.name)
    if self._clustered:
      cmd = util.GcloudCommand(self, 'redis', 'clusters', 'delete', self.name)
      cmd.use_alpha_gcloud = True
    cmd.flags['region'] = self.redis_region
    cmd.Issue(timeout=COMMAND_TIMEOUT, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the instance exists."""
    _, _, retcode = self.DescribeInstance()
    return retcode == 0

  def DescribeInstance(self):
    """Calls describe instance using the gcloud tool.

    Returns:
      stdout, stderr, and retcode.
    """
    cmd = util.GcloudCommand(self, 'redis', 'instances', 'describe', self.name)
    if self._clustered:
      cmd = util.GcloudCommand(self, 'redis', 'clusters', 'describe', self.name)
      cmd.use_alpha_gcloud = True
    cmd.flags['region'] = self.redis_region
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find redis instance %s', self.name)
    return stdout, stderr, retcode

  @vm_util.Retry(max_retries=5)
  def _PopulateEndpoint(self):
    """Populates endpoint information about the instance.

    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on instance
    """
    stdout, _, retcode = self.DescribeInstance()
    if retcode != 0:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on {}'.format(self.name)
      )
    if self._clustered:
      self._ip = json.loads(stdout)['discoveryEndpoints'][0]['address']
      self._port = 6379
      return
    self._ip = json.loads(stdout)['host']
    self._port = json.loads(stdout)['port']

  def MeasureCpuUtilization(self, interval_length):  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
    """Measure the average CPU utilization on GCP instance in percentage."""
    now = time.time()
    seconds = int(now)
    interval = monitoring_v3.TimeInterval()
    interval.end_time.seconds = seconds
    interval.start_time.seconds = seconds - interval_length
    client = monitoring_v3.MetricServiceClient()

    api_filter = (
        'metric.type = "redis.googleapis.com/stats/cpu_utilization" '
        'AND resource.labels.instance_id = "projects/'
        + self.project
        + '/locations/'
        + self.redis_region
        + '/instances/'
        + self.name
        + '"'
    )

    time_series = client.list_time_series(
        name='projects/' + self.project,
        filter_=api_filter,
        interval=interval,
        view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    )

    return self._ParseMonitoringTimeSeries(time_series)

  def _ParseMonitoringTimeSeries(self, time_series):
    """Parses time series data and returns average CPU across intervals in %.

    For example, an interval of 3 minutes would be represented as [x, y, z],
    where x, y, and z are cpu seconds.
    average CPU usage per minute in cpu seconds = (x + y + z) / 3
    average cpu usage in percentage = [(x + y + z) / 3] / 60

    Args:
      time_series: time series of cpu seconds returned by monitoring.

    Returns:
      Percentage CPU use.
    """
    intervals = []
    # For each of the four types of load, sum the CPU across all intervals
    for i, time_interval in enumerate(time_series):
      for j, interval in enumerate(time_interval.points):
        if i == 0:
          intervals.append(interval.value.double_value)
        else:
          intervals[j] += interval.value.double_value

    if intervals:
      # Average over all minute intervals captured
      averaged = sum(intervals) / len(intervals)
      # averaged is in the unit of cpu seconds per minute.
      # So divide by 60sec in 1 min to get a percentage usage over the minute.
      return averaged / 60
    return None

  def GetInstanceSize(self):
    """Return the size of the GCP instance in gigabytes."""
    return self.size
