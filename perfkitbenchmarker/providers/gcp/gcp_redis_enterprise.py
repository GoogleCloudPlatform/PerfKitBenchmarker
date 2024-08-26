# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP-hosted Redis Cloud.

Redis Cloud is a fully-managed database service that is compatible with Redis.
"""
import dataclasses
import json
import logging
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util
import requests

_KEYFILE = flags.DEFINE_string(
    'redis_cloud_keyfile',
    None,
    'JSON file containing the redis cloud account key and user key, with the'
    ' format {"account_key": "KEY", "user_key": "KEY"}.',
)
_PAYMENT_METHOD_ID = flags.DEFINE_integer(
    'redis_cloud_payment_method_id',
    None,
    'The payment method id to use for the redis instance.',
)
_GB = flags.DEFINE_integer(
    'redis_cloud_gb',
    1,
    'The size of the redis instance in GB.',
)
_OPS_PER_SEC = flags.DEFINE_integer(
    'redis_cloud_ops_per_sec',
    10000,
    'The number of operations per second for the redis instance.',
)

FLAGS = flags.FLAGS

_Json = dict[str, Any]

_REDIS_API_URL = 'https://api.redislabs.com/v1'


@dataclasses.dataclass
class _ShardConfiguration:
  """Class representing info about a Redis Cloud shard configuration."""

  type: str = ''
  dollars_per_hr: float = 0.0
  size_gb: int = 0  # Max dataset size per shard
  quantity: int = 0  # Not including replicas


class RetryableShardInfoError(Exception):
  """Exception for retryable errors when getting shard info."""


class GcpRedisEnterprise(managed_memory_store.BaseManagedMemoryStore):
  """Object representing a managed Redis Enterprise instance running on GCP."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'redis_enterprise'
  MEMORY_STORE = managed_memory_store.REDIS

  def __init__(self, spec):
    super().__init__(spec)
    self.name = f'pkb-{FLAGS.run_uri}'
    self.project = FLAGS.project or util.GetDefaultProject()
    self.redis_region = FLAGS.cloud_redis_region
    # Network is required in order to enable VPC peering
    bm_spec: benchmark_spec.BenchmarkSpec = context.GetThreadBenchmarkSpec()
    # pytype: disable=attribute-error
    self.network = (
        bm_spec.vms[0].network.network_resource.name
        if not gcp_flags.GCE_NETWORK_NAMES.value
        else gcp_flags.GCE_NETWORK_NAMES.value[0]
    )
    # pytype: enable=attribute-error
    self.subscription_id = ''
    self.database_id = ''
    self.redis_version = ''
    self.shard_info: _ShardConfiguration = None
    self.peering_name = f'pkb-redis-cloud-peering-{FLAGS.run_uri}'

    self._request_headers = None

    if self.replicas_per_shard > 1:
      raise errors.Config.InvalidValue(
          'Redis Cloud supports at most 1 replica per shard.'
      )

  @property
  def request_headers(self) -> dict[str, Any]:
    if self._request_headers is not None:
      return self._request_headers
    cmd = util.GcloudCommand(self, 'storage', 'cat', _KEYFILE.value)
    cmd.flags['format'] = 'none'
    stdout, _, _ = cmd.Issue()
    keyfile_json = json.loads(stdout)
    self._request_headers = {
        'accept': 'application/json',
        'x-api-key': keyfile_json['account_key'],
        'x-api-secret-key': keyfile_json['user_key'],
    }
    return self._request_headers

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    self.metadata.update({
        'redis_cloud_shard_type': self.shard_info.type,
        'redis_cloud_shard_dollars_per_hr': self.shard_info.dollars_per_hr,
        'redis_cloud_shard_size_gb': self.shard_info.size_gb,
        'cloud_redis_region': self.redis_region,
        'shard_count': self.shard_count,
        'replicas_per_shard': self.replicas_per_shard,
        'node_count': self.node_count,
    })
    return self.metadata

  def _CreateVpcPeeringRequest(self):
    """Creates a VPC peering request for the Redis Enterprise instance."""
    payload = {
        'provider': 'GCP',
        'vpcProjectUid': self.project,
        'vpcNetworkName': self.network,
    }
    logging.info('Create peering request payload: %s', payload)
    result = requests.post(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/peerings',
        headers=self.request_headers,
        json=payload,
    )
    logging.info('Create peering request response: %s', result.text)
    self._WaitForTaskCompletion(result.json()['taskId'])

    result = requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/peerings',
        headers=self.request_headers,
    )
    task = self._WaitForTaskCompletion(result.json()['taskId'])
    peering = task['response']['resource']['peerings'][0]

    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'peerings', 'create', self.peering_name
    )
    cmd.flags['project'] = self.project
    cmd.flags['network'] = self.network
    cmd.flags['peer-network'] = peering['redisNetworkName']
    cmd.flags['peer-project'] = peering['redisProjectUid']
    cmd.Issue()

  def _GetTask(self, task_id: str) -> dict[str, Any]:
    """Returns the API task with the given task ID."""
    logging.info('Getting task: %s', task_id)
    result = requests.get(
        f'{_REDIS_API_URL}/tasks/{task_id}',
        headers=self.request_headers,
    )
    logging.info('Get task response: %s', result.text)
    return result.json()

  def _WaitForTaskCompletion(
      self, task_id: str, wait_until_status: str | None = None
  ) -> _Json:
    """Waits for the API task with the given task ID to complete."""
    logging.info('Waiting for task %s to finish', task_id)
    while True:
      task = self._GetTask(task_id)
      status = task.get('status', None)
      if (
          wait_until_status is not None and status == wait_until_status
      ) or status == 'processing-completed':
        logging.info('Task completed with response: %s', json.dumps(task))
        return task
      time.sleep(10)

  def _GetCreateArgs(self) -> _Json:
    """Returns the payload to use for creating the instance."""
    region_config = {
        'region': self.redis_region,
        'multipleAvailabilityZones': self.multi_az,
        'networking': {'deploymentCIDR': '192.168.0.0/24'},
    }
    if self.zones:
      region_config['preferredAvailabilityZones'] = self.zones
    return {
        'name': self.name,
        'deploymentType': 'single-region',
        'paymentMethodId': _PAYMENT_METHOD_ID.value,
        'cloudProviders': [{
            'provider': 'GCP',
            'regions': [region_config],
        }],
        'databases': [{
            'name': f'pkb-{FLAGS.run_uri}',
            'protocol': 'redis',
            'datasetSizeInGb': _GB.value,
            'supportOSSClusterApi': self._clustered,
            'dataPersistence': 'none',
            'replication': self.replicas_per_shard > 0,
            'throughputMeasurement': {
                'by': 'operations-per-second',
                'value': _OPS_PER_SEC.value,
            },
            'quantity': 1,
        }],
    }

  def _Create(self):
    """Creates the instance."""
    result = requests.post(
        f'{_REDIS_API_URL}/subscriptions',
        headers=self.request_headers,
        json=self._GetCreateArgs(),
    )
    logging.info('Create subscription response: %s', result.text)
    task = self._GetTask(result.json()['taskId'])
    expected_statuses = ['processing-in-progress', 'received']
    if task.get('status', None) not in expected_statuses:
      # Note that this is not retryable in favor of failing fast.
      raise errors.Resource.CreationError(
          f'Failed to create subscription: expected status {expected_statuses},'
          f' got {task.get("status", None)}'
      )
    task = self._WaitForTaskCompletion(task['taskId'])
    self.subscription_id = task['response']['resourceId']
    self.database_id = requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases',
        headers=self.request_headers,
    ).json()['subscription'][0]['databases'][0]['databaseId']
    logging.info(
        'Created subscription %s with database %s',
        self.subscription_id,
        self.database_id,
    )
    self._AddTags()

  def _AddTags(self) -> None:
    """Adds tags to the instance."""
    for key, value in util.GetDefaultTags().items():
      result = requests.post(
          f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases/{self.database_id}/tags',
          headers=self.request_headers,
          json={'key': key, 'value': value},
      )
      if result.status_code != 200:
        raise errors.Resource.CreationError(
            f'Failed to add tag {key} to database {self.name}. Response:'
            f' {result.json()}'
        )

  def _EnableTls(self) -> None:
    """Enables TLS on the instance."""
    if not FLAGS.cloud_redis_tls:
      return
    result = requests.put(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases/{self.database_id}',
        headers=self.request_headers,
        json={'enableTls': True},
    )
    self._WaitForTaskCompletion(result.json()['taskId'])

  def _PostCreate(self):
    """Runs post create steps."""
    self._CreateVpcPeeringRequest()
    self._WaitForSubscriptionToIncludePricing()
    self._EnableTls()
    subscription = self._GetSubscription()
    database = self._GetDatabase()
    logging.info(
        'Subscription: %s\nDatabase: %s',
        json.dumps(subscription),
        json.dumps(database),
    )
    self.redis_version = database['redisVersionCompliance']
    self.shard_info = self._GetShardType(subscription, database)
    self.shard_count = self.shard_info.quantity
    self.node_count = self._GetNodeCount()

  def _GetDatabase(self):
    """Returns the database associated with the subscription."""
    return requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases/{self.database_id}',
        headers=self.request_headers,
    ).json()

  def _GetSubscription(self):
    """Returns the subscription associated with the subscription."""
    return requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}',
        headers=self.request_headers,
    ).json()

  @vm_util.Retry(
      max_retries=20, retryable_exceptions=(RetryableShardInfoError,)
  )
  def _WaitForSubscriptionToIncludePricing(self):
    """Waits for the subscription to include pricing information."""
    subscription = self._GetSubscription()
    for pricing_detail in subscription['subscriptionPricing']:
      if pricing_detail['type'] == 'Shards':
        logging.info(
            'Subscription has pricing information %s',
            json.dumps(subscription, indent=2),
        )
        return
    raise RetryableShardInfoError(
        'Failed to find shard configuration in subscription:'
        f' {json.dumps(subscription)}'
    )

  def _GetShardType(
      self, subscription_json: _Json, database_json: _Json
  ) -> _ShardConfiguration:
    """Returns the shard type of the subscription."""
    shard = _ShardConfiguration()
    pricing_details = subscription_json['subscriptionPricing']
    for pricing_detail in pricing_details:
      if pricing_detail['type'] == 'Shards':
        shard.type = pricing_detail['typeDetails']
        shard.dollars_per_hr = pricing_detail['pricePerUnit']
        shard.quantity = int(
            pricing_detail['quantity'] / (1 + self.replicas_per_shard)
        )
        shard.size_gb = database_json['datasetSizeInGb'] / shard.quantity
        return shard
    raise errors.Benchmarks.RunError(
        'Failed to find shard configuration in subscription:'
        f' {json.dumps(subscription_json)}'
    )

  def _WaitUntilCompleteOrSubscriptionDeleted(self, result: _Json) -> None:
    """Waits until the task is complete or the subscription is deleted."""
    delete_task = self._WaitForTaskCompletion(
        result['taskId'], wait_until_status='processing-error'
    )
    if delete_task['status'] == 'processing-error' and delete_task['response'][
        'error'
    ]['type'] not in ['SUBSCRIPTION_NOT_ACTIVE', 'SUBSCRIPTION_NOT_FOUND']:
      raise errors.Resource.CleanupError(f'Failed to delete: {delete_task}')

  def _DeleteDatabase(self):
    """Deletes the database associated with the subscription."""
    result = requests.delete(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases/{self.database_id}',
        headers=self.request_headers,
    )
    logging.info('Attempting to delete database: %s', result.text)
    self._WaitUntilCompleteOrSubscriptionDeleted(result.json())

  def _DeleteSubscription(self):
    """Deletes the subscription."""
    result = requests.delete(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}',
        headers=self.request_headers,
    )
    logging.info('Attempting to delete subscription: %s', result.text)
    self._WaitUntilCompleteOrSubscriptionDeleted(result.json())

  def _DeletePeering(self):
    """Deletes the VPC peering."""
    cmd = util.GcloudCommand(
        self, 'compute', 'networks', 'peerings', 'delete', self.peering_name
    )
    cmd.flags['project'] = self.project
    cmd.flags['network'] = self.network
    cmd.Issue(raise_on_failure=False)

  def _Delete(self):
    """Deletes the instance."""
    self._DeleteDatabase()
    self._DeleteSubscription()
    self._DeletePeering()
    logging.info('Finished deleting Redis Cloud subscription, DB, and peering.')

  def _Exists(self):
    """Returns true if the instance exists."""
    if not self.subscription_id or not self.database_id:
      return False
    result = requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}',
        headers=self.request_headers,
    )
    logging.info('Checking if subscription exists: %s', result.text)
    if result.status_code == 200:
      return True
    return False

  def _IsReady(self) -> bool:
    """Returns true if the instance is ready."""
    if not self.subscription_id or not self.database_id:
      return False
    result = requests.get(
        f'{_REDIS_API_URL}/subscriptions/{self.subscription_id}/databases/{self.database_id}',
        headers=self.request_headers,
    )
    status = result.json()['status']
    logging.info('Checking if database is ready, status: %s', status)
    if status == 'active':
      return True
    return False

  def _PopulateEndpoint(self):
    """Populates endpoint information about the instance.

    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on instance
    """
    private_endpoint = self._GetDatabase()['privateEndpoint']
    logging.info('Database private endpoint: %s', private_endpoint)
    self._ip = private_endpoint.split(':')[0]
    self._port = private_endpoint.split(':')[1]

  def GetMemoryStorePassword(self) -> str:
    """Returns the access password of the managed memory store, if any."""
    if not self._password:
      self._password = self._GetDatabase()['security']['password']
      logging.info('Database password: %s', self._password)
    return self._password
