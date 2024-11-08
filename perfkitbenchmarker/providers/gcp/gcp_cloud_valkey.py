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
"""Module containing class for GCP's Cloud Valkey instances."""
import json
import logging
import os
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS

COMMAND_TIMEOUT = 3600

_DEFAULT_VERSION = 'VALKEY_8_0'
_DEFAULT_ZONE = 'us-central1-c'


class CloudValkey(managed_memory_store.BaseManagedMemoryStore):
  """Object representing a GCP Cloud Memorystore (Valkey) instance."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'memorystore'
  MEMORY_STORE = managed_memory_store.VALKEY

  def __init__(self, spec):
    super().__init__(spec)
    self.project = FLAGS.project
    self.node_type = ''
    self.location = FLAGS.cloud_redis_region
    self.zone_distribution = gcp_flags.REDIS_ZONE_DISTRIBUTION.value
    self.node_type = gcp_flags.REDIS_NODE_TYPE.value
    if self.zone_distribution == 'single-zone':
      zone = FLAGS.zone[0] if FLAGS.zone else _DEFAULT_ZONE
      self.zones = [zone]
    self.version = spec.version or _DEFAULT_VERSION
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
    os.environ['CLOUDSDK_API_ENDPOINT_OVERRIDES_MEMORYSTORE'] = (
        gcp_flags.CLOUD_VALKEY_API_OVERRIDE.value
    )

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    # Metadata uses redis terminology for backwards compatibility.
    self.metadata.update({
        'cloud_redis_region': self.location,
        'cloud_redis_version': self.GetReadableVersion(),
        'cloud_redis_node_type': self.node_type,
        'cloud_redis_zone_distribution': self.zone_distribution,
    })
    return self.metadata

  def GetReadableVersion(self):
    """Parses Valkey major and minor version number."""
    if self.version.count('_') < 2:
      logging.info(
          (
              'Could not parse version string correctly, '
              'full Valkey version returned: %s'
          ),
          self.version,
      )
      return self.version
    return '.'.join(self.version.split('_')[1:])

  def _CreateServiceConnectionPolicy(self) -> None:
    """Creates a service connection policy for the VPC."""
    cmd = util.GcloudCommand(
        self,
        'network-connectivity',
        'service-connection-policies',
        'create',
        f'pkb-{FLAGS.run_uri}-policy',
    )
    cmd.flags['service-class'] = 'gcp-memorystore'
    cmd.flags['network'] = self.network
    cmd.flags['region'] = self.location
    cmd.flags['subnets'] = (
        'https://www.googleapis.com/compute/v1'
        f'/projects/{self.project}'
        f'/regions/{self.location}/subnetworks/{self.subnet}'
    )
    cmd.Issue(raise_on_failure=False)

  def _GetClusterCreateCommand(self) -> util.GcloudCommand:
    """Returns the command used to create the cluster."""
    cmd = util.GcloudCommand(
        self, 'memorystore', 'instances', 'create', self.name
    )
    cmd.flags['location'] = self.location

    network_name = f'projects/{self.project}/global/networks/{self.network}'
    cmd.flags['psc-auto-connections'] = (
        f'network={network_name},projectId={self.project}'
    )

    cmd.flags['shard-count'] = self.shard_count
    cmd.flags['replica-count'] = self.replicas_per_shard
    cmd.flags['node-type'] = self.node_type
    cmd.flags['engine-version'] = self.version

    cmd.flags['zone-distribution-config-mode'] = self.zone_distribution
    if self.zone_distribution == 'single-zone':
      cmd.flags['zone-distribution-config'] = self.zones[0]

    if self.enable_tls:
      cmd.flags['transit-encryption-mode'] = 'server-authentication'

    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    cmd.use_beta_gcloud = True

    return cmd

  def _Create(self):
    """Creates the instance."""
    self._CreateServiceConnectionPolicy()
    cmd = self._GetClusterCreateCommand()
    cmd.Issue(timeout=COMMAND_TIMEOUT)

  def _IsReady(self):
    """Returns whether cluster is ready."""
    instance_details, _, _ = self.DescribeInstance()
    return json.loads(instance_details).get('state') == 'ACTIVE'

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(
        self, 'memorystore', 'instances', 'delete', self.name
    )
    cmd.flags['location'] = self.location
    cmd.use_beta_gcloud = True
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
    cmd = util.GcloudCommand(
        self, 'memorystore', 'instances', 'describe', self.name
    )
    cmd.flags['location'] = self.location
    cmd.use_beta_gcloud = True
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find memorystore instance %s', self.name)
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
    if self.clustered:
      self._ip = json.loads(stdout)['discoveryEndpoints'][0]['address']
      self._port = 6379
      return
    self._ip = json.loads(stdout)['host']
    self._port = json.loads(stdout)['port']
