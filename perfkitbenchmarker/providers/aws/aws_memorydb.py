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
"""Module containing class for AWS' MemoryDB redis clusters.

Currently the following configurations are supported:
- Single primary node.
- Single primary node and single replica that provides failover into another
zone in the same region.
"""
import json
import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

_DEFAULT_VERSION = '7.1'

REDIS_VERSION_MAPPING = {
    'redis_7_1': '7.1',
}


class AwsMemoryDb(managed_memory_store.BaseManagedMemoryStore):
  """Object representing a AWS MemoryDB instance."""

  CLOUD = provider_info.AWS
  SERVICE_TYPE = 'memorydb'
  MEMORY_STORE = managed_memory_store.REDIS

  # AWS Clusters can take up to 2 hours to create
  READY_TIMEOUT = 120 * 60

  def __init__(self, spec):
    super().__init__(spec)
    self.subnet_group_name = 'subnet-%s' % self.name
    self.version = REDIS_VERSION_MAPPING[spec.version]
    self.node_type = aws_flags.MEMORYDB_NODE_TYPE.value
    self.redis_region = FLAGS.cloud_redis_region
    self.failover_zone = aws_flags.MEMORYDB_FAILOVER_ZONE.value
    self.failover_subnet = None

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    self.metadata.update({
        'cloud_redis_version': self.version,
        'cloud_redis_node_type': self.node_type,
        'cloud_redis_region': self.redis_region,
        'cloud_redis_primary_zone': self._GetClientVm().zone,
        'cloud_redis_failover_zone': self.failover_zone,
    })
    return self.metadata

  def _CreateDependencies(self) -> None:
    """Create the subnet dependencies."""
    subnet_id = self._GetClientVm().network.subnet.id
    cmd = [
        'aws',
        'memorydb',
        'create-subnet-group',
        '--subnet-group-name',
        self.subnet_group_name,
        '--region',
        self.redis_region,
        '--subnet-ids',
        subnet_id,
    ]

    if self.failover_zone:
      regional_network = self._GetClientVm().network.regional_network
      vpc_id = regional_network.vpc.id
      cidr = regional_network.vpc.NextSubnetCidrBlock()
      self.failover_subnet = aws_network.AwsSubnet(
          self.failover_zone, vpc_id, cidr_block=cidr
      )
      self.failover_subnet.Create()
      cmd += [self.failover_subnet.id]

    cmd += ['--tags']
    cmd += util.MakeFormattedDefaultTags()
    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self) -> None:
    """Delete the subnet dependencies."""
    cmd = [
        'aws',
        'memorydb',
        'delete-subnet-group',
        '--region=%s' % self.redis_region,
        '--subnet-group-name=%s' % self.subnet_group_name,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

    if self.failover_subnet:
      self.failover_subnet.Delete()

  def _Create(self) -> None:
    """Creates the cluster."""
    cmd = [
        'aws',
        'memorydb',
        'create-cluster',
        '--cluster-name',
        self.name,
        '--node-type',
        self.node_type,
        '--engine',
        'redis',
        '--engine-version',
        self.version,
        '--num-shards',
        '1',
        '--num-replicas-per-shard',
        str(self.replicas_per_shard),
        '--parameter-group-name',
        'default.memorydb-redis7.search',
        '--subnet-group-name',
        self.subnet_group_name,
        '--acl-name',
        'open-access',
        '--region',
        self.redis_region,
    ]

    if self.enable_tls:
      cmd += [
          '--tls-enabled',
      ]
    else:
      cmd += [
          '--no-tls-enabled',
      ]

    cmd += ['--tags']
    cmd += util.MakeFormattedDefaultTags()
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Delete(self):
    """Deletes the cluster."""
    cmd = [
        'aws',
        'memorydb',
        'delete-cluster',
        '--cluster-name',
        self.name,
        '--region',
        self.redis_region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsDeleting(self) -> bool:
    """Returns True if cluster is being deleted and false otherwise."""
    cluster_info = self.DescribeCluster()
    return cluster_info.get('Status', '') == 'deleting'

  def _IsReady(self) -> bool:
    """Returns True if cluster is ready and false otherwise."""
    cluster_info = self.DescribeCluster()
    return cluster_info.get('Status', '') == 'available'

  def _Exists(self) -> bool:
    """Returns true if the cluster exists and is not being deleted."""
    cluster_info = self.DescribeCluster()
    return 'Status' in cluster_info and cluster_info['Status'] not in [
        'deleting',
        'create-failed',
    ]

  def DescribeCluster(self) -> dict[str, Any]:
    """Returns the CLI describe output for the cluster."""
    cmd = [
        'aws',
        'memorydb',
        'describe-clusters',
        '--cluster-name',
        self.name,
        '--region',
        self.redis_region,
    ]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find cluster %s, %s', self.name, stderr)
      return {}
    for cluster_info in json.loads(stdout)['Clusters']:
      if cluster_info['Name'] == self.name:
        return cluster_info
    return {}

  @vm_util.Retry(max_retries=5)
  def _PopulateEndpoint(self) -> None:
    """Populates address and port information from cluster_info.

    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on cluster
    """
    cluster_info = self.DescribeCluster()
    if not cluster_info:
      raise errors.Resource.RetryableGetError(
          f'Failed to retrieve information on {self.name}'
      )
    primary_endpoint = cluster_info['ClusterEndpoint']
    self._ip = primary_endpoint['Address']
    self._port = primary_endpoint['Port']
