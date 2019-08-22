# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for AWS' elasticache memcached clusters."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import managed_memory_store
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util


MEMCACHED_VERSION = '1.5.10'
FLAGS = flags.FLAGS


class ElastiCacheMemcached(managed_memory_store.BaseManagedMemoryStore):
  """Object representing a AWS Elasticache memcached instance."""

  CLOUD = providers.AWS
  MEMORY_STORE = managed_memory_store.MEMCACHED

  def __init__(self, spec):
    super(ElastiCacheMemcached, self).__init__(spec)
    self.subnet_group_name = 'subnet-%s' % self.name
    self.zone = self.spec.vms[0].zone
    self.region = util.GetRegionFromZone(self.zone)
    self.node_type = FLAGS.cache_node_type

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    if FLAGS.managed_memory_store_version:
      raise errors.Config.InvalidValue('Custom Memcached version not '
                                       'supported. Version is {}.'.format(
                                           MEMCACHED_VERSION))

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the cache cluster.

    Returns:
      dict mapping string property key to value.
    """
    result = {
        'cloud_memcached_version': MEMCACHED_VERSION,
        'cloud_memcached_node_type': self.node_type,
    }
    return result

  def _CreateDependencies(self):
    """Create the subnet dependencies."""
    subnet_id = self.spec.vms[0].network.subnet.id
    cmd = ['aws', 'elasticache', 'create-cache-subnet-group',
           '--region', self.region,
           '--cache-subnet-group-name', self.subnet_group_name,
           '--cache-subnet-group-description', '"memcached benchmark subnet"',
           '--subnet-ids', subnet_id]

    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    """Delete the subnet dependencies."""
    cmd = ['aws', 'elasticache', 'delete-cache-subnet-group',
           '--region', self.region,
           '--cache-subnet-group-name', self.subnet_group_name]
    vm_util.IssueCommand(cmd)

  def _Create(self):
    """Creates the cache cluster."""
    cmd = ['aws', 'elasticache', 'create-cache-cluster',
           '--engine', 'memcached',
           '--region', self.region,
           '--cache-cluster-id', self.name,
           '--preferred-availability-zone', self.zone,
           '--num-cache-nodes', str(managed_memory_store.MEMCACHED_NODE_COUNT),
           '--engine-version', MEMCACHED_VERSION,
           '--cache-node-type', self.node_type,
           '--cache-subnet-group-name', self.subnet_group_name]

    cmd += ['--tags']
    cmd += util.MakeFormattedDefaultTags()
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the cache cluster."""
    cmd = ['aws', 'elasticache', 'delete-cache-cluster',
           '--region', self.region,
           '--cache-cluster-id', self.name]
    vm_util.IssueCommand(cmd)

  def _IsDeleting(self):
    """Returns True if cluster is being deleted and false otherwise."""
    cluster_info = self._DescribeInstance()
    return cluster_info.get('CacheClusterStatus', '') == 'deleting'

  def _IsReady(self):
    """Returns True if cluster is ready and false otherwise."""
    cluster_info = self._DescribeInstance()
    return cluster_info.get('CacheClusterStatus', '') == 'available'

  def _Exists(self):
    """Returns true if the cluster exists and is not being deleted."""
    cluster_info = self._DescribeInstance()
    return cluster_info.get('CacheClusterStatus', '') not in [
        '', 'deleting', 'create-failed']

  def _DescribeInstance(self):
    """Calls describe on cluster.

    Returns:
      dict mapping string cluster_info property key to value.
    """
    cmd = ['aws', 'elasticache', 'describe-cache-clusters',
           '--region', self.region,
           '--cache-cluster-id', self.name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.info('Could not find cluster %s, %s', self.name, stderr)
      return {}
    for cluster_info in json.loads(stdout)['CacheClusters']:
      if cluster_info['CacheClusterId'] == self.name:
        return cluster_info
    return {}

  @vm_util.Retry(max_retries=5)
  def _PopulateEndpoint(self):
    """Populates address and port information from cluster_info.

    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on cluster
    """
    cluster_info = self._DescribeInstance()
    if not cluster_info:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on {0}.'.format(self.name))

    endpoint = cluster_info['ConfigurationEndpoint']
    self._ip = endpoint['Address']
    self._port = endpoint['Port']
