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
"""Module containing class for AWS' elasticache redis clusters.

Clusters can be created and deleted.
"""
import json
import logging
from perfkitbenchmarker import cloud_redis
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
REDIS_VERSION_MAPPING = {'REDIS_3_2': '3.2.10'}


class ElastiCacheRedis(cloud_redis.BaseCloudRedis):
  """Object representing a AWS Elasticache redis instance."""

  CLOUD = providers.AWS

  def __init__(self, spec):
    super(ElastiCacheRedis, self).__init__(spec)
    self.subnet_group_name = 'subnet-%s' % spec.redis_name
    self.cluster_name = 'pkb-%s' % FLAGS.run_uri
    self.version = REDIS_VERSION_MAPPING[spec.redis_version]
    self.node_type = FLAGS.redis_node_type

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    result = super(ElastiCacheRedis, self).GetResourceMetadata()
    result['version'] = self.version
    result['node_type'] = self.node_type
    return result

  def _CreateDependencies(self):
    """Create the subnet dependencies."""
    cmd = ['aws', 'elasticache', 'create-cache-subnet-group',
           '--region=%s' % FLAGS.redis_region,
           '--cache-subnet-group-name=%s' % self.subnet_group_name,
           '--cache-subnet-group-description="PKB redis benchmark subnet"',
           '--subnet-ids=%s' % self.spec.subnet_id]
    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    """Delete the subnet dependencies."""
    cmd = ['aws', 'elasticache', 'delete-cache-subnet-group',
           '--region=%s' % FLAGS.redis_region,
           '--cache-subnet-group-name=%s' % self.subnet_group_name]
    vm_util.IssueCommand(cmd)

  def _Create(self):
    """Creates the cluster."""
    cmd = ['aws', 'elasticache', 'create-cache-cluster',
           '--engine=redis',
           '--num-cache-nodes=1',  # must be 1 for redis clusters
           '--engine-version=%s' % self.version,
           '--region=%s' % FLAGS.redis_region,
           '--cache-cluster-id=%s' % self.cluster_name,
           '--cache-node-type=%s' % self.node_type,
           '--cache-subnet-group-name=%s' % self.subnet_group_name]
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the cluster."""
    cmd = ['aws', 'elasticache', 'delete-cache-cluster',
           '--region=%s' % FLAGS.redis_region,
           '--cache-cluster-id=%s' % self.cluster_name]
    vm_util.IssueCommand(cmd)

  def _IsDeleting(self):
    """Returns True if cluster is being deleted and false otherwise."""
    cluster_info = self.DescribeInstance()
    return cluster_info.get('CacheClusterStatus', '') == 'deleting'

  def _IsReady(self):
    """Returns True if cluster is ready and false otherwise."""
    cluster_info = self.DescribeInstance()
    return cluster_info.get('CacheClusterStatus', '') == 'available'

  def _Exists(self):
    """Returns true if the cluster exists and is not being deleted."""
    cluster_info = self.DescribeInstance()
    return ('CacheClusterStatus' in cluster_info and
            cluster_info['CacheClusterStatus'] != 'deleting')

  def DescribeInstance(self):
    """Calls describe on cluster.

    Returns:
      dict mapping string cluster_info property key to value.
    """
    cmd = ['aws', 'elasticache', 'describe-cache-clusters',
           '--show-cache-node-info',
           '--region=%s' % FLAGS.redis_region,
           '--cache-cluster-id=%s' % self.cluster_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      logging.info('Could not find cluster %s, %s', self.cluster_name, stderr)
      return {}
    for cluster_info in json.loads(stdout)['CacheClusters']:
      if cluster_info['CacheClusterId'] == self.cluster_name:
        return cluster_info
    return {}

  @vm_util.Retry(max_retries=5)
  def GetInstanceDetails(self):
    """Flattens address and port information from cluster_info.

    Returns:
      dict mapping string cluster_info property key to value.
    Raises:
      errors.Resource.RetryableGetError:
      Failed to retrieve information on cluster
    """
    cluster_info = self.DescribeInstance()
    if not cluster_info:
      raise errors.Resource.RetryableGetError(
          'Failed to retrieve information on %s', self.cluster_name)

    if 'ConfigurationEndpoint' in cluster_info:   # multiple nodes
      cluster_info['host'] = cluster_info['ConfigurationEndpoint']['Address']
      cluster_info['port'] = cluster_info['ConfigurationEndpoint']['Port']
    else:
      cache_node = cluster_info['CacheNodes'][0]  # single node
      cluster_info['host'] = cache_node['Endpoint']['Address']
      cluster_info['port'] = cache_node['Endpoint']['Port']
    return cluster_info
