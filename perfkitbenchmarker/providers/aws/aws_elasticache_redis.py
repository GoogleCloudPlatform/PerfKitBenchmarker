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
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util

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
    self.redis_region = FLAGS.redis_region
    self.failover_zone = FLAGS.aws_elasticache_failover_zone
    self.failover_subnet = None

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    if FLAGS.redis_failover_style in [cloud_redis.Failover.FAILOVER_NONE,
                                      cloud_redis.Failover.FAILOVER_SAME_ZONE]:
      if FLAGS.aws_elasticache_failover_zone:
        raise errors.Config.InvalidValue(
            'The aws_elasticache_failover_zone flag is ignored. '
            'There is no need for a failover zone when there is no failover. '
            'Same zone failover will fail over to the same zone.')
    else:
      if (not FLAGS.aws_elasticache_failover_zone or
          FLAGS.aws_elasticache_failover_zone[:-1] != FLAGS.redis_region):
        raise errors.Config.InvalidValue(
            'Invalid failover zone. '
            'A failover zone in %s must be specified. ' % FLAGS.redis_region)

  def GetResourceMetadata(self):
    """Returns a dict containing metadata about the instance.

    Returns:
      dict mapping string property key to value.
    """
    result = super(ElastiCacheRedis, self).GetResourceMetadata()
    result['version'] = self.version
    result['node_type'] = self.node_type
    result['region'] = self.redis_region
    result['primary_zone'] = self.spec.client_vm.zone
    result['failover_zone'] = self.failover_zone
    return result

  def _CreateDependencies(self):
    """Create the subnet dependencies."""
    subnet_id = self.spec.client_vm.network.subnet.id
    cmd = ['aws', 'elasticache', 'create-cache-subnet-group',
           '--region', self.redis_region,
           '--cache-subnet-group-name', self.subnet_group_name,
           '--cache-subnet-group-description', '"PKB redis benchmark subnet"',
           '--subnet-ids', subnet_id]

    if self.failover_style == cloud_redis.Failover.FAILOVER_SAME_REGION:
      regional_network = self.spec.client_vm.network.regional_network
      vpc_id = regional_network.vpc.id
      cidr = regional_network.vpc.NextSubnetCidrBlock()
      self.failover_subnet = aws_network.AwsSubnet(
          self.failover_zone, vpc_id, cidr_block=cidr)
      self.failover_subnet.Create()
      cmd += [self.failover_subnet.id]

    vm_util.IssueCommand(cmd)

  def _DeleteDependencies(self):
    """Delete the subnet dependencies."""
    cmd = ['aws', 'elasticache', 'delete-cache-subnet-group',
           '--region=%s' % self.redis_region,
           '--cache-subnet-group-name=%s' % self.subnet_group_name]
    vm_util.IssueCommand(cmd)

    if self.failover_subnet:
      self.failover_subnet.Delete()

  def _Create(self):
    """Creates the cluster."""
    cmd = ['aws', 'elasticache', 'create-replication-group',
           '--engine', 'redis',
           '--engine-version', self.version,
           '--replication-group-id', self.cluster_name,
           '--replication-group-description', self.cluster_name,
           '--region', self.redis_region,
           '--cache-node-type', self.node_type,
           '--cache-subnet-group-name', self.subnet_group_name,
           '--preferred-cache-cluster-a-zs', self.spec.client_vm.zone]

    if self.failover_style == cloud_redis.Failover.FAILOVER_SAME_REGION:
      cmd += [self.failover_zone]

    elif self.failover_style == cloud_redis.Failover.FAILOVER_SAME_ZONE:
      cmd += [self.spec.client_vm.zone]

    if self.failover_style != cloud_redis.Failover.FAILOVER_NONE:
      cmd += ['--automatic-failover-enabled',
              '--num-cache-clusters', '2']

    cmd += ['--tags']
    cmd += util.MakeFormattedDefaultTags()
    vm_util.IssueCommand(cmd)

  def _Delete(self):
    """Deletes the cluster."""
    cmd = ['aws', 'elasticache', 'delete-replication-group',
           '--region', self.redis_region,
           '--replication-group-id', self.cluster_name]
    vm_util.IssueCommand(cmd)

  def _IsDeleting(self):
    """Returns True if cluster is being deleted and false otherwise."""
    cluster_info = self.DescribeInstance()
    return cluster_info.get('Status', '') == 'deleting'

  def _IsReady(self):
    """Returns True if cluster is ready and false otherwise."""
    cluster_info = self.DescribeInstance()
    return cluster_info.get('Status', '') == 'available'

  def _Exists(self):
    """Returns true if the cluster exists and is not being deleted."""
    cluster_info = self.DescribeInstance()
    return ('Status' in cluster_info and
            cluster_info['Status'] not in ['deleting', 'create-failed'])

  def DescribeInstance(self):
    """Calls describe on cluster.

    Returns:
      dict mapping string cluster_info property key to value.
    """
    cmd = ['aws', 'elasticache', 'describe-replication-groups',
           '--region', self.redis_region,
           '--replication-group-id', self.cluster_name]
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode != 0:
      logging.info('Could not find cluster %s, %s', self.cluster_name, stderr)
      return {}
    for cluster_info in json.loads(stdout)['ReplicationGroups']:
      if cluster_info['ReplicationGroupId'] == self.cluster_name:
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

    primary_endpoint = cluster_info['NodeGroups'][0]['PrimaryEndpoint']
    cluster_info['host'] = primary_endpoint['Address']
    cluster_info['port'] = primary_endpoint['Port']
    return cluster_info
