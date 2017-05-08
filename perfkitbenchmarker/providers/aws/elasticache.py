# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memcached_server
from perfkitbenchmarker.memcache_service import MemcacheService
from perfkitbenchmarker.providers.aws import aws_network


ELASTICACHE_PORT = 11211


class ElastiCacheMemcacheService(MemcacheService):

  CLOUD = providers.AWS

  def __init__(self, network, cluster_id, region, node_type, num_servers=1):
    self.cluster_id = cluster_id
    self.region = region
    self.node_type = node_type
    self.num_servers = num_servers
    self.hosts = []  # [(ip, port)]

    self.vpc_id = network.subnet.vpc_id
    self.security_group_id = \
        network.regional_network.vpc.default_security_group_id
    self.subnet_id = network.subnet.id
    self.subnet_group_name = '%ssubnet' % cluster_id

  def Create(self):
    # Open the port memcached needs
    aws_network.AwsFirewall.GetFirewall() \
        .AllowPortInSecurityGroup(self.region, self.security_group_id,
                                  ELASTICACHE_PORT)

    # Create a cache subnet group
    cmd = ['aws', 'elasticache', 'create-cache-subnet-group',
           '--region=%s' % self.region,
           '--cache-subnet-group-name=%s' % self.subnet_group_name,
           '--cache-subnet-group-description="PKB memcached_ycsb benchmark"',
           '--subnet-ids=%s' % self.subnet_id]
    vm_util.IssueCommand(cmd)

    # Create the cluster
    cmd = ['aws', 'elasticache', 'create-cache-cluster',
           '--engine=memcached',
           '--cache-subnet-group-name=%s' % self.subnet_group_name,
           '--cache-cluster-id=%s' % self.cluster_id,
           '--num-cache-nodes=%s' % self.num_servers,
           '--region=%s' % self.region,
           '--cache-node-type=%s' % self.node_type]
    vm_util.IssueCommand(cmd)

    # Wait for the cluster to come up
    cluster_info = self._WaitForClusterUp()

    # Parse out the hosts
    self.hosts = \
        [(node['Endpoint']['Address'], node['Endpoint']['Port'])
         for node in cluster_info['CacheNodes']]
    assert len(self.hosts) == self.num_servers

  def Destroy(self):
    # Delete the ElastiCache cluster
    cmd = ['aws', 'elasticache', 'delete-cache-cluster',
           '--cache-cluster-id=%s' % self.cluster_id,
           '--region=%s' % self.region]
    vm_util.IssueCommand(cmd)
    # Don't have to delete the subnet group. It will be deleted with the subnet.

  def Flush(self):
    vm_util.RunThreaded(memcached_server.FlushMemcachedServer, self.hosts)

  def GetHosts(self):
    return ["%s:%s" % (ip, port) for ip, port in self.hosts]

  def GetMetadata(self):
    return {'num_servers': self.num_servers,
            'elasticache_region': self.region,
            'elasticache_node_type': self.node_type}

  def _GetClusterInfo(self):
    cmd = ['aws', 'elasticache', 'describe-cache-clusters']
    cmd += ['--cache-cluster-id=%s' % self.cluster_id]
    cmd += ['--region=%s' % self.region]
    cmd += ['--show-cache-node-info']
    out, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(out)["CacheClusters"][0]

  @vm_util.Retry(poll_interval=15, timeout=300,
                 retryable_exceptions=(errors.Resource.RetryableCreationError))
  def _WaitForClusterUp(self):
    """Block until the ElastiCache memcached cluster is up.

    Will timeout after 5 minutes, and raise an exception. Before the timeout
    expires any exceptions are caught and the status check is retried.

    We check the status of the cluster using the AWS CLI.

    Returns:
      The cluster info json as a dict

    Raises:
      errors.Resource.RetryableCreationError when response is not as expected or
        if there is an error connecting to the port or otherwise running the
        remote check command.
    """
    logging.info("Trying to get ElastiCache cluster info for %s",
                 self.cluster_id)
    cluster_status = None
    try:
      cluster_info = self._GetClusterInfo()
      cluster_status = cluster_info['CacheClusterStatus']
      if cluster_status == 'available':
        logging.info("ElastiCache memcached cluster is up and running.")
        return cluster_info
    except errors.VirtualMachine.RemoteCommandError as e:
      raise errors.Resource.RetryableCreationError(
          "ElastiCache memcached cluster not up yet: %s." % str(e))
    else:
      raise errors.Resource.RetryableCreationError(
          "ElastiCache memcached cluster not up yet. Status: %s" %
          cluster_status)
