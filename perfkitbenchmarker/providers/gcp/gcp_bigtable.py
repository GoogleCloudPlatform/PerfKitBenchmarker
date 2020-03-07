# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's bigtable instances.

Clusters can be created and deleted.
"""

import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS


def _ValidateReplicationFlags(flag_dict):
  """Verifies correct usage of the bigtable replication flags."""
  return (not flag_dict['bigtable_replication_cluster'] or
          flag_dict['bigtable_replication_cluster_zone'])

flags.DEFINE_integer('bigtable_node_count', 3,
                     'Number of nodes to create in the bigtable cluster.')
flags.DEFINE_enum('bigtable_storage_type', 'ssd', ['ssd', 'hdd'],
                  'Storage class for the cluster')
flags.DEFINE_string('google_bigtable_zone', 'us-central1-b',
                    'Bigtable zone.')
flags.DEFINE_boolean('bigtable_replication_cluster', False,
                     'Whether to create a Bigtable replication cluster.')
flags.DEFINE_string('bigtable_replication_cluster_zone', None,
                    'Zone in which to create a Bigtable replication cluster.')
flags.register_multi_flags_validator(
    ['bigtable_replication_cluster', 'bigtable_replication_cluster_zone'],
    _ValidateReplicationFlags, message='bigtable_replication_cluster_zone must '
    'be set if bigtable_replication_cluster is True.')


class GcpBigtableInstance(resource.BaseResource):
  """Object representing a GCP Bigtable Instance.

  Attributes:
    name: Instance and cluster name.
    num_nodes: Number of nodes in the instance's cluster.
    project: Enclosing project for the instance.
    zone: zone of the instance's cluster.
  """

  def __init__(self, name, project, zone):
    super(GcpBigtableInstance, self).__init__()
    self.num_nodes = FLAGS.bigtable_node_count
    self.storage_type = FLAGS.bigtable_storage_type
    self.name = name
    self.zone = zone
    self.project = project

  def _Create(self):
    """Creates the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'create',
                             self.name)
    cmd.flags['display-name'] = self.name
    cmd.flags['cluster'] = '{}-0'.format(self.name)
    cmd.flags['cluster-num-nodes'] = self.num_nodes
    cmd.flags['cluster-storage-type'] = self.storage_type
    cmd.flags['cluster-zone'] = self.zone
    cmd.flags['project'] = self.project
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    cmd.Issue()

    if FLAGS.bigtable_replication_cluster:
      cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'clusters', 'create',
                               '{}-1'.format(self.name))
      cmd.flags['instance'] = self.name
      cmd.flags['zone'] = FLAGS.bigtable_replication_cluster_zone
      cmd.flags['num-nodes'] = self.num_nodes
      cmd.Issue()

    logging.info('Creating instance %s.', self.name)

  def _Delete(self):
    """Deletes the instance."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'delete',
                             self.name)
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Returns true if the instance exists."""
    cmd = util.GcloudCommand(self, 'beta', 'bigtable', 'instances', 'list')
    cmd.flags['format'] = 'json'
    cmd.flags['project'] = self.project
    # The zone flag makes this command fail.
    cmd.flags['zone'] = []
    stdout, stderr, retcode = cmd.Issue(
        suppress_warning=True, raise_on_failure=False)
    if retcode != 0:
      # This is not ideal, as we're returning false not because we know
      # the table isn't there, but because we can't figure out whether
      # it is there.  This behavior is consistent without other
      # _Exists methods.
      logging.error('Unable to list GCP Bigtable instances. Return code %s '
                    'STDOUT: %s\nSTDERR: %s', retcode, stdout, stderr)
      return False
    result = json.loads(stdout)
    for instance in result:
      if instance['displayName'] == self.name:
        return instance['state'] == 'READY'


def GetClustersDecription(instance_name, project):
  """Gets descriptions of all the clusters given the instance and project.

  This is a module function to allow getting description of clusters not created
  by pkb.

  Args:
    instance_name: Instance to get cluster descriptions for.
    project: Project where instance is in.

  Returns:
    A list of cluster descriptions dicts.
  """
  cmd = util.GcloudCommand(None, 'beta', 'bigtable', 'clusters', 'list')
  cmd.flags['instances'] = instance_name
  cmd.flags['project'] = project
  stdout, stderr, retcode = cmd.Issue(
      suppress_warning=True, raise_on_failure=False)
  if retcode:
    logging.error('Command "%s" failed:\nSTDOUT:\n%s\nSTDERR:\n%s',
                  ' '.join(cmd), stdout, stderr)
  output = json.loads(stdout)

  result = []
  for cluster_details in output:
    current_instance_name = cluster_details['name'].split('/')[3]
    if current_instance_name == instance_name:
      cluster_details['name'] = cluster_details['name'].split('/')[5]
      cluster_details['zone'] = cluster_details['location'].split('/')[3]
      result.append(cluster_details)

  return result

