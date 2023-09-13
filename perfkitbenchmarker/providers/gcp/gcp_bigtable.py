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
import time
from typing import Any, Dict, List, Optional

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import util
import requests

FLAGS = flags.FLAGS

ENDPOINT = flags.DEFINE_string(
    'google_bigtable_endpoint',
    'bigtable.googleapis.com',
    'Google API endpoint for Cloud Bigtable.',
)
ADMIN_ENDPOINT = flags.DEFINE_string(
    'google_bigtable_admin_endpoint',
    'bigtableadmin.googleapis.com',
    'Google API endpoint for Cloud Bigtable table administration.',
)
flags.DEFINE_string('google_bigtable_instance_name', None,
                    'Bigtable instance name. If not specified, new instance '
                    'will be created and deleted on the fly. If specified, '
                    'the instance is considered user managed and will not '
                    'created/deleted by PKB.')
flags.DEFINE_integer(
    'bigtable_node_count',
    None,
    (
        'Number of nodes to create in the bigtable cluster. '
        'Ignored if --bigtable_autoscaling_min_nodes is set.'
    ),
)
_LOAD_NODES = flags.DEFINE_integer(
    'bigtable_load_node_count',
    None,
    'The number of nodes for the Bigtable instance to use for the load'
    ' phase. Assumes that the benchmark calls UpdateRunCapacity to set the '
    ' correct node count manually before the run phase.',
)
_AUTOSCALING_MIN_NODES = flags.DEFINE_integer(
    'bigtable_autoscaling_min_nodes', None,
    'Minimum number of nodes for autoscaling.')
_AUTOSCALING_MAX_NODES = flags.DEFINE_integer(
    'bigtable_autoscaling_max_nodes', None,
    'Maximum number of nodes for autoscaling.')
_AUTOSCALING_CPU_TARGET = flags.DEFINE_integer(
    'bigtable_autoscaling_cpu_target', None,
    'The target CPU utilization percent for autoscaling.')
flags.DEFINE_enum('bigtable_storage_type', None, ['ssd', 'hdd'],
                  'Storage class for the cluster')
flags.DEFINE_string('google_bigtable_zone', None,
                    'Bigtable zone.')
flags.DEFINE_boolean('bigtable_replication_cluster', None,
                     'Whether to create a Bigtable replication cluster.')
flags.DEFINE_string('bigtable_replication_cluster_zone', None,
                    'Zone in which to create a Bigtable replication cluster.')
flags.DEFINE_boolean('bigtable_multicluster_routing', None,
                     'Whether to use multi-cluster routing.')

_DEFAULT_NODE_COUNT = 3
_DEFAULT_STORAGE_TYPE = 'ssd'
_DEFAULT_ZONE = 'us-central1-b'
_DEFAULT_REPLICATION_ZONE = 'us-central1-c'

_FROZEN_NODE_COUNT = 1


class BigtableSpec(non_relational_db.BaseNonRelationalDbSpec):
  """Configurable options of a Bigtable instance. See below for descriptions."""

  SERVICE_TYPE = non_relational_db.BIGTABLE

  name: str
  zone: str
  project: str
  node_count: int
  load_node_count: int
  storage_type: str
  replication_cluster: bool
  replication_cluster_zone: str
  multicluster_routing: bool
  autoscaling_min_nodes: int
  autoscaling_max_nodes: int
  autoscaling_cpu_target: int

  def __init__(self, component_full_name, flag_values, **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes / constructor args for each configurable option."""
    result = super()._GetOptionDecoderConstructions()
    none_ok = {'default': None, 'none_ok': True}
    result.update({
        'name': (option_decoders.StringDecoder, none_ok),
        'zone': (option_decoders.StringDecoder, none_ok),
        'project': (option_decoders.StringDecoder, none_ok),
        'node_count': (option_decoders.IntDecoder, none_ok),
        'load_node_count': (option_decoders.IntDecoder, none_ok),
        'storage_type': (option_decoders.StringDecoder, none_ok),
        'replication_cluster': (option_decoders.BooleanDecoder, none_ok),
        'replication_cluster_zone': (option_decoders.StringDecoder, none_ok),
        'multicluster_routing': (option_decoders.BooleanDecoder, none_ok),
        'autoscaling_min_nodes': (option_decoders.IntDecoder, none_ok),
        'autoscaling_max_nodes': (option_decoders.IntDecoder, none_ok),
        'autoscaling_cpu_target': (option_decoders.IntDecoder, none_ok),
    })
    return result

  @classmethod
  def _ValidateConfig(cls, config_values) -> None:
    """Verifies correct usage of the bigtable config options."""
    if (config_values.get('multicluster_routing', False) and
        not config_values.get('replication_cluster', False)):
      raise errors.Config.InvalidValue(
          'bigtable_replication_cluster must be set if '
          'bigtable_multicluster_routing is True.')

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values) -> None:
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May be
        modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
        provided config values.
    """
    super()._ApplyFlags(config_values, flag_values)
    option_name_from_flag = {
        'google_bigtable_instance_name': 'name',
        'google_bigtable_zone': 'zone',
        'bigtable_storage_type': 'storage_type',
        'bigtable_node_count': 'node_count',
        'bigtable_load_node_count': 'load_node_count',
        'bigtable_replication_cluster': 'replication_cluster',
        'bigtable_replication_cluster_zone': 'replication_cluster_zone',
        'bigtable_multicluster_routing': 'multicluster_routing',
        'bigtable_autoscaling_min_nodes': 'autoscaling_min_nodes',
        'bigtable_autoscaling_max_nodes': 'autoscaling_max_nodes',
        'bigtable_autoscaling_cpu_target': 'autoscaling_cpu_target',
    }
    for flag_name, option_name in option_name_from_flag.items():
      if flag_values[flag_name].present:
        config_values[option_name] = flag_values[flag_name].value

    cls._ValidateConfig(config_values)

  def __repr__(self) -> str:
    return str(self.__dict__)


class GcpBigtableInstance(non_relational_db.BaseNonRelationalDb):
  """Object representing a GCP Bigtable Instance.

  See https://cloud.google.com/bigtable/docs/overview.

  For replication settings, see

  For autoscaling/multicluster attributes, see
  https://cloud.google.com/bigtable/docs/autoscaling.

  Attributes:
    name: Instance and cluster name.
    project: Enclosing project for the instance.
    zone: zone of the instance's cluster.
    node_count: Number of nodes in the instance's cluster.
    storage_type: Storage class for the cluster.
    replication_cluster: Whether the instance has a replication cluster.
    replication_cluster_zone: Zone for the replication cluster.
    multicluster_routing: Whether the instance uses multicluster_routing.
    autoscaling_min_nodes: Minimum number of nodes for autoscaling.
    autoscaling_max_nodes: Maximum number of nodes for autoscaling.
    autoscaling_cpu_target: CPU utilization percent for autoscaling.
  """

  SERVICE_TYPE = non_relational_db.BIGTABLE

  def __init__(self,
               name: Optional[str],
               project: Optional[str],
               zone: Optional[str],
               node_count: Optional[int],
               load_node_count: Optional[int],
               storage_type: Optional[str],
               replication_cluster: Optional[bool],
               replication_cluster_zone: Optional[str],
               multicluster_routing: Optional[bool],
               autoscaling_min_nodes: Optional[int],
               autoscaling_max_nodes: Optional[int],
               autoscaling_cpu_target: Optional[int],
               **kwargs):
    super(GcpBigtableInstance, self).__init__(**kwargs)
    if name is not None:
      self.user_managed = True
    self.name: str = name or f'pkb-bigtable-{FLAGS.run_uri}'
    self.zone: str = zone or FLAGS.google_bigtable_zone
    self.project: str = project or FLAGS.project or util.GetDefaultProject()
    self.node_count: int = node_count or _DEFAULT_NODE_COUNT
    self._load_node_count = load_node_count or self.node_count
    self.storage_type: str = storage_type or _DEFAULT_STORAGE_TYPE
    self.replication_cluster: bool = replication_cluster or False
    self.replication_cluster_zone: Optional[str] = (
        replication_cluster_zone or
        _DEFAULT_REPLICATION_ZONE) if self.replication_cluster else None
    self.multicluster_routing: bool = multicluster_routing or False
    self.autoscaling_min_nodes: Optional[int] = autoscaling_min_nodes or None
    self.autoscaling_max_nodes: Optional[int] = autoscaling_max_nodes or None
    self.autoscaling_cpu_target: Optional[int] = autoscaling_cpu_target or None

  @classmethod
  def FromSpec(cls, spec: BigtableSpec) -> 'GcpBigtableInstance':
    return cls(
        name=spec.name,
        zone=spec.zone,
        project=spec.project,
        node_count=spec.node_count,
        load_node_count=spec.load_node_count,
        storage_type=spec.storage_type,
        replication_cluster=spec.replication_cluster,
        replication_cluster_zone=spec.replication_cluster_zone,
        multicluster_routing=spec.multicluster_routing,
        autoscaling_min_nodes=spec.autoscaling_min_nodes,
        autoscaling_max_nodes=spec.autoscaling_max_nodes,
        autoscaling_cpu_target=spec.autoscaling_cpu_target,
        delete_on_freeze_error=spec.delete_on_freeze_error,
        create_on_restore_error=spec.create_on_restore_error,
        enable_freeze_restore=spec.enable_freeze_restore)

  def _BuildClusterConfigs(self) -> List[str]:
    """Return flag values for --cluster_config when creating an instance.

    Returns:
      List of strings for repeated --cluster_config flag values.
    """
    flag_values = []
    cluster_config = {
        'id': f'{self.name}-0',
        'zone': self.zone,
        'nodes': self.node_count,
        # Depending on flag settings, the config may be incomplete, but we rely
        # on gcloud to validate for us.
        'autoscaling-min-nodes': self.autoscaling_min_nodes,
        'autoscaling-max-nodes': self.autoscaling_max_nodes,
        'autoscaling-cpu-target': self.autoscaling_cpu_target,
    }

    # Ignore nodes if autoscaling is configured. --bigtable_node_count has a
    # default value so we want to maintain backwards compatibility.
    if self.autoscaling_min_nodes:
      del cluster_config['nodes']

    keys_to_remove = []
    for k, v in cluster_config.items():
      if v is None:
        keys_to_remove.append(k)
    for key in keys_to_remove:
      del cluster_config[key]

    flag_values.append(','.join(
        '{}={}'.format(k, v) for (k, v) in cluster_config.items()))

    if self.replication_cluster:
      replication_cluster_config = cluster_config.copy()
      replication_cluster_config['id'] = f'{self.name}-1'
      replication_cluster_config['zone'] = self.replication_cluster_zone
      flag_values.append(','.join(
          '{}={}'.format(k, v)
          for (k, v) in replication_cluster_config.items()))

    return flag_values

  def _Create(self):
    """Creates the instance."""
    cmd = _GetBigtableGcloudCommand(self, 'bigtable', 'instances', 'create',
                                    self.name)
    cmd.flags['display-name'] = self.name
    cmd.flags['cluster-storage-type'] = self.storage_type
    cmd.flags['project'] = self.project
    cmd.flags['cluster-config'] = self._BuildClusterConfigs()

    logging.info('Creating instance %s.', self.name)

    _, stderr, _ = cmd.Issue()
    if 'Insufficient node quota' in stderr:
      raise errors.Benchmarks.QuotaFailure(
          f'Insufficient node quota in project {self.project} '
          f'and zone {self.zone}')

    self._UpdateLabels(util.GetDefaultTags())

    if self.multicluster_routing:
      cmd = _GetBigtableGcloudCommand(
          self, 'bigtable', 'app-profiles', 'update', 'default')
      cmd.flags['instance'] = self.name
      cmd.flags['route-any'] = True
      cmd.flags['force'] = True
      cmd.Issue()

  def _GetLabels(self) -> Dict[str, Any]:
    """Gets labels from the current instance."""
    return self._DescribeInstance().get('labels', {})

  def _UpdateLabels(self, labels: Dict[str, Any]) -> None:
    """Updates the labels of the current instance."""
    header = {'Authorization': f'Bearer {util.GetAccessToken()}'}
    url = (
        f'https://{ADMIN_ENDPOINT.value}/v2/'
        f'projects/{self.project}/instances/{self.name}'
    )
    # Keep any existing labels
    tags = self._GetLabels()
    tags.update(labels)
    response = requests.patch(
        url,
        headers=header,
        params={'updateMask': 'labels'},
        json={'labels': tags})
    logging.info('Update labels: status code %s, %s', response.status_code,
                 response.text)
    if response.status_code != 200:
      raise errors.Resource.UpdateError(
          f'Unable to update Bigtable instance: {response.text}')

  def _UpdateTimeout(self, timeout_minutes: int) -> None:
    """See base class."""
    self._UpdateLabels(util.GetDefaultTags(timeout_minutes))

  def _Delete(self):
    """Deletes the instance."""
    cmd = _GetBigtableGcloudCommand(self, 'bigtable', 'instances', 'delete',
                                    self.name)
    cmd.Issue(raise_on_failure=False)

  def _DescribeInstance(self) -> Dict[str, Any]:
    cmd = _GetBigtableGcloudCommand(
        self, 'bigtable', 'instances', 'describe', self.name)
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Describing instance %s failed: %s', self.name, stderr)
      return {}
    return json.loads(stdout)

  def _Exists(self):
    """Returns true if the instance exists."""
    instance = self._DescribeInstance()
    if not instance:
      return False
    return instance['state'] == 'READY'

  def GetResourceMetadata(self) -> Dict[str, Any]:
    metadata = {}
    if self.user_managed:
      clusters = GetClustersDescription(self.name, self.project)
      metadata['bigtable_zone'] = [
          cluster['zone'] for cluster in clusters]
      metadata['bigtable_storage_type'] = [
          cluster['defaultStorageType'] for cluster in clusters]
      metadata['bigtable_node_count'] = [
          cluster['serveNodes'] for cluster in clusters]
    else:
      metadata['bigtable_zone'] = self.zone
      metadata['bigtable_replication_zone'] = self.replication_cluster_zone
      metadata['bigtable_storage_type'] = self.storage_type
      metadata['bigtable_node_count'] = self.node_count
      metadata['bigtable_multicluster_routing'] = self.multicluster_routing
    return metadata

  def _GetClusters(self) -> List[Dict[str, Any]]:
    cmd = _GetBigtableGcloudCommand(self, 'bigtable', 'clusters', 'list')
    cmd.flags['instances'] = self.name
    stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      logging.error('Listing clusters %s failed: %s', self.name, stderr)
      return []
    return json.loads(stdout)

  def _UpdateNodes(self, nodes: int) -> None:
    """Updates clusters to the specified node count and waits until ready."""
    # User managed instances currently aren't supported since instance
    # attributes aren't discovered after initialization.
    if self.user_managed:
      return
    clusters = self._GetClusters()
    for i in range(len(clusters)):
      # Do nothing if the node count is already equal to what we want.
      if clusters[i]['serveNodes'] == nodes:
        continue
      cluster_name = clusters[i]['name']
      cmd = _GetBigtableGcloudCommand(
          self, 'bigtable', 'clusters', 'update', cluster_name
      )
      cmd.flags['instance'] = self.name
      cmd.flags['num-nodes'] = nodes or self.node_count
      cmd.Issue()
    # Note that Exists is implemented like IsReady, but should likely be
    # refactored.
    while not self._Exists():
      time.sleep(10)

  def UpdateCapacityForLoad(self) -> None:
    """See base class."""
    self._UpdateNodes(self._load_node_count)

  def UpdateCapacityForRun(self) -> None:
    """See base class."""
    self._UpdateNodes(self.node_count)

  def _Freeze(self) -> None:
    self._UpdateNodes(_FROZEN_NODE_COUNT)

  def _Restore(self) -> None:
    self._UpdateNodes(self.node_count)


def _GetBigtableGcloudCommand(instance, *args) -> util.GcloudCommand:
  """Returns gcloud bigtable commmand without the zone argument."""
  cmd = util.GcloudCommand(instance, *args)
  # The zone flag makes this command fail.
  cmd.flags['zone'] = []
  return cmd


def GetClustersDescription(instance_name, project):
  """Gets descriptions of all the clusters given the instance and project.

  This is a module function to allow getting description of clusters not created
  by pkb.

  Args:
    instance_name: Instance to get cluster descriptions for.
    project: Project where instance is in.

  Returns:
    A list of cluster descriptions dicts.
  """
  cmd = util.GcloudCommand(None, 'bigtable', 'clusters', 'list')
  cmd.flags['instances'] = instance_name
  cmd.flags['project'] = project
  stdout, stderr, retcode = cmd.Issue(raise_on_failure=False)
  if retcode:
    logging.error('Command "%s" failed:\nSTDOUT:\n%s\nSTDERR:\n%s',
                  repr(cmd), stdout, stderr)
  output = json.loads(stdout)

  result = []
  for cluster_details in output:
    current_instance_name = cluster_details['name'].split('/')[3]
    if current_instance_name == instance_name:
      cluster_details['name'] = cluster_details['name'].split('/')[5]
      cluster_details['zone'] = cluster_details['location'].split('/')[3]
      result.append(cluster_details)

  return result
