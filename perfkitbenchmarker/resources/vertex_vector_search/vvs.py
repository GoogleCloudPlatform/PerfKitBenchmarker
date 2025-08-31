# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Define Vertex Vector Search resources."""

import logging

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS


class VertexVectorSearch(resource.BaseResource):
  """Object representing a Vertex Vector Search instance."""

  CLOUD: list[str] = ['GCP', 'AWS', 'Azure']
  RESOURCE_TYPE = 'VertexVectorSearch'
  client_vm: linux_virtual_machine.BaseLinuxVirtualMachine

  def __init__(self, vvs_spec):
    super().__init__()
    self.location = vvs_spec.location
    self.dataset_bucket_url = vvs_spec.dataset_bucket_url
    self.vpc_network = vvs_spec.vpc_network
    self.machine_type = vvs_spec.machine_type
    self.min_replica_count = vvs_spec.min_replica_count
    self.max_replica_count = vvs_spec.max_replica_count
    self.index_name = 'vvs-index-' + FLAGS.run_uri
    self.endpoint_name = 'vvs-endpoint-' + FLAGS.run_uri
    self.base_url = vvs_spec.base_url

  def _Create(self):
    """Create a vector search server."""
    # Indexes are usually created in the benchmark.
    pass

  def _CreateResource(self):
    """Create a vector search server."""
    # Indexes are usually created in the benchmark.
    self.created = True

  def _Exists(self):
    """Method to validate the existence of a Vertex Vector Search server.

    Returns:
      Boolean value indicating the existence of a Vertex Vector Search server.
    """
    index_id = self._FindIndexId()
    endpoint_id = self._FindEndpointId()
    return bool(index_id or endpoint_id)

  def _FindIndexId(self):
    """Find the created index ID using the index name.

    Returns:
      The ID of the index, None if not found.
    """
    cmd = util.GcloudCommand(self, 'ai', 'indexes', 'list')
    cmd.flags['region'] = self.location
    cmd.flags['filter'] = f'displayName:{self.index_name}'
    cmd.flags['format'] = 'value(name.basename())'

    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    index_id = stdout.strip()

    if not index_id:
      logging.info(
          "Index '%s' not found", self.index_name
      )
      return None

    return index_id

  def _FindEndpointId(self):
    """Find the created endpoint ID.

    Returns:
      The ID of the endpoint, None if not found.
    """
    cmd = util.GcloudCommand(self, 'ai', 'index-endpoints', 'list')
    cmd.flags['region'] = self.location
    cmd.flags['filter'] = f'displayName:{self.endpoint_name}'
    cmd.flags['format'] = 'value(name.basename())'

    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    endpoint_id = stdout.strip()

    if not endpoint_id:
      logging.info(
          "Endpoint '%s' not found", self.endpoint_name
      )
      return None

    return endpoint_id

  def _FindDeployedIndexId(self, endpoint_id):
    """Finds the deployed index ID.

    Indexes are deployed to endpoints in the benchmark. This function is used
    to find the deployed index ID for a given endpoint.
    Args:
        endpoint_id: The ID of the endpoint to find the deployed index ID for.

    Returns:
        The ID of the deployed index on the endpoint, None if not found.
    """
    cmd = util.GcloudCommand(
        self, 'ai', 'index-endpoints', 'describe', endpoint_id
    )
    cmd.flags['region'] = self.location
    cmd.flags['format'] = 'value(deployedIndexes[0].id)'
    stdout, _, _ = cmd.Issue()
    deployed_index_id = stdout.strip()

    if not deployed_index_id:
      logging.info(
          "Deployed index '%s' not found", endpoint_id
      )
      return None

    return deployed_index_id

  def _UndeployIndex(self, endpoint_id, deployed_index_id):
    """Undeploy the index from the endpoint."""
    undeploy_cmd = util.GcloudCommand(
        self, 'ai', 'index-endpoints', 'undeploy-index', endpoint_id
    )
    undeploy_cmd.flags['deployed-index-id'] = deployed_index_id
    undeploy_cmd.flags['region'] = self.location
    logging.info(
        'Attempting to undeploy index %s from endpoint %s.',
        deployed_index_id,
        endpoint_id,
    )
    undeploy_cmd.Issue()

  def _DeleteIndexEndpoint(self, endpoint_id):
    """Delete the endpoint."""
    delete_endpoint_cmd = util.GcloudCommand(
        self, 'ai', 'index-endpoints', 'delete', endpoint_id
    )
    delete_endpoint_cmd.flags['region'] = self.location

    logging.info('Attempting to delete endpoint %s.', endpoint_id)
    delete_endpoint_cmd.Issue()

  def _DeleteIndex(self, index_id):
    """Delete the index."""
    delete_index_cmd = util.GcloudCommand(
        self, 'ai', 'indexes', 'delete', index_id
    )
    delete_index_cmd.flags['region'] = self.location
    logging.info('Attempting to delete endpoint %s.', index_id)
    delete_index_cmd.Issue()

  def _Delete(self):
    """Undeploys the index from the endpoint, then deletes the index and endpoint."""

    logging.info('Starting VVS resource deletion...')
    index_id = self._FindIndexId()
    endpoint_id = self._FindEndpointId()

    if not index_id and not endpoint_id:
      return

    if endpoint_id:
      self._HandleUndeployIndex(endpoint_id)
      self._HandleEndpointDeletion(endpoint_id)

    if index_id:
      self._HandleIndexDeletion(index_id)

  def _HandleUndeployIndex(self, endpoint_id):
    """Undeploys the index from the given endpoint."""
    deployed_index_id = self._FindDeployedIndexId(endpoint_id)
    if not deployed_index_id:
      return

    logging.info('Found deployed_index_id: %s', deployed_index_id)
    try:
      logging.info('Undeploying the index from the endpoint...')
      self._UndeployIndex(endpoint_id, deployed_index_id)
    except errors.VmUtil.IssueCommandError as e:
      logging.warning(
          'Failed to undeploy index %s from endpoint %s: %s',
          deployed_index_id, endpoint_id, e
      )

  def _HandleEndpointDeletion(self, endpoint_id):
    """Attempts to delete the index endpoint."""
    try:
      if self._FindEndpointId():
        logging.info('Deleting endpoint %s', endpoint_id)
        self._DeleteIndexEndpoint(endpoint_id)
    except errors.VmUtil.IssueCommandError as e:
      logging.warning(
          'Failed to delete endpoint %s: %s. Continuing with deletion.',
          endpoint_id,
          e,
      )

  def _HandleIndexDeletion(self, index_id):
    """Attempts to delete the index."""
    try:
      if self._FindIndexId():
        logging.info('Deleting index %s', index_id)
        self._DeleteIndex(index_id)
    except errors.VmUtil.IssueCommandError as e:
      logging.warning('Failed to delete index %s: %s.', index_id, e)
      raise errors.Resource.RetryableDeletionError(
          f'Index deletion command failed: {e}'
      )

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata."""
    metadata = {
        'location': self.location,
        'dataset_bucket_url': self.dataset_bucket_url,
        'vpc_network': self.vpc_network,
        'machine_type': self.machine_type,
        'min_replica_count': self.min_replica_count,
        'max_replica_count': self.max_replica_count,
        'index_name': self.index_name,
    }
    return metadata

  def SetVms(self, vm_groups):
    """Sets the client VMs that will interact with VVS."""
    self.client_vms = vm_groups[
        'clients' if 'clients' in vm_groups else 'default'
    ]
    self.client_vm = self.client_vms[0]


def GetVVSResourceClass(
    cloud: str,
) -> resource.AutoRegisterResourceMeta | None:
  """Gets the Vertex Vector Search resource class for the given cloud."""
  return resource.GetResourceClass(VertexVectorSearch, CLOUD=cloud)
