"""Define pinecone resources."""

from absl import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.pinecone import flags as pinecone_flags

FLAGS = flags.FLAGS


class PineconeServer(resource.BaseResource):
  """Object representing a Snowflake Data Warehouse Instance."""

  CLOUD: list[str] = ['GCP', 'AWS', 'Azure']
  SERVICE_TYPE = pinecone_flags.POD
  RESOURCE_TYPE = 'PineconeServer'
  client_vm: linux_virtual_machine.BaseLinuxVirtualMachine

  def __init__(self, pinecone_spec):
    super().__init__()
    self.server_shards = pinecone_spec.server_shards
    self.server_replicas = pinecone_spec.server_replicas
    self.server_environment = pinecone_spec.server_environment
    self.server_pod_type = pinecone_spec.server_pod_type
    self.server_api_key = pinecone_spec.server_api_key
    if not self.server_api_key:
      self.server_api_key = self._GetAPIKeyFromFile()
    self.index_name = 'pinecone-index-' + FLAGS.run_uri

  def _Create(self):
    """Create a pinecone server."""
    # Indexes are usually created in the benchmark.
    pass

  def _Exists(self):
    """Method to validate the existence of a Pinecone server.

    Returns:
      Boolean value indicating the existence of a pinecone server.
    """
    return True

  def _DeleteResource(self):
    """Delete a Pinecone index."""
    self.client_vm.RemoteCommand(
        f'curl -i -X DELETE "https://api.pinecone.io/indexes/{self.index_name}"'
        f' -H "Api-Key: {self.server_api_key}" -H "X-Pinecone-API-Version:'
        ' 2024-07"',
        ignore_failure=True,
    )

  def _Delete(self):
    """Delete a Pinecone index."""
    # Pinecone override the DeleteResource.
    pass

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata.

    Child classes can extend this if needed.

    Raises:
       RelationalDbPropertyNotSetError: if any expected metadata is missing.
    """
    metadata = {
        'server_shards': self.server_shards,
        'server_replicas': self.server_replicas,
        'server_environment': self.server_environment,
        'server_pod_type': self.server_pod_type,
        'index_name': self.index_name,
    }
    return metadata

  def _GetAPIKeyFromFile(self) -> str:
    """Get the API key from a file."""
    return vm_util.IssueCommand(
        ['cat', pinecone_flags.PINECONE_API_KEY_PATH.value]
    )[0].strip()

  def SetVms(self, vm_groups):
    self.client_vms = vm_groups[
        'clients' if 'clients' in vm_groups else 'default'
    ]
    self.client_vm = self.client_vms[0]


def GetPineconeResourceClass(
    cloud: str,
) -> resource.AutoRegisterResourceMeta | None:
  """Gets the pinecone server class for the given cloud."""
  return resource.GetResourceClass(PineconeServer, CLOUD=cloud)
