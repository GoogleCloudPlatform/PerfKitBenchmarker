"""AI agent service resource for PKB.

This module defines the AI agent service resource, which manages the lifecycle
of agentic applications.
"""

import abc

from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import resource
from perfkitbenchmarker import virtual_machine


def GetAiAgentServiceClass(cloud: str, deployment_type: str):
  """Returns the correct AI agent service class based on cloud/type."""
  return resource.GetResourceClass(
      BaseAiAgentService, CLOUD=cloud, DEPLOYMENT_TYPE=deployment_type
  )


class BaseAiAgentService(resource.BaseResource):
  """Base class for AI agent services."""

  RESOURCE_TYPE = 'BaseAiAgentService'
  REQUIRED_ATTRS = ['CLOUD', 'DEPLOYMENT_TYPE']
  CLOUD = 'abstract'
  DEPLOYMENT_TYPE = 'abstract'

  def __init__(self, client_vm, ai_agent_spec):
    super().__init__()
    self.client_vm = client_vm

  @abc.abstractmethod
  def Execute(
      self,
      workload_script: str,
      model: str,
      output_dir: str,
      model_location: str | None = None,
  ) -> None:
    """Executes the agentic workload."""

  @property
  @abc.abstractmethod
  def base_dir(self) -> str:
    """Object storage path used to stage files."""

  @property
  @abc.abstractmethod
  def storage_service(self) -> object_storage_service.ObjectStorageService:
    """Storage service accessible by this service."""

  def PrepareClientVm(
      self, client_vm: virtual_machine.BaseVirtualMachine, packages: list[str]
  ):
    """Hook to prepare client VM before running benchmark."""
