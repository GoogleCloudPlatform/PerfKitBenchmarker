"""AI agent service resource for PKB.

This module defines the AI agent service resource, which manages the lifecycle
of agentic applications.
"""

import abc

from perfkitbenchmarker import resource


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
    self.spec = ai_agent_spec

  def _CreateDependencies(self):
    """Creates common dependencies and executes custom deployment logic."""
    self._EnsureObjectStorage()
    self._StageAgentCode()

  @abc.abstractmethod
  def _EnsureObjectStorage(self):
    """Ensures intermediate object storage for Agent communication exists."""

  @abc.abstractmethod
  def _StageAgentCode(self):
    """Prepare agent's code for deployment."""

  @abc.abstractmethod
  def Execute(
      self,
      output_dir: str,
      prompt: str | None = None,
  ) -> None:
    """Executes the agentic workload."""

  @property
  @abc.abstractmethod
  def base_dir(self):
    """Object storage path used to stage files."""

  @property
  @abc.abstractmethod
  def storage_service(self):
    """Storage service accessible by this service."""
