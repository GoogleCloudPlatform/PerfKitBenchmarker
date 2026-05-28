"""AI agent service resource for PKB.

This module defines the AI agent service resource, which manages the lifecycle
of agentic applications.
"""

import abc
from typing import Any

from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
import yaml


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
    self.agent_config: dict[str, Any] = {}

  def UploadDeployConfigToClientVm(self, client_vm_path: str) -> None:
    config_dict = self._GetDeploymentConfig()
    config_str = yaml.safe_dump(config_dict)
    config_local_path = vm_util.WriteTemporaryFile(
        config_str, origin='deploy_config.yaml'
    )
    self.client_vm.PushDataFile(config_local_path, client_vm_path)

  def UploadRunConfigToClientVm(
      self, client_vm_path: str, output_dir: str, prompt_file: str
  ) -> None:
    config_dict = self._GetRunConfig(output_dir, prompt_file)
    config_str = yaml.safe_dump(config_dict)
    config_local_path = vm_util.WriteTemporaryFile(
        config_str, origin='run_config.yaml'
    )
    self.client_vm.PushDataFile(config_local_path, client_vm_path)

  @abc.abstractmethod
  def _GetDeploymentConfig(self) -> dict[str, Any]:
    """Gets config dict for deployment/creation."""

  @abc.abstractmethod
  def _GetRunConfig(self, output_dir: str, prompt_file: str) -> dict[str, Any]:
    """Gets config dict for running the agent."""

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
