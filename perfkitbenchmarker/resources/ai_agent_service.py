"""AI agent service resource for PKB.

This module defines the AI agent service resource, which manages the lifecycle
of agentic applications.
"""

import abc
from typing import Any

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
import yaml

FLAGS = flags.FLAGS

AI_AGENT_INITIAL_PROMPT_URL = flags.DEFINE_string(
    'ai_agent_initial_prompt_url',
    None,
    'Object storage URL (e.g. gs://bucket/prompt.txt) to an initial prompt.',
)


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
      self,
      client_vm_path: str,
      output_dir: str,
      prompt: str,
      session_id: str,
      user_id: str,
      agent_config: dict[str, Any] | None = None,
  ) -> None:
    config_dict = self._GetRunConfig(
        output_dir, prompt, session_id, user_id, agent_config
    )
    config_str = yaml.safe_dump(config_dict)
    config_local_path = vm_util.WriteTemporaryFile(
        config_str, origin='run_config.yaml'
    )
    self.client_vm.PushDataFile(config_local_path, client_vm_path)

  def _GetDeploymentConfig(self) -> dict[str, Any]:
    """Gets config dict for deployment/creation."""
    return {'run_uri': FLAGS.run_uri}

  def _GetInitialPromptText(self) -> str | None:
    """Fetches the initial prompt text from object storage."""
    url = AI_AGENT_INITIAL_PROMPT_URL.value
    if not url:
      return None

    local_path = vm_util.PrependTempDir('initial_prompt.txt')
    self.storage_service.Copy(url, local_path)
    with open(local_path, 'r') as f:
      return f.read().strip()

  def _GetRunConfig(
      self,
      output_dir: str,
      prompt: str,
      session_id: str,
      user_id: str,
      agent_config: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Gets config dict for running the agent."""
    return {
        'agent': self.spec.agent,
        'framework': self.spec.framework,
        'prompt': prompt,
        'output_dir': output_dir,
        'session_id': session_id,
        'user_id': user_id,
        'run_uri': FLAGS.run_uri,
        'agent_config': (
            agent_config if agent_config is not None else self.agent_config
        ),
    }

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
      agent_config: dict[str, Any] | None = None,
      session_id: str | None = None,
      user_id: str | None = None,
  ) -> None:
    """Runs the prompt on the client VM."""

  @property
  @abc.abstractmethod
  def base_dir(self):
    """Object storage path used to stage files."""

  @property
  @abc.abstractmethod
  def storage_service(self):
    """Storage service accessible by this service."""
