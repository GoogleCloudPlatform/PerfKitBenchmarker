"""A fake implementation of a managed AI model for testing."""

from typing import Any
from unittest import mock

from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.resources import managed_ai_model_spec


class FakeManagedAiModel(managed_ai_model.BaseManagedAiModel):
  """Fake managed AI model for testing."""

  CLOUD = 'TEST'

  def __init__(self, **kwargs: Any) -> Any:
    self.model_spec = mock.create_autospec(
        managed_ai_model_spec.BaseManagedAiModelSpec
    )
    self.model_spec.model_name = 'llama2'
    self.model_spec.max_scale = 1
    super().__init__(
        model_spec=self.model_spec,
        vm=mock.create_autospec(virtual_machine.BaseVirtualMachine),
        **kwargs
    )
    self.existing_endpoints: list[str] = ['one-endpoint']
    self.zone = 'us-central1-a'
    self.vm = None

  def GetRegionFromZone(self, zone: str) -> str:
    return zone + '-region'

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    return [prompt]

  def GetPromptCommand(self, prompt: str, max_tokens: int, temperature: float):
    return f'echo {prompt}'

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    del region
    return self.existing_endpoints

  def _Create(self) -> None:
    pass

  def _Delete(self) -> None:
    pass

  def _InitializeNewModel(self) -> managed_ai_model.BaseManagedAiModel:
    return FakeManagedAiModel()
