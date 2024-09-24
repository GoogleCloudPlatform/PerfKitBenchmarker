"""A fake implementation of a managed AI model for testing."""

from typing import Any

from perfkitbenchmarker.resources import managed_ai_model


class FakeManagedAiModel(managed_ai_model.BaseManagedAiModel):
  """Fake managed AI model for testing."""

  CLOUD = 'TEST'

  def __init__(self, **kwargs: Any) -> Any:
    super().__init__(**kwargs)
    self.existing_endpoints: list[str] = ['one-endpoint']
    self.zone = 'us-central1-a'

  def GetRegionFromZone(self, zone: str) -> str:
    return zone + '-region'

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    return [prompt]

  def ListExistingEndpoints(self, region: str | None = None) -> list[str]:
    del region
    return self.existing_endpoints

  def _Create(self) -> None:
    pass

  def _Delete(self) -> None:
    pass

  def _InitializeNewModel(self) -> managed_ai_model.BaseManagedAiModel:
    return FakeManagedAiModel()
