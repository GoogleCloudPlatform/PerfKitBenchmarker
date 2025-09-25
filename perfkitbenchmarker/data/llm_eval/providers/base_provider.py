"""Abstract base class for LLM providers."""

import abc
from typing import Any, Dict, List

from logger import get_logger


logger = get_logger(__name__)

TEMPERATURE = 0.0


class BaseProvider(abc.ABC):
  """Abstract base class for LLM providers."""

  @abc.abstractmethod
  def get_models(self, include_preview: bool = False) -> List[str]:
    """Fetches the latest generation of text-based models from the provider.

    Args:
      include_preview: Whether to include preview models in the list.

    Returns:
      A list of model names.
    """
    pass

  @abc.abstractmethod
  def benchmark_model(
      self, model_name: str, prompt: str, prompt_id: str, max_output_tokens: int
  ) -> Dict[str, Any]:
    """Benchmarks a single model.

    Args:
      model_name: The name of the model to benchmark.
      prompt: The prompt to send to the model.
      prompt_id: The ID of the prompt.
      max_output_tokens: The maximum number of tokens to generate.

    Returns:
      A dictionary of benchmark results.
    """
    logger.debug(f"Benchmarking model: {model_name} with prompt: '{prompt_id}'")
