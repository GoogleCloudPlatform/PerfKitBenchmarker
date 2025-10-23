"""Abstract base class for LLM providers."""

import abc
import dataclasses
from typing import Any, Dict, List, Optional

from logger import get_logger


logger = get_logger(__name__)

TEMPERATURE = 0.0


@dataclasses.dataclass
class NonStreamingResult:
  """Structured result for a non-streaming benchmark."""

  output_tokens: Optional[int] = None
  total_time_seconds: Optional[float] = None
  error: Optional[str] = None


@dataclasses.dataclass
class StreamingResult:
  """Structured result for a streaming benchmark."""

  output_tokens: Optional[int] = None
  total_time_seconds: Optional[float] = None
  time_to_first_token_seconds: Optional[float] = None
  error: Optional[str] = None


@dataclasses.dataclass
class LlmBenchmarkResult:
  """A structured object for LLM benchmark results."""

  input_request_tokens: Optional[int] = None
  error: Optional[str] = None

  # Non-Streaming Metrics
  non_streaming_full_response_output_tokens: Optional[int] = None
  non_streaming_full_response_time_in_seconds: Optional[float] = None

  # Streaming Metrics
  streaming_full_response_output_tokens: Optional[int] = None
  streaming_full_response_time_in_seconds: Optional[float] = None
  streaming_time_to_first_token_in_seconds: Optional[float] = None
  streaming_error: Optional[str] = None

  def as_dict(self) -> Dict[str, Any]:
    """Return the dataclass as a dictionary, filtering out None values."""
    return {
        k: v for k, v in dataclasses.asdict(self).items() if v is not None
    }

  @classmethod
  def from_results(
      cls,
      input_tokens: int,
      non_streaming_result: NonStreamingResult,
      streaming_result: StreamingResult,
  ) -> 'LlmBenchmarkResult':
    """Factory method to create a result from constituent parts."""
    return cls(
        input_request_tokens=input_tokens,
        non_streaming_full_response_output_tokens=non_streaming_result.output_tokens,
        non_streaming_full_response_time_in_seconds=non_streaming_result.total_time_seconds,
        streaming_full_response_output_tokens=streaming_result.output_tokens,
        streaming_full_response_time_in_seconds=streaming_result.total_time_seconds,
        streaming_time_to_first_token_in_seconds=streaming_result.time_to_first_token_seconds,
        streaming_error=streaming_result.error,
    )


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

  def benchmark_model(
      self, model_name: str, prompt: str, prompt_id: str, max_output_tokens: int
  ) -> LlmBenchmarkResult:
    """Benchmarks a single model using the template method pattern.

    Args:
      model_name: The name of the model to benchmark.
      prompt: The prompt to send to the model.
      prompt_id: The ID of the prompt.
      max_output_tokens: The maximum number of tokens to generate.

    Returns:
      An LlmBenchmarkResult object containing the benchmark results.
    """
    logger.debug(f"Benchmarking model: {model_name} with prompt: '{prompt_id}'")
    client = self._create_client()
    input_tokens = self._count_input_tokens(client, prompt, model_name)

    non_streaming_result = self._execute_non_streaming(
        client, model_name, prompt, max_output_tokens
    )
    if non_streaming_result.error:
      return LlmBenchmarkResult(
          input_request_tokens=input_tokens, error=non_streaming_result.error
      )

    streaming_result = self._execute_streaming(
        client, model_name, prompt, max_output_tokens
    )

    return LlmBenchmarkResult.from_results(
        input_tokens, non_streaming_result, streaming_result
    )

  @abc.abstractmethod
  def _create_client(self) -> Any:
    """Creates and returns a provider-specific API client."""
    pass

  @abc.abstractmethod
  def _count_input_tokens(
      self, client: Any, prompt: str, model_name: str
  ) -> int:
    """Counts the number of tokens in the input prompt."""
    pass

  @abc.abstractmethod
  def _execute_non_streaming(
      self, client: Any, model_name: str, prompt: str, max_output_tokens: int
  ) -> NonStreamingResult:
    """Executes the non-streaming benchmark."""
    pass

  @abc.abstractmethod
  def _execute_streaming(
      self, client: Any, model_name: str, prompt: str, max_output_tokens: int
  ) -> StreamingResult:
    """Executes the streaming benchmark."""
    pass
