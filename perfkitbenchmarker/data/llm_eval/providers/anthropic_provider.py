"""Provider for benchmarking Anthropic models."""

import os
import time
from typing import Any, List

from anthropic import Anthropic
from anthropic import AnthropicError

from . import base_provider
from .base_provider import NonStreamingResult
from .base_provider import StreamingResult

MAX_OUTPUT_TOKENS = 1024


class AnthropicProvider(base_provider.BaseProvider):
  """Provider for benchmarking Anthropic models."""

  def get_models(self, include_preview: bool = False) -> List[str]:
    """Fetches the latest generation of text-based models from Anthropic.

    Args:
      include_preview: Whether to include preview models in the list.

    Returns:
      A list of model names.
    """
    client = self._create_client()
    return [
        model.id
        for model in client.models.list()
        if 'claude' in model.id
        and 'instant' not in model.id
        and '2024' not in model.id
    ]

  def _create_client(self) -> Anthropic:
    """Creates and returns an Anthropic client."""
    return Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

  def _count_input_tokens(
      self, client: Anthropic, prompt: str, model_name: str
  ) -> int:
    """Counts the number of tokens in the input prompt."""
    # For Anthropic, we can only get the input token count from the response of
    # a non-streaming call. We will retrieve it there and pass it back.
    return 0

  def _execute_non_streaming(
      self,
      client: Anthropic,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> NonStreamingResult:
    """Executes the non-streaming benchmark."""
    start_time = time.time()
    try:
      response = client.messages.create(
          model=model_name,
          max_tokens=max_output_tokens or MAX_OUTPUT_TOKENS,
          messages=[{'role': 'user', 'content': prompt}],
          temperature=base_provider.TEMPERATURE,
      )
      end_time = time.time()
      return NonStreamingResult(
          total_time_seconds=round(end_time - start_time, 2),
          output_tokens=response.usage.output_tokens,
      )
    except AnthropicError as e:
      return NonStreamingResult(error=str(e))

  def _execute_streaming(
      self,
      client: Anthropic,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> StreamingResult:
    """Executes the streaming benchmark."""
    start_time = time.time()
    first_token_time = None
    output_text = ''
    try:
      with client.messages.stream(
          model=model_name,
          max_tokens=max_output_tokens or MAX_OUTPUT_TOKENS,
          messages=[{'role': 'user', 'content': prompt}],
          temperature=base_provider.TEMPERATURE,
      ) as stream:
        for text in stream.text_stream:
          if first_token_time is None:
            first_token_time = time.time()
          output_text += text
      end_time = time.time()
      return StreamingResult(
          time_to_first_token_seconds=round(first_token_time - start_time, 2),
          total_time_seconds=round(end_time - start_time, 2),
          output_tokens=len(output_text),
      )
    except AnthropicError as e:
      return StreamingResult(error=str(e))
