"""Provider for benchmarking Anthropic models."""

import os
import time
from typing import Any, Dict, List

from anthropic import Anthropic

from . import base_provider

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
    client = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    return [
        model.id
        for model in client.models.list()
        if 'claude' in model.id
        and 'instant' not in model.id
        and '2024' not in model.id
    ]

  def benchmark_model(
      self, model_name: str, prompt: str, prompt_id: str, max_output_tokens: int
  ) -> Dict[str, Any]:
    super().benchmark_model(model_name, prompt, prompt_id, max_output_tokens)
    client = Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])
    results = {}

    # Non-streaming
    start_time = time.time()
    response = client.messages.create(
        model=model_name,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[{'role': 'user', 'content': prompt}],
        temperature=base_provider.TEMPERATURE,
    )
    end_time = time.time()
    results['non_streaming_full_response_time_in_seconds'] = round(
        end_time - start_time, 2
    )
    results['input_request_tokens'] = response.usage.input_tokens
    results['non_streaming_full_response_output_tokens'] = (
        response.usage.output_tokens
    )

    # Streaming
    start_time = time.time()
    first_token_time = None
    output_text = ''
    with client.messages.stream(
        model=model_name,
        max_tokens=MAX_OUTPUT_TOKENS,
        messages=[{'role': 'user', 'content': prompt}],
        temperature=base_provider.TEMPERATURE,
    ) as stream:
      for text in stream.text_stream:
        if first_token_time is None:
          first_token_time = time.time()
        output_text += text
    end_time = time.time()

    results['streaming_time_to_first_token_in_seconds'] = round(
        first_token_time - start_time, 2
    )
    results['streaming_full_response_time_in_seconds'] = round(
        end_time - start_time, 2
    )
    results['streaming_full_response_output_tokens'] = len(output_text)

    return results
