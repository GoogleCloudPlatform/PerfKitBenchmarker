"""Provider for benchmarking OpenAI models."""

import datetime
import itertools
import os
import re
import time
from typing import List

from logger import get_logger
import openai
from openai import OpenAI

from . import base_provider


MAX_OUTPUT_TOKENS = 1024
logger = get_logger(__name__)


class OpenAIProvider(base_provider.BaseProvider):
  """Provider for benchmarking OpenAI models."""

  def get_models(self, include_preview: bool = False) -> List[str]:
    """Fetches the latest generation of text-based models from OpenAI.

    Args:
      include_preview: Whether to include preview models in the list.

    Returns:
      A list of model names.
    """
    client = self._create_client()
    models = [
        model.id
        for model in client.models.list()
        if 'gpt' in model.id
        and 'audio' not in model.id
        and 'realtime' not in model.id
        and 'instruct' not in model.id
        and 'tts' not in model.id
        and 'transcribe' not in model.id
        and 'image' not in model.id
    ]

    def get_base_model(model_name: str) -> str:
      """Gets the base model name from a full model name.

      Args:
        model_name: The full name of the model.

      Returns:
        The base name of the model.
      """
      base = model_name.split('-preview')[0]
      base = base.split('-latest')[0]

      # Remove date suffixes like -2024-04-09
      base = re.sub(r'-\d{4}-\d{2}-\d{2}$', '', base)

      # Remove version suffixes like -0125 or -1106
      base = re.sub(r'-\d{4}$', '', base)

      # Remove size suffixes like -16k
      base = re.sub(r'-\d+k$', '', base)

      if 'chatgpt-' in base:
        base = base.replace('chatgpt-', 'gpt-')

      return base

    models.sort(key=get_base_model)
    grouped_models = {
        k: list(v) for k, v in itertools.groupby(models, key=get_base_model)
    }

    final_models = []
    for _, model_list in grouped_models.items():
      preview_models = [m for m in model_list if 'preview' in m]
      ga_models = [m for m in model_list if 'preview' not in m]

      if ga_models:
        # Sort by length to find the base model (e.g., 'gpt-4' vs 'gpt-4-0613')
        ga_models.sort(key=len)
        final_models.append(ga_models[0])

      if include_preview and preview_models:

        def extract_date(model_name):
          match = re.search(r'(\d{4}-\d{2}-\d{2})', model_name)
          if match:
            return datetime.datetime.strptime(match.group(1), '%Y-%m-%d')

          match = re.search(r'-(\d{4})-preview', model_name)
          if match:
            try:
              # Handle MMDD format
              return datetime.datetime.strptime(match.group(1), '%m%d').replace(
                  year=datetime.datetime.now().year
              )
            except ValueError:
              pass
          return datetime.datetime.min

        preview_models.sort(key=extract_date, reverse=True)
        final_models.extend(preview_models[:2])

    if not include_preview:
      return sorted(list(set([m for m in final_models if 'preview' not in m])))

    return sorted(list(set(final_models)))

  def _create_client(self) -> OpenAI:
    """Creates and returns an OpenAI client."""
    return OpenAI(api_key=os.environ['OPENAI_API_KEY'])

  def _count_input_tokens(
      self, client: OpenAI, prompt: str, model_name: str
  ) -> int:
    """Counts the number of tokens in the input prompt."""
    # For OpenAI, we can only get the input token count from the response of a
    # non-streaming call. We will retrieve it there and pass it back.
    return 0

  def _execute_non_streaming(
      self,
      client: OpenAI,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> base_provider.NonStreamingResult:
    """Executes the non-streaming benchmark."""
    logger.debug('Starting non-streaming benchmark...')
    start_time = time.time()
    tokens_to_use = (
        max_output_tokens
        if max_output_tokens is not None
        else MAX_OUTPUT_TOKENS
    )
    params = {
        'model': model_name,
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': base_provider.TEMPERATURE,
    }

    if 'nano' in model_name or 'gpt-5' in model_name:
      params['max_completion_tokens'] = tokens_to_use
    else:
      params['max_tokens'] = tokens_to_use

    logger.debug(f'Non-streaming request params: {params}')
    try:
      response = client.chat.completions.create(**params)
      end_time = time.time()
      logger.debug(f'Non-streaming response: {response}')
      return base_provider.NonStreamingResult(
          total_time_seconds=round(end_time - start_time, 2),
          output_tokens=response.usage.completion_tokens,
      )
    except openai.OpenAIError as e:
      logger.debug(f'Non-streaming benchmark failed: {e}')
      return base_provider.NonStreamingResult(error=str(e))

  def _execute_streaming(
      self,
      client: OpenAI,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> base_provider.StreamingResult:
    """Executes the streaming benchmark."""
    logger.debug('Starting streaming benchmark...')
    start_time = time.time()
    first_token_time = None
    tokens_to_use = (
        max_output_tokens
        if max_output_tokens is not None
        else MAX_OUTPUT_TOKENS
    )
    params = {
        'model': model_name,
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': base_provider.TEMPERATURE,
    }

    if 'nano' in model_name or 'gpt-5' in model_name:
      params['max_completion_tokens'] = tokens_to_use
    else:
      params['max_tokens'] = tokens_to_use

    logger.debug(f'Streaming request params: {params}')
    try:
      stream = client.chat.completions.create(**params, stream=True)
      output_text = ''
      for chunk in stream:
        if first_token_time is None:
          first_token_time = time.time()
        if chunk.choices[0].delta.content:
          output_text += chunk.choices[0].delta.content
      end_time = time.time()
      logger.debug('Finished streaming benchmark.')

      # Re-encode to count tokens for streaming
      re_encode_params = {
          'model': model_name,
          'messages': [{'role': 'user', 'content': output_text}],
      }

      if 'nano' in model_name or 'gpt-5' in model_name:
        re_encode_params['max_completion_tokens'] = tokens_to_use
      else:
        re_encode_params['max_tokens'] = tokens_to_use

      logger.debug(f'Re-encoding request params: {re_encode_params}')
      response = client.chat.completions.create(**re_encode_params)
      logger.debug(f'Re-encoding response: {response}')
      output_tokens = response.usage.prompt_tokens

      return base_provider.StreamingResult(
          time_to_first_token_seconds=round(first_token_time - start_time, 2),
          total_time_seconds=round(end_time - start_time, 2),
          output_tokens=output_tokens,
      )
    except openai.OpenAIError as e:
      logger.debug(f'Streaming benchmark failed: {e}')
      return base_provider.StreamingResult(error=str(e))
