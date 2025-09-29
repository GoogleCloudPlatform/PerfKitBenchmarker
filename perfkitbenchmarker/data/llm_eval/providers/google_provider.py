"""Provider for benchmarking Google models."""

import datetime
import itertools
import os
import re
import time
from typing import Any, List

from google import genai
from google.api_core import exceptions
from google.genai import types
from logger import get_logger

from . import base_provider


MAX_OUTPUT_TOKENS = 1024
logger = get_logger(__name__)

SAFETY_SETTINGS = [
    {
        'category': types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        'threshold': types.HarmBlockThreshold.BLOCK_NONE,
    },
    {
        'category': types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        'threshold': types.HarmBlockThreshold.BLOCK_NONE,
    },
    {
        'category': types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        'threshold': types.HarmBlockThreshold.BLOCK_NONE,
    },
    {
        'category': types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        'threshold': types.HarmBlockThreshold.BLOCK_NONE,
    },
]


class GoogleProvider(base_provider.BaseProvider):
  """Provider for benchmarking Google models."""

  def get_models(self, include_preview: bool = False) -> List[str]:
    """Fetches the latest generation of text-based models from Google.

    Args:
      include_preview: Whether to include preview models in the list.

    Returns:
      A list of model names.
    """
    client = self._create_client()
    models = []
    for m in client.models.list():
      if (
          'generateContent' in m.supported_actions
          and '2.5' in m.name
          and 'tts' not in m.name
          and 'image' not in m.name
      ):
        models.append(m.name)

    if not include_preview:
      return [m for m in models if 'preview' not in m]

    def get_base_model(model_name):
      return model_name.split('-preview-')[0]

    models.sort(key=get_base_model)
    grouped_models = {
        k: list(v) for k, v in itertools.groupby(models, key=get_base_model)
    }

    final_models = []
    for _, model_list in grouped_models.items():
      timed_models = [m for m in model_list if 'preview' in m]
      other_models = [m for m in model_list if 'preview' not in m]

      def extract_date(model_name: str) -> datetime.datetime:
        """Extracts the date from a model name.

        Args:
          model_name: The name of the model.

        Returns:
          The date from the model name, or the minimum datetime if no date is
          found.
        """
        match = re.search(r'(\d{2}-\d{2})', model_name)
        if match:
          return datetime.datetime.strptime(match.group(1), '%m-%d')
        return datetime.datetime.min

      timed_models.sort(key=extract_date, reverse=True)
      final_models.extend(other_models)
      final_models.extend(timed_models[:2])

    return final_models

  def _create_client(self) -> genai.Client:
    """Creates and returns a Google GenAI client."""
    return genai.Client(api_key=os.environ['GOOGLE_API_KEY'])

  def _count_input_tokens(
      self, client: genai.Client, prompt: str, model_name: str
  ) -> int:
    """Counts the number of tokens in the input prompt."""
    return client.models.count_tokens(
        model=model_name, contents=prompt
    ).total_tokens

  def _execute_non_streaming(
      self,
      client: genai.Client,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> base_provider.NonStreamingResult:
    """Executes the non-streaming benchmark."""
    logger.debug('Starting non-streaming benchmark...')
    start_time = time.time()
    try:
      config = types.GenerateContentConfig(
          temperature=base_provider.TEMPERATURE
      )
      if max_output_tokens:
        config.max_output_tokens = max_output_tokens
      config.safety_settings = SAFETY_SETTINGS
      response = client.models.generate_content(
          model=model_name,
          contents=prompt,
          config=config,
      )
      end_time = time.time()
      logger.debug(f'Response: {response}')
      if not response.text:
        logger.debug('Non-streaming benchmark failed.')
        if response.candidates:
          error_msg = (
              'No response text found. Finish reason:'
              f' {response.candidates[0].finish_reason}, Safety Ratings:'
              f' {response.candidates[0].safety_ratings}'
          )
        else:
          block_reason = (
              response.prompt_feedback.block_reason
              if response.prompt_feedback
              else 'Unknown'
          )
          error_msg = (
              'No response text found. Finish reason:'
              f' {block_reason}'
          )
        return base_provider.NonStreamingResult(error=error_msg)
    except exceptions.GoogleAPIError as e:
      logger.debug('Non-streaming benchmark failed.')
      return base_provider.NonStreamingResult(error=str(e))
    logger.debug('Finished non-streaming benchmark.')

    return base_provider.NonStreamingResult(
        total_time_seconds=round(end_time - start_time, 2),
        output_tokens=client.models.count_tokens(
            model=model_name, contents=response.text
        ).total_tokens,
    )

  def _execute_streaming(
      self,
      client: genai.Client,
      model_name: str,
      prompt: str,
      max_output_tokens: int,
  ) -> base_provider.StreamingResult:
    """Executes the streaming benchmark."""
    logger.debug('Starting streaming benchmark...')
    start_time = time.time()
    first_token_time = None
    config = types.GenerateContentConfig(temperature=base_provider.TEMPERATURE)
    if max_output_tokens:
      config.max_output_tokens = max_output_tokens
    config.safety_settings = SAFETY_SETTINGS
    stream = client.models.generate_content_stream(
        model=model_name,
        contents=prompt,
        config=config,
    )
    output_text = ''
    try:
      for chunk in stream:
        if first_token_time is None:
          first_token_time = time.time()
        if chunk.text:
          output_text += chunk.text
    except exceptions.GoogleAPIError as e:
      logger.debug('Streaming benchmark failed.')
      return base_provider.StreamingResult(error=str(e))
    end_time = time.time()
    logger.debug('Finished streaming benchmark.')

    return base_provider.StreamingResult(
        time_to_first_token_seconds=round(first_token_time - start_time, 2),
        total_time_seconds=round(end_time - start_time, 2),
        output_tokens=client.models.count_tokens(
            model=model_name, contents=output_text
        ).total_tokens,
    )
