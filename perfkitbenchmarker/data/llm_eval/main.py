"""Main script for running LLM benchmarks.

This script provides a command-line interface for benchmarking large language
models from various providers. It supports model discovery, benchmarking
specific models, and various output formats.
"""

import enum
import json
from typing import Any, Dict, List, Tuple

from absl import app
from absl import flags
import dotenv
from logger import get_logger
from logger import set_logging_level
from prompts import OPEN_ENDED_PROMPTS
from prompts import SUMMARIZATION_PROMPTS
from providers.anthropic_provider import AnthropicProvider
from providers.base_provider import BaseProvider
from providers.google_provider import GoogleProvider
from providers.openai_provider import OpenAIProvider

dotenv.load_dotenv()

FLAGS = flags.FLAGS
logger = get_logger(__name__)


class Provider(enum.Enum):
  GOOGLE = 'google'
  OPENAI = 'openai'
  ANTHROPIC = 'anthropic'


flags.DEFINE_enum(
    'provider',
    None,
    [p.value for p in Provider],
    'The provider to benchmark.',
)
flags.mark_flag_as_required('provider')
flags.DEFINE_integer(
    'max_models',
    -1,
    (
        'The maximum number of models to test in a single invocation. This '
        'flag has no effect if the --models flag is specified.'
    ),
)
flags.DEFINE_list(
    'specific_models',
    None,
    'An optional list of models to benchmark, overriding the discovery.',
)
flags.DEFINE_boolean(
    'discover_models_only',
    False,
    'Discover models and exit without benchmarking. Can be used with'
    ' --include_preview.',
)
flags.DEFINE_boolean(
    'include_preview',
    False,
    'Include preview models from the benchmark. This flag has no effect when'
    ' --models is used.',
)
flags.DEFINE_boolean('debug', False, 'Enable debug logging.')


PROVIDER_MAPPING = {
    Provider.GOOGLE.value: GoogleProvider,
    Provider.OPENAI.value: OpenAIProvider,
    Provider.ANTHROPIC.value: AnthropicProvider,
}


def _get_models_to_test(provider: BaseProvider) -> Tuple[List[str], List[str]]:
  """Gets the list of models to test based on the command-line flags.

  Args:
    provider: The provider to get models from.

  Returns:
    A tuple containing the list of models to test and the list of skipped
    models.
  """
  if FLAGS.specific_models:
    if FLAGS.include_preview:
      raise ValueError('Cannot use --specific_models with --include_preview.')
    if FLAGS.max_models != 20:
      raise ValueError('Cannot use --specific_models with --max_models.')
    models_to_test = FLAGS.specific_models
    skipped_models = []
  else:
    all_models = provider.get_models(include_preview=FLAGS.include_preview)
    models_to_test = all_models[: FLAGS.max_models]
    skipped_models = all_models[FLAGS.max_models :]

  logger.debug(f'Models to be benchmarked: {models_to_test}')

  return models_to_test, skipped_models


def _get_provider() -> BaseProvider:
  """Gets the provider instance based on the command-line flags.

  Returns:
    An instance of the selected provider.
  """
  logger.debug(f'Starting benchmark for provider: {FLAGS.provider}')

  provider_class = PROVIDER_MAPPING.get(FLAGS.provider)
  if not provider_class:
    raise ValueError(f'Invalid provider: {FLAGS.provider}')
  return provider_class()


def _handle_discover_only(models_to_test: List[str]) -> bool:
  """Handles the discover_models_only flag.

  Args:
    models_to_test: The list of models to test.

  Returns:
    True if the script should exit after discovering models, False otherwise.
  """
  if not FLAGS.discover_models_only:
    return False

  if FLAGS.specific_models:
    raise ValueError(
        'Cannot use --specific_models with --discover_models_only.'
    )

  logger.debug('Discovering models...')
  print(json.dumps({'models': models_to_test}, indent=2))
  return True


def _run_benchmarks(
    provider: BaseProvider, models_to_test: List[str]
) -> Dict[str, Any]:
  """Runs the benchmarks for the given models.

  Args:
    provider: The provider to use for benchmarking.
    models_to_test: The list of models to test.

  Returns:
    A dictionary of benchmark results.
  """
  results = {}
  all_prompts = {**SUMMARIZATION_PROMPTS, **OPEN_ENDED_PROMPTS}
  for model_name in models_to_test:
    logger.debug(f'Benchmarking model: {model_name}')
    results[model_name] = {}
    for prompt_id, prompt_data in all_prompts.items():
      try:
        results[model_name][prompt_id] = provider.benchmark_model(
            model_name,
            prompt_data['text'],
            prompt_id,
            prompt_data['max_tokens'],
        ).as_dict()
      except Exception as e:  # pylint: disable=broad-exception-caught
        # Catching broad exception to ensure one failed prompt doesn't stop
        # the entire benchmark run.
        logger.debug(f"  Error benchmarking prompt '{prompt_id}': {e}")
        results[model_name][prompt_id] = {'error': str(e)}
    logger.debug(f'Finished benchmarking model: {model_name}')
  return results


def main(argv: List[str]) -> None:
  """Main function for the benchmark script.

  Args:
    argv: The command-line arguments.
  """
  del argv  # Unused
  set_logging_level(FLAGS.debug)
  provider = _get_provider()
  models_to_test, skipped_models = _get_models_to_test(provider)

  if _handle_discover_only(models_to_test):
    return

  results = _run_benchmarks(provider, models_to_test)

  logger.debug('Benchmark complete. Finalizing output.')

  num_skipped_models = len(skipped_models)
  output = {
      'results': results,
      'models_skipped': num_skipped_models,
      'skipped_models': skipped_models,
      'prompts_tested': {**SUMMARIZATION_PROMPTS, **OPEN_ENDED_PROMPTS},
  }

  print(json.dumps(output, indent=2))


if __name__ == '__main__':
  app.run(main)
