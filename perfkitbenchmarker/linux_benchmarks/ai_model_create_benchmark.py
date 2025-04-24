# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Benchmark to measure the creation time of a managed AI Model."""

import logging
from typing import Any
from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources import managed_ai_model

BENCHMARK_NAME = 'ai_model_create'
BENCHMARK_CONFIG = """
ai_model_create:
  description: >
    Times creation of a managed AI model.
  ai_model:
    model_name: 'llama2'
    model_size: '7b'
    cloud: 'GCP'
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: 1
  flags:
    gcloud_scopes: cloud-platform
"""


_CREATE_SECOND_MODEL = flags.DEFINE_boolean(
    'create_second_model',
    False,
    'Whether to create & benchmark a second model in addition to the first.',
)

_VALIDATE_EXISTING_MODELS = flags.DEFINE_boolean(
    'validate_existing_models',
    False,
    'Whether to fail the benchmark if there are other models in the region.',
)


def GetConfig(user_config: dict[Any, Any]) -> dict[Any, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  del benchmark_spec


def _ValidateExistingModels(
    ai_model: managed_ai_model.BaseManagedAiModel, expected_count: int
) -> int:
  """Validates that no other models are running in the region."""
  endpoints = ai_model.ListExistingEndpoints()
  # The presence of other models in a region changes startup performance.
  if len(endpoints) != expected_count:
    message = (
        f'Expected {expected_count} model(s) but found all these models:'
        f' {endpoints}.'
    )
    if _VALIDATE_EXISTING_MODELS.value:
      raise errors.Benchmarks.KnownIntermittentError(message)
    else:
      message += ' Continuing benchmark as validate_existing_models is False.'
      logging.warning(message)
  return len(endpoints)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the example benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  logging.info('Running Run phase & gathering response times for model 1')
  model1 = benchmark_spec.ai_model
  assert model1
  num_endpoints = _ValidateExistingModels(model1, 1)
  SendPromptsForModel(model1)

  if not _CREATE_SECOND_MODEL.value:
    logging.info('Only benchmarking one model by flag; returning')
    return []
  if num_endpoints != 1:
    logging.warning(
        'Not creating a second model as there were already other models in the'
        ' region before the first one this benchmark created. Ending benchmark'
        ' with only one set of results.'
    )
    return []

  logging.info('Creating model 2 & gathering response times')
  model2 = model1.InitializeNewModel()
  model2.Create()
  benchmark_spec.resources.append(model2)
  SendPromptsForModel(model2)
  # All resource samples gathered by benchmark_spec automatically.
  return []


def SendPromptsForModel(
    ai_model: managed_ai_model.BaseManagedAiModel,
):
  _SendPrompt(ai_model, 'Why do crabs walk sideways?')
  _SendPrompt(ai_model, 'How can I save more money each month?')


def _SendPrompt(
    ai_model: managed_ai_model.BaseManagedAiModel,
    prompt: str,
):
  """Sends a prompt to the model and prints the response."""
  responses = ai_model.SendPrompt(
      prompt=prompt, max_tokens=512, temperature=0.8
  )
  for response in responses:
    logging.info('Sent request & got response: %s', response)


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleanup resources to their original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  logging.info('Running Cleanup phase of the benchmark')
  del benchmark_spec
