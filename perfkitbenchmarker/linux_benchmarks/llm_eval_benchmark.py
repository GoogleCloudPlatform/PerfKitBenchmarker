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

"""Runs the llm_eval benchmark.

This benchmark tool evaluates the performance of large language models (LLMs)
from various providers, including Google, OpenAI, and Anthropic.

Features:
- Benchmark LLMs from Google, OpenAI, and Anthropic.
- Discover the latest models from each provider.
- Benchmark multiple prompts in a single run.
- Output results in JSON format.
- Filter out non-text models.
- Discover-only mode to list models without benchmarking.
- Optionally include preview models in the benchmark.
- Consistent temperature setting of 0.0 for deterministic results.

The benchmark can be customized using flags to select the provider,
filter models, and control execution.
"""

import json
import logging
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec as pkb_benchmark_config_spec

BENCHMARK_NAME = 'llm_eval'
BENCHMARK_CONFIG = """
llm_eval:
  description: Runs llm_eval benchmark.
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""

BENCHMARK_DATA = {
    '.env': '6102705161035710e3866e44451531aad83174f71999c7b2a94313747375d0a0'
}
_PROVIDER = flags.DEFINE_enum(
    'llm_eval_provider',
    'google',
    ['google', 'openai', 'anthropic'],
    'The provider to benchmark.',
)
_MAX_MODELS = flags.DEFINE_integer(
    'llm_eval_max_models',
    20,
    'The maximum number of models to test in a single invocation.',
)
_SPECIFIC_MODELS = flags.DEFINE_list(
    'llm_eval_specific_models',
    None,
    'An optional list of models to benchmark, overriding the discovery.',
)
_DISCOVER_MODELS_ONLY = flags.DEFINE_boolean(
    'llm_eval_discover_models_only',
    False,
    'Discover models and exit without benchmarking.',
)
_INCLUDE_PREVIEW = flags.DEFINE_boolean(
    'llm_eval_include_preview',
    False,
    'Include preview models from the benchmark.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Loads and returns the benchmark config.

  Args:
    user_config: A dictionary of the user's command line flags.

  Returns:
    The benchmark configuration.
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(
    benchmark_config: pkb_benchmark_config_spec.BenchmarkConfigSpec,
) -> None:
  """Verifies that the required resources are present.

  Args:
    benchmark_config: The benchmark configuration.

  Raises:
    errors.Config.InvalidValue: On invalid flag settings.
  """
  del benchmark_config
  if _SPECIFIC_MODELS.value:
    if _INCLUDE_PREVIEW.value:
      raise errors.Config.InvalidValue(
          'Cannot use --llm_eval_specific_models with'
          ' --llm_eval_include_preview.'
      )
    if _MAX_MODELS.present:
      raise errors.Config.InvalidValue(
          'Cannot use --llm_eval_specific_models with --llm_eval_max_models.'
      )
  if _DISCOVER_MODELS_ONLY.value:
    if _SPECIFIC_MODELS.value:
      raise errors.Config.InvalidValue(
          'Cannot use --llm_eval_specific_models with'
          ' --llm_eval_discover_models_only.'
      )


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Installs and sets up llm_eval on the target vm.

  Args:
    spec: The benchmark specification.
  """
  del spec  # Unused
  logging.info('Executing llm_eval Prepare function.')
  vm = benchmark_spec.vm_groups['clients'][0]
  vm.Install('pip')
  vm.PushFile(data.ResourcePath('llm_eval'), vm_util.VM_TMP_DIR)
  vm.RemoteCommand(
      f'pip install -r {vm_util.VM_TMP_DIR}/llm_eval/requirements.txt'
  )
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, ['.env'], f'{vm_util.VM_TMP_DIR}/llm_eval'
  )


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs llm_eval on the target vm.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  vm = spec.vm_groups['clients'][0]
  logging.info('Executing llm_eval Run function.')
  cmd_parts = [
      'python3',
      'main.py',
      f'--provider={_PROVIDER.value}',
  ]
  if _DISCOVER_MODELS_ONLY.value:
    cmd_parts.append('--discover_models_only')
    if _INCLUDE_PREVIEW.value:
      cmd_parts.append('--include_preview')
  else:
    if _SPECIFIC_MODELS.value:
      cmd_parts.append(f"--specific_models={','.join(_SPECIFIC_MODELS.value)}")
    else:
      cmd_parts.append(f'--max_models={_MAX_MODELS.value}')
      if _INCLUDE_PREVIEW.value:
        cmd_parts.append('--include_preview')

  run_command = ' '.join(cmd_parts)
  full_cmd = f'cd {vm_util.VM_TMP_DIR}/llm_eval && {run_command}'
  stdout, _ = vm.RemoteCommand(full_cmd)

  if _DISCOVER_MODELS_ONLY.value:
    metadata = {'discovered_models': json.loads(stdout)}
    return [sample.Sample('discovered_models', 0, '', metadata)]
  else:
    results = json.loads(stdout)
    samples = []
    prompts_tested = results.get('prompts_tested', {})
    for model, model_results in results['results'].items():
      for prompt, prompt_results in model_results.items():
        metadata = {
            'model': model,
            'prompt': prompt,
            'provider': _PROVIDER.value,
            'prompt_text': prompts_tested.get(prompt, {}).get('text'),
        }
        if 'error' in prompt_results:
          metadata['error_message'] = prompt_results['error']
          samples.append(sample.Sample('error_count', 1, 'count', metadata))
        else:
          for metric, value in prompt_results.items():
            unit = ''
            if 'tokens' in metric:
              unit = 'tokens'
            elif 'seconds' in metric:
              unit = 'seconds'
            samples.append(sample.Sample(metric, value, unit, metadata))

    return samples


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleans up llm_eval on the target vm.

  Args:
    spec: The benchmark specification.
  """
  del spec  # Unused
  pass
