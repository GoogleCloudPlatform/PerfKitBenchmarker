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
"""Benchmark to measure the throughput of a managed AI Model's inference."""

import logging
import statistics
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources import managed_ai_model
from perfkitbenchmarker.scripts import throughput_load_driver


BENCHMARK_NAME = 'ai_model_throughput'
BENCHMARK_CONFIG = """
ai_model_throughput:
  description: >
    Records the throughput of a model.
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

_STARTING_REQUESTS = flags.DEFINE_integer(
    'ai_starting_requests',
    5,
    'Number of requests to send in parallel at beginning of test.',
)

_MAX_PARALLEL_REQUESTS = flags.DEFINE_integer(
    'ai_max_requests',
    None,
    'Max number of requests to send in parallel before ending the test. Set to'
    ' None or the same number as starting requests to effectively run a QPS'
    ' test at only that value.',
)


_QUEUE_WAIT_TIME = 10 * 60
# Sagemaker times out requests if they take longer than 95 seconds.
_FAIL_LATENCY = 95

_SHARED_REQUEST = 'Why do crabs walk sideways?'


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


def CheckPrerequisites(benchmark_config):
  del benchmark_config
  if (
      _MAX_PARALLEL_REQUESTS.value
      and _MAX_PARALLEL_REQUESTS.value < _STARTING_REQUESTS.value
  ):
    raise errors.Config.InvalidValue(
        'ai_max_requests must be None or >= ai_starting_requests. Got:'
        f' {_MAX_PARALLEL_REQUESTS.value} as compared to'
        f' {_STARTING_REQUESTS.value}'
    )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the throughput benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  logging.info('Running Run phase & finding throughput')
  model = benchmark_spec.ai_model
  assert model
  # Label whether it's the first model or not.
  endpoints = model.ListExistingEndpoints()
  model.metadata.update({'First Model': len(endpoints) == 1})
  # Confirm we can send one request.
  model.SendPrompt(_SHARED_REQUEST, 512, 1.0)
  return FindMaxThroughput(model)


def FindMaxThroughput(
    ai_model: managed_ai_model.BaseManagedAiModel,
) -> list[sample.Sample]:
  """Finds the max throughput for the model."""
  command = ai_model.GetPromptCommand(_SHARED_REQUEST, 512, 1.0)
  logging.info(
      'Finding max throughput & calling models with command: %s', command
  )
  step = 3
  last_responses = []
  burst_requests = _STARTING_REQUESTS.value
  max_requests = _MAX_PARALLEL_REQUESTS.value or (_STARTING_REQUESTS.value + 1)
  failed_responses = []
  responses = []
  for burst_requests in range(_STARTING_REQUESTS.value, max_requests, step):
    logging.info('Sending %s qps', burst_requests)
    responses = throughput_load_driver.BurstRequestsOverTime(
        command,
        burst_requests,
        throughput_load_driver.TEST_DURATION.value,
        throughput_load_driver.BURST_TIME.value,
    )
    failed_responses = [
        response
        for response in responses
        if response.status != 0
        or response.end_time - response.start_time > _FAIL_LATENCY
    ]
    if failed_responses:
      logging.info(
          'Reached failure point when trying %s bursts with %s failures',
          burst_requests,
          len(failed_responses),
      )
      break
    last_responses = responses
  if not last_responses:
    logging.warning(
        'The very first QPS tried had errors. Probably a smaller staring'
        ' QPS needs to be chosen.',
    )
    return _AggregateResponses(
        responses, failed_responses, ai_model, _STARTING_REQUESTS.value
    )
  last_successful_bursts = burst_requests
  if failed_responses:
    # We just failed, so output results from the last successful QPS.
    last_successful_bursts = burst_requests - step
  else:
    logging.warning(
        'Reached max burst value of %s without failures. Ending the test &'
        ' outputting results from the highest run QPS.',
        last_successful_bursts,
    )
  samples = _AggregateResponses(
      last_responses, failed_responses, ai_model, last_successful_bursts
  )
  assert samples
  metadata = samples[0].metadata
  samples.append(
      sample.Sample(
          'max_throughput',
          last_successful_bursts / throughput_load_driver.BURST_TIME.value,
          'count',
          metadata,
      )
  )
  return samples


def _AggregateResponses(
    responses: list[throughput_load_driver.CommandResponse],
    failed_responses: list[throughput_load_driver.CommandResponse],
    model: managed_ai_model.BaseManagedAiModel,
    burst_requests: int,
) -> list[sample.Sample]:
  """Aggregates the responses into samples."""
  successful_durations = [
      response.end_time - response.start_time for response in responses
  ]
  logging.info('Response durations dump: %s', successful_durations)
  failed_durations = [
      response.end_time - response.start_time for response in failed_responses
  ]
  logging.info('Failed response durations dump: %s', failed_durations)
  metadata = model.GetResourceMetadata()
  effective_qps = burst_requests / throughput_load_driver.BURST_TIME.value
  metadata.update({
      'parallel_requests': burst_requests,
      'test_duration': throughput_load_driver.TEST_DURATION.value,
      'burst_time': throughput_load_driver.BURST_TIME.value,
      'effective_qps': effective_qps,
  })
  samples = []
  if failed_durations:
    samples.append(
        sample.Sample(
            'failure_median_response_time',
            statistics.median(failed_durations),
            'seconds',
            metadata,
        )
    )
    samples.append(
        sample.Sample(
            'num_failures',
            len(failed_durations),
            'count',
            metadata,
        )
    )
  if not successful_durations:
    return samples
  samples.append(
      sample.Sample(
          'success_rate',
          len(successful_durations)
          / (len(successful_durations) + len(failed_durations))
          * 100.0,
          'percent',
          metadata,
      )
  )
  samples.append(
      sample.Sample(
          'num_responses',
          len(responses),
          'count',
          metadata,
      )
  )
  samples.append(
      sample.Sample(
          'median_response_time',
          statistics.median(successful_durations),
          'seconds',
          metadata,
      )
  )
  samples.append(
      sample.Sample(
          'mean_response_time',
          statistics.mean(successful_durations),
          'seconds',
          metadata,
      )
  )
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleanup resources to their original state.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  logging.info('Running Cleanup phase of the benchmark')
  del benchmark_spec
