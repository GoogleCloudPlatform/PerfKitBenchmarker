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

import dataclasses
import logging
import multiprocessing
import statistics
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources import managed_ai_model


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

_PARALLEL_REQUESTS = flags.DEFINE_integer(
    'ai_parallel_requests', 5, 'Number of requests to send in parallel.'
)

_TEST_DURATION = flags.DEFINE_integer(
    'ai_test_duration', 60, 'Number of seconds over which requests are sent.'
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


@dataclasses.dataclass
class ModelResponse:
  """A response from the model."""

  start_time: float
  end_time: float
  response: str | None = None
  status: int = 0


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the example benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  logging.info('Running Run phase & finding throughput')
  model = benchmark_spec.ai_model
  # Label whether it's the first model or not.
  endpoints = model.ListExistingEndpoints()
  model.metadata.update({'First Model': len(endpoints) == 1})
  # Confirm we can send one request.
  _SendPrompt(model, 'Why do crabs walk sideways?')
  responses = SendQpsOverTime(
      model, _PARALLEL_REQUESTS.value, _TEST_DURATION.value
  )
  return _AggregateResponses(responses, model)


def _AggregateResponses(
    responses: list[ModelResponse], model: managed_ai_model.BaseManagedAiModel
) -> list[sample.Sample]:
  """Aggregates the responses into samples."""
  durations = [
      response.end_time - response.start_time
      for response in responses
      if response.status == 0
  ]
  logging.info('Response durations dump: %s', durations)
  samples = []
  samples.append(
      sample.Sample(
          'success_rate',
          len(durations) / len(responses) * 100.0,
          'percent',
          model.GetResourceMetadata(),
      )
  )
  samples.append(
      sample.Sample(
          'total_responses',
          len(responses),
          'count',
          model.GetResourceMetadata(),
      )
  )
  samples.append(
      sample.Sample(
          'median_response_time',
          statistics.median(durations),
          'seconds',
          model.GetResourceMetadata(),
      )
  )
  samples.append(
      sample.Sample(
          'mean_response_time',
          statistics.mean(durations),
          'seconds',
          model.GetResourceMetadata(),
      )
  )
  return samples


def SendParallelRequests(
    ai_model: managed_ai_model.BaseManagedAiModel,
    requests: int,
    output_queue: multiprocessing.Queue,
) -> list[multiprocessing.Process]:
  """Sends X requests to the model in parallel."""
  logging.info('Sending %s requests in parallel', requests)
  processes = []
  for _ in range(requests):
    p = multiprocessing.Process(
        target=TimePromptsForModel, args=(ai_model, output_queue)
    )
    processes.append(p)
    p.start()
  _UnitTestIdleTime()
  return processes


def _UnitTestIdleTime():
  """Sleeps in unit test."""
  pass


def SendQpsOverTime(
    ai_model: managed_ai_model.BaseManagedAiModel,
    qps: int,
    duration: int,
) -> list[ModelResponse]:
  """Sends X requests to the model in parallel over duration seconds."""
  start_time = time.time()
  logging.info('Starting to send %s qps over %s duration', qps, duration)
  output_queue = multiprocessing.Queue()
  processes = []
  goal_bursts = duration
  for _ in range(goal_bursts):
    process_start_time = time.time()
    processes += SendParallelRequests(ai_model, qps, output_queue)
    process_startup_duration = time.time() - process_start_time
    if process_startup_duration > 1:
      elapsed_time = time.time() - start_time
      raise errors.Benchmarks.RunError(
          f'After running for {elapsed_time} seconds, the client took'
          f' {elapsed_time} seconds to send {qps} requests, which is more than'
          ' the one second needed to meet QPS. This means the client is not'
          ' powerful enough & a CPU with more clients should be used.'
      )
    # Wait 1 second
    while time.time() - process_start_time < 1:
      time.sleep(0.1)
  # Wait for all processes to finish.
  for p in processes:
    p.join()
  # Allocate list for results:
  results = []
  while not output_queue.empty():
    results.append(output_queue.get())
  logging.info('All processes finished in: %s', time.time() - start_time)
  logging.info('Dumping all response results: %s', results)
  expected_results = duration * qps
  if len(results) < expected_results:
    raise errors.Benchmarks.RunError(
        f'Expected to get {expected_results} results but only got'
        f' {len(results)} from {len(processes)} processes. Some data has been'
        ' dropped.'
    )
  return results


def TimePromptsForModel(
    ai_model: managed_ai_model.BaseManagedAiModel,
    output_queue: multiprocessing.Queue,
):
  """Times the prompts for the model & stores timing in the output queue."""
  start_time = time.time()
  status = 0
  response = None
  try:
    response = _SendPrompt(ai_model, 'Why do crabs walk sideways?')
    end_time = time.time()
  except errors.Resource.GetError as ex:
    end_time = time.time()
    logging.info('Failed to send prompt: %s', ex)
    status = 1
  output_queue.put(ModelResponse(start_time, end_time, response, status))


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
