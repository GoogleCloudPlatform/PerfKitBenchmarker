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
"""Load driver which runs commands in parallel.

Intentionally has no dependencies on PKB so that it can run on a client VM, but
is also imported by PKB to share flags, testing, & convenience functionality /
constants.
"""

import dataclasses
import json
import logging
import math
import multiprocessing
import os
import subprocess
import time

from absl import app
from absl import flags


_PARALLEL_REQUESTS = flags.DEFINE_integer(
    '_ai_throughput_parallel_requests',
    5,
    'Number of requests to send in parallel at beginning of test. Only used by'
    ' the client VM script.',
)

_REQUEST_COMMAND = flags.DEFINE_string(
    '_ai_throughput_command',
    '',
    'Command to run for each request. Only used by the client VM script.',
)

TEST_DURATION = flags.DEFINE_integer(
    'ai_test_duration',
    60,
    'Number of seconds over which requests are sent. Used for both client VM &'
    ' overall PKB.',
)

BURST_TIME = flags.DEFINE_float(
    'ai_burst_time',
    1.0,
    'Number of seconds between each burst of requests. Used for both client VM'
    ' & overall PKB.',
)

THROW_ON_CLIENT_ERRORS = flags.DEFINE_bool(
    'ai_throw_on_client_errors',
    False,
    'Whether to throw an exception if the client is not powerful enough to'
    ' send the desired QPS. Used for both client VM & overall PKB.',
)


# Sagemaker times out requests if they take longer than 95 seconds.
_FAIL_LATENCY = 95
_QUEUE_WAIT_TIME = _FAIL_LATENCY * 2


@dataclasses.dataclass
class CommandResponse:
  """A response from the command + how long it took."""

  start_time: float
  end_time: float
  response: str | None = None
  error: str | None = None
  status: int = 0


class ClientError(Exception):
  """An error with the client sending requests."""


def main(argv):
  """Sends the load with command line flags & writes results to a file."""
  del argv
  start_time = time.time()
  Run()
  logging.info(
      'Took %s seconds to Run & write responses. throughput_load_driver is'
      ' done.',
      time.time() - start_time,
  )
  # Exit even if some processes are still running.
  os._exit(0)


def Run() -> list[CommandResponse]:
  """Sends the load with command line flags & writes results to a file."""
  responses = BurstRequestsOverTime(
      _REQUEST_COMMAND.value,
      _PARALLEL_REQUESTS.value,
      TEST_DURATION.value,
      BURST_TIME.value,
  )
  file_path = GetOutputFilePath(_PARALLEL_REQUESTS.value)
  logging.info('Writing %s responses to %s', len(responses), file_path)
  responses_dicts = [dataclasses.asdict(response) for response in responses]
  with open(file_path, 'w') as f:
    json.dump({'responses': responses_dicts}, f)
  return responses


def GetOutputFilePath(burst_requests: int) -> str:
  """Returns the output file path for the given burst requests."""
  return f'/tmp/throughput_results_{burst_requests}.json'


def ReadJsonResponses(burst_requests: int) -> list[CommandResponse]:
  """Reads the json responses from the file."""
  with open(GetOutputFilePath(burst_requests), 'r') as f:
    loaded_json = json.load(f)
    responses_dicts = loaded_json['responses']
  responses = [
      CommandResponse(**response_dict) for response_dict in responses_dicts
  ]
  return responses


def GetOverallTimeout() -> float:
  """Returns an overall timeout for the throughput operation."""
  return TEST_DURATION.value + _QUEUE_WAIT_TIME * 2


def GetExpectedNumberResponses(
    burst_requests: int,
    total_duration: int,
    time_between_bursts: float,
) -> int:
  """Returns the expected number of responses for the given parameters."""
  return math.floor(total_duration / time_between_bursts) * burst_requests


def BurstRequestsOverTime(
    command: str,
    burst_requests: int,
    total_duration: int,
    time_between_bursts: float = 1.0,
) -> list[CommandResponse]:
  """Sends X requests to the model in parallel over total_duration seconds."""
  start_time = time.time()
  goal_bursts = math.floor(total_duration / time_between_bursts)
  logging.info(
      'Starting to send %s requests every %s seconds over %s duration %s times',
      burst_requests,
      time_between_bursts,
      total_duration,
      goal_bursts,
  )
  output_queue = multiprocessing.Queue()
  processes = []
  for _ in range(goal_bursts):
    process_start_time = time.time()
    processes += _SendParallelRequests(command, burst_requests, output_queue)
    process_startup_duration = time.time() - process_start_time
    if process_startup_duration > time_between_bursts:
      elapsed_time = time.time() - start_time
      _EncounterClientError(
          f'After running for {elapsed_time} seconds, the client took'
          f' {process_startup_duration} seconds to send'
          f' {burst_requests} requests, which is more than the'
          f' {time_between_bursts} seconds needed to meet QPS. This means the'
          ' client is not powerful enough & client with more CPUs should be'
          ' used.'
      )
    # Wait to send next burst.
    while time.time() - process_start_time < time_between_bursts:
      time.sleep(0.1)

  results = _EmptyQueue(output_queue)
  _WaitForProcesses(processes)
  results = results + _EmptyQueue(output_queue)
  logging.info('Dumping all %s response results: %s', len(results), results)
  if results:
    logging.info('Logging one full response: %s', results[0])
  expected_results = goal_bursts * burst_requests
  if len(results) < expected_results:
    logging.info(
        'Theoretically started %s results but only got %s responses.'
        ' Exact reason is unknown, but this is not entirely unexpected.',
        expected_results,
        len(results),
    )
  return results


def _EmptyQueue(output_queue: multiprocessing.Queue) -> list[CommandResponse]:
  """Empties the queue, with a timeout & returns the results."""
  logging.info('Waiting for all queued results')
  results = []
  queue_start_time = time.time()
  queue_duration = 0
  while not output_queue.empty():
    results.append(output_queue.get())
    queue_duration = time.time() - queue_start_time
    if queue_duration > _QUEUE_WAIT_TIME:
      _EncounterClientError(
          f'Waited more than {_QUEUE_WAIT_TIME} seconds for the queue to'
          ' empty. Exiting, but some data may have been dropped. Collected'
          f' {len(results)} results in the meantime',
      )
      break
  logging.info(
      'All %s queue results collected in: %s.',
      len(results),
      queue_duration,
  )
  return results


def _WaitForProcesses(processes: list[multiprocessing.Process]):
  """Waits for processes to finish, terminating any after waiting too long."""
  process_start_time = time.time()
  process_duration = 0
  original_process_count = len(processes)
  num_joined = 0
  while processes:
    process = processes.pop()
    if process_duration > _QUEUE_WAIT_TIME:
      process.terminate()
    else:
      process.join(_FAIL_LATENCY)
      num_joined += 1
    process_duration = time.time() - process_start_time
  if process_duration > _QUEUE_WAIT_TIME:
    _EncounterClientError(
        f'Waited more than {_QUEUE_WAIT_TIME} seconds for processes to join.'
        ' Exiting, but some data may have been dropped. Collected'
        f' {num_joined} out of'
        f' {original_process_count} total processes with join'
    )
  logging.info(
      'All %s processes finished joining or terimnated in %s seconds.',
      original_process_count,
      process_duration,
  )


def _SendParallelRequests(
    command: str,
    requests: int,
    output_queue: multiprocessing.Queue,
) -> list[multiprocessing.Process]:
  """Sends X requests to the model in parallel."""
  logging.info('Sending %s requests in parallel', requests)
  processes = []
  for _ in range(requests):
    p = multiprocessing.Process(
        target=_TimeCommand, args=(command, output_queue)
    )
    processes.append(p)
    p.start()
  _UnitTestIdleTime()
  return processes


def _UnitTestIdleTime():
  """Sleeps in unit test."""
  pass


def _EncounterClientError(error_msg):
  """Throws or logs a client error."""
  if THROW_ON_CLIENT_ERRORS.value:
    raise ClientError(error_msg)
  logging.warning(error_msg)


def _TimeCommand(
    command: str,
    output_queue: multiprocessing.Queue,
):
  """Times the command & stores length + output in the queue."""
  start_time = time.time()
  response, err, status = _RunCommand(command)
  end_time = time.time()
  output_queue.put(CommandResponse(start_time, end_time, response, err, status))


def _RunCommand(
    command: str,
) -> tuple[str, str, int]:
  """Runs a command and returns stdout, stderr, and return code."""
  result = subprocess.run(
      command.split(' '), check=False, capture_output=True, text=True
  )
  return result.stdout, result.stderr, result.returncode


if __name__ == '__main__':
  app.run(main)
