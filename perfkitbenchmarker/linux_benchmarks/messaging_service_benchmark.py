# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Messaging Service benchmark.

This benchmark runs in a client VM and benchmarks messaging services from
different cloud providers. It measures latency to publish/pull messages from
the client VM.

This benchmark first send a command to the client VM to publish messages. When
that completes it send commands to pull the messages. Measuring latency of
single message publish/pull in each scenario:
  - publish: it publishes N messages of size X (N and X can be specified
    with number_of_messages, and message_size FLAGS respectively). It
    measures the latency between each call to publish the message and the
    message being successfully published.
  - pull: It pulls N messages 1 by 1. It measures the latency of:
      - A call to pull the message and the message being received.
      - A call to pull the message and the message being received and
      acknowledged.
"""

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'messaging_service'
BENCHMARK_CONFIG = """
messaging_service:
  description: messaging_service benchmark
  vm_groups:
    default:
      os_type: debian10
      vm_spec:
        AWS:
          machine_type: m5.2xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_D8d_v4
          zone: eastus
        GCP:
          machine_type: n2-standard-8
          zone: us-central1-a
  messaging_service:
    delivery: pull
"""

SINGLE_OP = 'single_op'
END_TO_END = 'end_to_end'
MEASUREMENT_CHOICES = [SINGLE_OP, END_TO_END]

FLAGS = flags.FLAGS

_MEASUREMENT = flags.DEFINE_enum(
    'messaging_service_measurement',
    'single_op',
    MEASUREMENT_CHOICES,
    help='Way to measure latency.')
_NUMBER_OF_MESSAGES = flags.DEFINE_integer(
    'messaging_service_number_of_messages',
    100,
    help='Number of messages to use on benchmark.')
_MESSAGE_SIZE = flags.DEFINE_integer(
    'messaging_service_message_size',
    10,
    help='Number of characters to have in a message. '
    "Ex: 1: 'A', 2: 'AA', ...")


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _CreateSamples(results: Dict[str, Any], number_of_messages: int,
                   message_size: int, cloud: str) -> List[sample.Sample]:
  """Handles sample creation from benchmark_scenario results."""
  samples = []
  common_metadata = {
      'number_of_messages': number_of_messages,
      'message_size': message_size,
      'cloud': cloud
  }

  for metric_name in results:
    metric_value = results[metric_name]['value']
    metric_unit = results[metric_name]['unit']
    metric_metadata = results[metric_name]['metadata']
    metric_metadata.update(common_metadata)

    # aggregated metrics, such as: mean, p50, p99...
    samples.append(
        sample.Sample(metric_name, metric_value, metric_unit, metric_metadata))

  return samples


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the client VM.

  Runs the prepare function from get_instance. It prepares the cloud environment
  with resource creation (for GCP Cloud Pub/Sub it creates topic and
  subscription) and prepares the client VM with packages and files needed to
  run the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  benchmark_spec.messaging_service.PrepareClientVm()


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Measure the latency to publish, pull, or publish and pull messages.

  Runs the run function from get_instance. It runs the benchmark specified with
  the flag: 'messaging_service_benchmark' from the client VM.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    List of samples. Produced when running the benchmark from client VM
    (on 'messaging_service.Run()' call).
  """
  service = benchmark_spec.messaging_service
  if _MEASUREMENT.value == SINGLE_OP:
    publish_results = service.Run(service.PUBLISH_LATENCY,
                                  int(_NUMBER_OF_MESSAGES.value),
                                  int(_MESSAGE_SIZE.value))
    pull_results = service.Run(service.PULL_LATENCY,
                               int(_NUMBER_OF_MESSAGES.value),
                               int(_MESSAGE_SIZE.value))
    publish_results.update(pull_results)
    results = publish_results
  elif _MEASUREMENT.value == END_TO_END:
    results = service.Run(service.END_TO_END_LATENCY,
                          int(_NUMBER_OF_MESSAGES.value),
                          int(_MESSAGE_SIZE.value))
  # Creating samples from results
  samples = _CreateSamples(results, int(_NUMBER_OF_MESSAGES.value),
                           int(_MESSAGE_SIZE.value), FLAGS.cloud)
  return samples


def Cleanup(_: bm_spec.BenchmarkSpec):
  pass
