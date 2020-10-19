# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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



import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
import re
import time

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'fast'
BENCHMARK_CONFIG = """
fast:
  description: Runs a quick test
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: null
"""

METRICS = ('Min Latency', 'Average Latency', 'Max Latency', 'Latency Std Dev')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

def Prepare(benchmark_spec):
    pass


def Run(benchmark_spec):
  """Run ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """

  results = _RunPing()

  return results


def _RunPing():

  time.sleep(30)
  results = []


  for i, metric in enumerate(METRICS):
    results.append(sample.Sample(metric, 0.1, 'ms'))
  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup ping on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
