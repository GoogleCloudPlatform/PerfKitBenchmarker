# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""The sysbench memory benchmark is a memory test in sysbench.

The sysbench memory benchmark will allocate a memory buffer and then read or
write from it, each time for the size of a pointer (so 32bit or 64bit), and each
execution until the total buffer size has been read from or written to.
"""

import io
import re
from typing import Any, Dict, List, Tuple
import numpy as np

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'sysbench_memory'
BENCHMARK_CONFIG = """
sysbench_memory:
  description: Runs sysbench memory on all vCPU's of a VM.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the VMs and other resources for running the benchmark.

  Installs the sysbench package.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('sysbench')


def _ParseSysbenchMemoryOutput(sysbench_memory_output: str) -> List[float]:
  """Parses sysbench_memory output.

  Extract relevant TPS numbers.

  Args:
    sysbench_memory_output: The output from sysbench.
  Returns:
    An array, the tps numbers.

  """
  tps_numbers = []

  sysbench_memory_output_io = io.StringIO(sysbench_memory_output)
  for line in sysbench_memory_output_io:
    # find lines with throughput information
    # sample output: "38225.96 MiB transferred (3822.38 MiB/sec)"
    match = re.search('[0-9]+[.,]?[0-9]* MiB/sec', line)
    if match:
      tps_numbers.append(float(match.group(0).split()[0]))

  return tps_numbers


def _AddMetricsFromTPSNumbers(
    tps_numbers: List[float]) -> List[Tuple[str, float, str]]:
  """Computes relevant metrics from tps_numbers.

  Specifically, we are interested in min/max/mean/(median-min)/(max-min).

  Args:
    tps_numbers: TPS numbers for each vCPU.
  Returns:
    An array, the tps metrics.

  """

  tps_min = np.min(tps_numbers)
  tps_max = np.max(tps_numbers)
  tps_mean = np.round(np.mean(tps_numbers), 2)
  tps_median = np.median(tps_numbers)
  vcpu_variation = np.round(tps_median - tps_min, 2)
  tps_range = np.round(tps_max - tps_min, 2)

  total = np.sum(tps_numbers)
  size = len(tps_numbers)
  all_max_diff = np.round((tps_max*size) - total, 2)
  all_median_diff = np.round((tps_median*size) - total, 2)

  metrics = []
  tps_unit = 'MiB/sec'
  metrics.append(('Minimum_throughput', tps_min, tps_unit))
  metrics.append(('Maximum_throughput', tps_max, tps_unit))
  metrics.append(('Average_throughput', tps_mean, tps_unit))
  metrics.append(('vcpu_variation', vcpu_variation, tps_unit))
  metrics.append(('Range_throughput', tps_range, tps_unit))
  metrics.append(('Difference_from_all_max', all_max_diff, tps_unit))
  metrics.append(('Difference_from_all_median', all_median_diff, tps_unit))

  return metrics


def GenerateMetricsForSysbenchMemoryOutput(
    sysbench_memory_output: str, metadata: Dict[str,
                                                Any]) -> List[sample.Sample]:
  """Generates results, an array of samples from sysbench_memory output.

  Obtains TPS metrics from _ParseSysbenchMemoryOutput and generates samples.

  Args:
    sysbench_memory_output: The output from sysbench.
    metadata: associated metadata

  Returns:
    results: a list of Samples
  """
  results = []
  tps_numbers = _ParseSysbenchMemoryOutput(sysbench_memory_output)
  metrics = _AddMetricsFromTPSNumbers(tps_numbers)
  for metric in metrics:
    results.append(sample.Sample(metric[0], metric[1], metric[2], metadata))

  return results


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the benchmark and returns a dict of performance data.

  It must be possible to run the benchmark multiple times after the Prepare
  stage.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.

  Returns:
    A list of performance samples.
  """
  metadata = {}

  vm = benchmark_spec.vms[0]
  num_cpus = vm.NumCpusForBenchmark()
  stdout, _ = vm.RemoteCommand(f'for CPU in `seq 0 {num_cpus-1}`; do echo -n '
                               '"CPU $CPU "; taskset --cpu-list $CPU sysbench '
                               'memory run; done', should_log=True)

  return GenerateMetricsForSysbenchMemoryOutput(stdout, metadata)


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up after the benchmark completes.

  The state of the VMs should be equivalent to the state before Prepare was
  called.

  Args:
    benchmark_spec: The benchmark spec for this sample benchmark.
  """
  del benchmark_spec
