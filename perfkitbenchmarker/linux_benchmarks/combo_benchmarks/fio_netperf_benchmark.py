# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs FIO and Netperf benchmarks in parallel.

Fio_netperf benchmark specifies its own benchmark config, using the first 2
VM's to run both FIO (on each VM) and Netperf (between the VM's). The benchmark-
specific flags for each benchmark can still be used, such as netperf_test_length
and fio_runtime (which are both specified in this benchmark config), which
determine how long the run stage lasts.
"""

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import fio_benchmark
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark
from perfkitbenchmarker.linux_packages import fio


BENCHMARK_NAME = 'fio_netperf'
BENCHMARK_CONFIG = """
fio_netperf:
  description: Run FIO and Netperf benchmarks in parallel
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    vm_2:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
  flags:
    netperf_test_length: 300
    fio_runtime: 300
    placement_group_style: closest_supported
"""

FLAGS = flags.FLAGS

MIN_RUN_STAGE_DURATION = 60
RUN_STAGE_DELAY_THRESHOLD = 0.01


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  """Merge BENCHMARK_CONFIG with user_config to create benchmark_spec.

  Args:
    user_config: user-define configs (through FLAGS.benchmark_config_file or
      FLAGS.config_override).

  Returns:
    merged configs
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepare both benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  min_test_length = min(
      FLAGS.netperf_test_length * len(FLAGS.netperf_num_streams),
      FLAGS.fio_runtime,
  )
  max_test_length = max(
      FLAGS.netperf_test_length * len(FLAGS.netperf_num_streams),
      FLAGS.fio_runtime,
  )
  if min_test_length < MIN_RUN_STAGE_DURATION:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Combo benchmark run stages must run for at least'
        f' {MIN_RUN_STAGE_DURATION} seconds.'
    )

  elif float(max_test_length) / min_test_length - 1 > RUN_STAGE_DELAY_THRESHOLD:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Combo benchmark run stages must have similar runtimes.'
    )

  vms = benchmark_spec.vms[:2]

  # Prepare Netperf benchmark
  client_vm, server_vm = vms
  background_tasks.RunThreaded(
      netperf_benchmark.PrepareNetperf, [client_vm, server_vm]
  )
  background_tasks.RunParallelThreads(
      [
          (netperf_benchmark.PrepareClientVM, [client_vm], {}),
          (
              netperf_benchmark.PrepareServerVM,
              [server_vm, client_vm.internal_ip, client_vm.ip_address],
              {},
          ),
      ],
      2,
  )

  # Prepare FIO benchmark
  exec_path = fio.GetFioExec()
  background_tasks.RunThreaded(
      lambda vm: fio_benchmark.PrepareWithExec(vm, exec_path), vms
  )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs both benchmarks in parallel.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    RunError: A run-stage error raised by an individual benchmark.
  """
  vms = benchmark_spec.vms[:2]
  output_samples_list = background_tasks.RunParallelThreads(
      [
          (fio_benchmark.RunFioOnVMs, [vms], {}),
          (netperf_benchmark.RunClientServerVMs, vms, {}),
      ],
      2,
  )

  # Both FIO and netperf benchmarks are guaranteed to have samples for
  # 'start time' and 'end time'.
  # FIO samples collected from client/server VM.
  fio_sample_start_times = []
  fio_sample_end_times = []
  for fio_sample in output_samples_list[0]:
    if fio_sample[0] == 'start_time':
      fio_sample_start_times.append(fio_sample[1])
    elif fio_sample[0] == 'end_time':
      fio_sample_end_times.append(fio_sample[1])
  # Netperf samples collected from client/server VM for each of num_streams.
  netperf_sample_start_times = []
  netperf_sample_end_times = []
  for netperf_sample in output_samples_list[1]:
    if netperf_sample[0] == 'start_time':
      netperf_sample_start_times.append(netperf_sample[1])
    elif netperf_sample[0] == 'end_time':
      netperf_sample_end_times.append(netperf_sample[1])

  min_test_length = min(FLAGS.netperf_test_length, FLAGS.fio_runtime)
  if (
      float(
          max(
              abs(
                  min(fio_sample_start_times) - min(netperf_sample_start_times)
              ),
              abs(
                  max(fio_sample_start_times) - min(netperf_sample_start_times)
              ),
          )
      )
      / min_test_length
      - 1
      > RUN_STAGE_DELAY_THRESHOLD
  ):
    raise errors.Benchmarks.RunError(
        'Run stage start delay threshold exceeded.'
    )

  if (
      float(
          max(
              abs(min(fio_sample_end_times) - max(netperf_sample_end_times)),
              abs(max(fio_sample_end_times) - max(netperf_sample_end_times)),
          )
      )
      / min_test_length
      - 1
      > RUN_STAGE_DELAY_THRESHOLD
  ):
    raise errors.Benchmarks.RunError('Run stage end delay threshold exceeded.')

  return output_samples_list[0] + output_samples_list[1]


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vms = benchmark_spec.vms[:2]
  background_tasks.RunThreaded(fio_benchmark.CleanupVM, vms)
  netperf_benchmark.CleanupClientServerVMs(vms[0], vms[1])
