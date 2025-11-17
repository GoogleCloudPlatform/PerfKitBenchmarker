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
from perfkitbenchmarker.linux_benchmarks import netperf_benchmark
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import fio_benchmark
from perfkitbenchmarker.linux_benchmarks.fio import utils


BENCHMARK_NAME = 'fio_netperf'
BENCHMARK_CONFIG = """
fio_netperf:
  description: Run FIO and Netperf benchmarks in parallel
  vm_groups:
    vm_1:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
    vm_2:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
  flags:
    placement_group_style: none
    netperf_test_length: 300
    netperf_benchmarks: TCP_STREAM
    netperf_num_streams: 200
    fio_runtime: 300
    fio_generate_scenarios: seq_1M_readwrite_100%_rwmixread-50
    fio_num_jobs: 8
    fio_io_depths: 64
    fio_target_mode: against_device_without_fill
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
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  # Running against raw device
  for vm_group in config['vm_groups'].keys():
    disk_spec = config['vm_groups'][vm_group]['disk_spec']
    for cloud in disk_spec:
      disk_spec[cloud]['mount_point'] = None
  return config


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepare both benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  min_test_length = min(
      FLAGS.netperf_test_length * len(FLAGS.netperf_num_streams),
      FLAGS.fio_runtime * len(FLAGS.fio_generate_scenarios),
  )
  max_test_length = max(
      FLAGS.netperf_test_length * len(FLAGS.netperf_num_streams),
      FLAGS.fio_runtime * len(FLAGS.fio_generate_scenarios),
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

  # Prepare FIO benchmark on receiving VM (from fio_raw_device_benchmark).
  vms[1].Install('fio')
  utils.PrefillIfEnabled(vms[1], constants.FIO_PATH)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs both benchmarks in parallel.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    RunError: A run-stage error raised by an individual benchmark.
  """
  # Only the receiving vm is used for FIO.
  vms = benchmark_spec.vms[:2]
  output_samples_list = background_tasks.RunParallelThreads(
      [
          (fio_benchmark.RunFioOnVMs, [[vms[1]]], {}),
          (netperf_benchmark.RunClientServerVMs, vms, {}),
      ],
      2,
  )

  # Both FIO and netperf benchmarks are guaranteed to have samples for
  # 'start time' and 'end time'.
  # FIO samples collected from client/server VM.
  sample_times = {}
  for fio_sample in output_samples_list[0]:
    if fio_sample.metric == 'start_time':
      key = 'fio_start_time'
      sample_times[key] = min(
          sample_times.get(key, float('inf')), fio_sample.value
      )

    elif fio_sample.metric == 'end_time':
      key = 'fio_end_time'
      sample_times[key] = max(
          sample_times.get(key, float('-inf')), fio_sample.value
      )

  # Netperf samples collected for each of num_streams.
  for netperf_sample in output_samples_list[1]:
    if netperf_sample.metric == 'start_time':
      sample_times['netperf_start_time'] = min(
          sample_times.get('netperf_start_time', float('inf')),
          netperf_sample.value,
      )
    elif netperf_sample.metric == 'end_time':
      sample_times['netperf_end_time'] = max(
          sample_times.get('netperf_end_time', float('-inf')),
          netperf_sample.value,
      )

  min_test_length = min(FLAGS.netperf_test_length, FLAGS.fio_runtime)
  if (
      float(
          abs(
              sample_times['fio_start_time']
              - sample_times['netperf_start_time']
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
          abs(sample_times['fio_end_time'] - sample_times['netperf_end_time'])
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
  netperf_benchmark.CleanupClientServerVMs(vms[0], vms[1])
