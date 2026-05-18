# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Benchmark to measure Max Throughput using fio."""

import json
import logging

from typing import Any
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS


BENCHMARK_NAME = 'fio_max_throughput'
BENCHMARK_CONFIG = """
fio_max_throughput:
  description: Runs fio to measure max throughput.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 1
  flags:
    fio_fill_size: 100%
    fio_fill_block_size: 128k
    fio_runtime: 120
"""
IODEPTH = 64


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  disk_spec = config['vm_groups']['default']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def CheckPrerequisites(benchmark_config: benchmark_spec.BenchmarkSpec) -> None:
  """Perform flag checks."""
  del benchmark_config  # unused
  if not utils.AgainstDevice():
    raise errors.Setup.InvalidFlagConfigurationError(
        'fio_max_throughput_benchmark only supported against device right now.'
    )
  ValidateNoCustomJobFile()


def ValidateNoCustomJobFile() -> None:
  if fio_flags.FIO_JOBFILE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        "Benchmark doesn't support custom job file"
    )


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  vm = spec.vms[0]
  vm.Install('fio')
  FLAGS['fio_target_mode'].value = utils.GetFioTargetModeWithoutFillForWrite()
  utils.PrefillIfEnabled(vm, constants.FIO_PATH)


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs fio over fixed iodepth and numjobs and return max throughput.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  vm = spec.vms[0]
  samples = []
  fio_scenarios = []
  numjobs_list = []
  numjobs = 1
  while numjobs < vm.NumCpusForBenchmark():
    numjobs_list.append(numjobs)
    numjobs *= 2
  if not numjobs_list or numjobs_list[-1] != vm.NumCpusForBenchmark():
    numjobs_list.append(vm.NumCpusForBenchmark())

  for numjobs in numjobs_list:
    scenario = f'seq_1M_{fio_flags.FIO_OPERATION_TYPE.value}_100%_iodepth-{IODEPTH}_numjobs-{numjobs}'
    if fio_flags.FIO_OPERATION_TYPE.value == constants.OPERATION_READWRITE:
      scenario += f'_rwmixread-{fio_flags.FIO_RW_MIX_READ.value}'
    fio_scenarios.append(scenario)
  benchmark_params = {
      'numjobs_array': numjobs_list,
  }
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_scenarios,
      benchmark_params,
  )
  test_samples = utils.RunTest(vm, constants.FIO_PATH, job_file_str)
  samples.extend(test_samples)
  max_throughput_sample = _GetMaxThroughputSample(test_samples)
  samples.append(
      sample.Sample(
          metric='max_throughput',
          value=max_throughput_sample.value,
          unit=max_throughput_sample.unit,
          metadata=max_throughput_sample.metadata,
      )
  )
  return samples


def _GetMaxThroughputSample(samples) -> sample.Sample:
  """Returns Max Throughput sample from the list of samples of fio run.

  Args:
    samples: A list of sample.Sample objects from the fio run.
  """
  bw_samples = list(filter(lambda x: x.metric.endswith(':bandwidth'), samples))
  if not bw_samples:
    raise errors.Benchmarks.RunError(
        'Max Throughput not found, please check the fio output in logs.'
    )

  # Group bandwidth samples by numjobs (since iodepth is fixed).
  # In RW workloads, each numjobs will have two samples, one for read and one
  # for write. We need to sum them up to get the total bandwidth.
  paired_samples = {}
  for s in bw_samples:
    key = s.metadata.get('numjobs')
    if key not in paired_samples:
      paired_samples[key] = []
    paired_samples[key].append(s)

  logging.info(
      'all_iodepth_numjobs_data: %s',
      json.dumps(
          {
              k: [{'metric': s.metric, 'value': s.value} for s in v]
              for k, v in paired_samples.items()
          },
          indent=2,
      ),
  )

  combined_samples = []
  for sample_list in paired_samples.values():
    total_bw = sum(s.value for s in sample_list)
    combined_samples.append(
        sample.Sample(
            metric='max_throughput',
            value=total_bw,
            unit=sample_list[0].unit,
            metadata=sample_list[0].metadata.copy(),
        )
    )

  return max(combined_samples, key=lambda x: x.value)


def Cleanup(_) -> None:
  pass
