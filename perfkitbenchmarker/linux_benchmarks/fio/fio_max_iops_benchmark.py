# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Benchmark to measure Max IOPS using fio."""
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio_max_iops'
BENCHMARK_CONFIG = """
fio_max_iops:
  description: Runs fio to measure max IOPS.
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
IODEPTH = 128
NUMJOBS = [4, 8, 16, 32, 64, 128]


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  disk_spec = config['vm_groups']['default']['disk_spec']
  for cloud in disk_spec:
    disk_spec[cloud]['mount_point'] = None
  return config


def CheckPrerequisites(benchmark_config):
  """Perform flag checks."""
  del benchmark_config  # unused
  if not utils.AgainstDevice():
    errors.Setup.InvalidFlagConfigurationError(
        'fio_max_iops_benchmark only supported against device right now.'
    )
  ValidateRWKind()
  ValidateNoCustomJobFile()


def ValidateRWKind():
  """Checks for invalid flag configurations."""
  if not fio_flags.FIO_OPERATION_TYPE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Please specify fio rw kind in --fio_rw_kind flag'
    )


def ValidateNoCustomJobFile():
  if fio_flags.FIO_JOBFILE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        "Benchmark doesn't support custom job file"
    )


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  vm = spec.vms[0]
  vm.Install('fio')
  FLAGS['fio_target_mode'].value = _GetFioTargetMode()
  utils.PrefillIfEnabled(vm, constants.FIO_PATH)


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs fio over fixed iodepth and numjobs and return max IOPS.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  vm = spec.vms[0]
  samples = []
  fio_scenarios = []
  for numjobs in NUMJOBS:
    if fio_flags.FIO_OPERATION_TYPE.value != constants.OPERATION_READWRITE:
      scenario = (
          f'rand_4k_{fio_flags.FIO_OPERATION_TYPE.value}_100%_iodepth-{IODEPTH}_numjobs-{numjobs}'
      )
    else:
      scenario = (
          f'rand_4k_{fio_flags.FIO_OPERATION_TYPE.value}_100%_iodepth-{IODEPTH}_numjobs-{numjobs}_rwmixread-{fio_flags.FIO_RW_MIX_READ.value}'
      )
    fio_scenarios.append(scenario)
  benchmark_params = {
      'numjobs_array': NUMJOBS,
  }
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_scenarios,
      benchmark_params,
  )
  test_samples = utils.RunTest(vm, constants.FIO_PATH, job_file_str)
  samples.extend(test_samples)
  max_iops_sample = _GetMaxIOPSSample(test_samples)
  samples.append(
      sample.Sample(
          metric='max_iops',
          value=max_iops_sample.value,
          unit=max_iops_sample.unit,
          metadata=max_iops_sample.metadata,
      )
  )
  return samples


def _GetFioTargetMode():
  """Returns the fio target mode based on the operation type.

  Always prefilling for read.
  """
  if fio_flags.FIO_OPERATION_TYPE.value == constants.OPERATION_WRITE:
    return constants.AGAINST_DEVICE_WITHOUT_FILL_MODE
  return constants.AGAINST_DEVICE_WITH_FILL_MODE


def _GetMaxIOPSSample(samples) -> sample.Sample:
  """Returns Max IOPS sample from the list of samples of fio run."""
  sorted_iops_samples = sorted(
      list(filter(lambda x: x.metric.endswith(':iops'), samples)),
      key=lambda x: x.value,
      reverse=True,
  )
  if not sorted_iops_samples:
    raise errors.Benchmarks.RunError(
        'Max IOPS not found, please check the fio output in logs.'
    )
  if int(sorted_iops_samples[0].metadata['numjobs']) == sorted(NUMJOBS)[
      -1
  ] and (
      (
          abs(sorted_iops_samples[0].value - sorted_iops_samples[1].value)
          / sorted_iops_samples[1].value
      )
      > 0.02
  ):
    # The current numjobs might not be reaching max IOPS since the difference
    # between the max two iops is more than 2 percent.
    raise errors.Benchmarks.RunError(
        'Max IOPS found with the largest numjobs, possibility of not'
        ' reaching the max.'
    )
  return sorted_iops_samples[0]


def Cleanup(_):
  pass
