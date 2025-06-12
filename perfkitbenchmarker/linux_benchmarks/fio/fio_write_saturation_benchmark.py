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
"""Benchmark to measure Max IOPS after write saturation."""

import copy
import logging

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio_write_saturation'
BENCHMARK_CONFIG = """
fio_write_saturation:
  description: Runs fio to measure max IOPS under write saturation.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 1
  flags:
    fio_fill_size: 200%
    fio_fill_block_size: 128k
    fio_generate_scenarios: rand_4k_write_100%
    fio_runtime: 300
    fio_target_mode: against_device_with_fill
"""
# Prefilling the disk with 200% of its size make sure all the disk segments
# are written to.


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
        'fio_write_saturation_benchmark only supported against device right'
        ' now.'
    )
  ValidateNoCustomJobFile()


def ValidateNoCustomJobFile():
  if fio_flags.FIO_JOBFILE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        "Benchmark doesn't support custom job file, Please use"
        ' --fio_generate_scenarios'
    )


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  vm = spec.vms[0]
  vm.Install('fio')
  utils.Prefill(vm, constants.FIO_PATH)


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs fio benchmark multiple times to reach steady state or write saturation.

  We keep running fio till we get 5 consecutive runs with iops within 5 percent
  of each other.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  vm = spec.vms[0]
  iodepth = 50
  numjobs = 2 * vm.num_cpus
  benchmark_params = {
      'iodepth': iodepth,
      'numjobs': numjobs,
  }
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_flags.FIO_GENERATE_SCENARIOS.value,
      benchmark_params,
      'fio-parent.job',
  )
  stable_count = 0
  all_samples = []
  iops_values = []
  samples = []
  write_iops_sample = None
  # running fio till we get 5 consecutive runs with iops within 5 percent
  # of each other.
  while stable_count < 5:
    samples = utils.RunTest(vm, constants.FIO_PATH, job_file_str)
    write_iops_sample = GetIOPSSample(samples)
    iops_values.append(write_iops_sample.value)
    logging.info('write_iops: %s', write_iops_sample.value)
    if not iops_values:
      stable_count = 1
    else:
      if (
          abs((write_iops_sample.value - iops_values[-1]) / iops_values[-1])
          < 0.05
      ):
        stable_count += 1
      else:
        stable_count = 0
  if samples and write_iops_sample:
    ValidateIops(vm, write_iops_sample.value)
    metadata = copy.deepcopy(write_iops_sample.metadata)
    metadata['all_write_iops'] = iops_values
    all_samples.append(
        sample.Sample(
            metric='saturation_write_iops',
            value=write_iops_sample.value,
            unit=write_iops_sample.unit,
            metadata=write_iops_sample.metadata,
        )
    )
    all_samples.extend(samples)
    all_samples.append(
        sample.Sample(
            metric='all_write_iops',
            value=-1,
            unit='',
            metadata=metadata,
        )
    )
  else:
    raise errors.Benchmarks.RunError(
        'Write IOPS not found, please check the fio output in logs.'
    )
  return all_samples


def ValidateIops(vm, saturation_iops):
  """Running double the iodepth to make sure config is enough for saturation.

  Comparing the saturation IOPS with double the iodepth iops, if they are within
  5 percent then config is good enough else raise exception.

  Args:
    vm: The virtual machine.
    saturation_iops: The saturation IOPS.
  """
  benchmark_params = {
      'iodepth': 100,
      'numjobs': 2 * vm.num_cpus,
  }
  job_file_str = utils.GenerateJobFile(
      vm.scratch_disks,
      fio_flags.FIO_GENERATE_SCENARIOS.value,
      benchmark_params,
  )
  validation_samples = utils.RunTest(vm, constants.FIO_PATH, job_file_str)
  validation_write_iops = GetIOPSSample(validation_samples)
  logging.info('validation_write_iops: %s', validation_write_iops.value)
  if (validation_write_iops.value - saturation_iops) / saturation_iops > 0.05:
    raise errors.Benchmarks.RunError(
        'Fio Config is not enough for saturation, please check the fio output'
        ' in logs.'
    )


def GetIOPSSample(samples) -> sample.Sample:
  for sample_details in samples:
    if sample_details.metric.endswith('write:iops'):
      return sample_details
  raise errors.Benchmarks.RunError(
      'write IOPS not found, please check the fio output in logs.'
  )


def Cleanup(_):
  pass
