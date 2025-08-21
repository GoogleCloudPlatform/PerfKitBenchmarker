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
"""Benchmark to measure Max IOPS under latency sla for SSDs.

This benchmark uses latency_target, latency_percentile, latency_window and
latency_run flags of fio
(https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-latency_target).

NOTE : latency_run=True doesn't stop fio after achieving the max performance, it
keeps running for the 'runtime' duration and returns average IOPS for that
duration that meets the latency SLA.

How this benchmark configures iodepth and numjobs ?
1. numjobs = vm's cpus.
Selects iodepth large enough to create room for queue depth growth, right now
it's hardcoded at 50.

How to use this benchmark?
Finalize your latency target and latency percentile. Latency window is a
sampling window, fio will the queue depth for that duration. 30 secs is a good
duration because it gives fio sometime to get stable.
"""
import copy
import logging
import math
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags
from perfkitbenchmarker.linux_benchmarks.fio import utils


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'fio_latency_sla'
BENCHMARK_CONFIG = """
fio_latency_sla:
  description: Runs fio to measure max IOPS under latency SLA.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      disk_spec: *default_500_gb
      vm_count: 1
"""
JOB_FILE = 'fio-parent.job'


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
        'fio_latency_sla_benchmark only supported against device right now.'
    )
  ValidateFioScenarioIsPresent()
  ValidateNoCustomJobFile()


def ValidateFioScenarioIsPresent():
  """Checks for invalid flag configurations."""
  if not fio_flags.FIO_GENERATE_SCENARIOS.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Please specify fio scenarios in --fio_generate_scenarios flag'
    )


def ValidateNoCustomJobFile():
  if fio_flags.FIO_JOBFILE.value:
    raise errors.Setup.InvalidFlagConfigurationError(
        "Benchmark doesn't support custom job file, Please use"
        ' --fio_generate_scenarios'
    )


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  vm = spec.vms[0]
  vm.Install('fio')
  utils.PrefillIfEnabled(vm, constants.FIO_PATH)


def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run the fio latency sla benchmark.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  vm = spec.vms[0]
  max_iodepth = 100
  numjobs = math.ceil(vm.num_cpus)/2
  benchmark_params = {
      'latency_target': fio_flags.FIO_LATENCY_TARGET.value,
      'latency_percentile': fio_flags.FIO_LATENCY_PERCENTILE.value,
      'numjobs': numjobs,
  }
  left_iodepth = 1
  right_iodepth = max_iodepth
  latency_under_sla_samples = []
  iodepth_details = {}
  max_iops_under_sla = 0
  latency_target = _ParseIntLatencyTargetAsMicroseconds(
      fio_flags.FIO_LATENCY_TARGET.value
  )
  while left_iodepth <= right_iodepth:
    iodepth = (left_iodepth + right_iodepth) // 2
    benchmark_params['iodepth'] = iodepth
    job_file_str = utils.GenerateJobFile(
        vm.scratch_disks,
        fio_flags.FIO_GENERATE_SCENARIOS.value,
        benchmark_params,
        JOB_FILE,
    )
    samples = utils.RunTest(vm, constants.FIO_PATH, job_file_str)
    latency_at_percentile_samples = GetLatencyPercentileSample(samples)
    iops_samples = GetIopsSamples(samples)
    iodepth_details[iodepth] = ConstructLatencyIopsMap(
        latency_at_percentile_samples, iops_samples
    )
    read_latency = (
        iodepth_details[iodepth]['read_latency']
        if 'read_latency' in iodepth_details[iodepth]
        else None
    )
    write_latency = (
        iodepth_details[iodepth]['write_latency']
        if 'write_latency' in iodepth_details[iodepth]
        else None
    )
    if not read_latency and not write_latency:
      raise errors.Benchmarks.RunError(
          'Latency is not present in the samples. Please check the logs')
    if latency_at_percentile_samples[0].unit != 'usec':
      raise errors.Benchmarks.RunError(
          'Latency unit is not usec. Please check and update latency_target is'
          ' needed'
      )
    # Checking if read or write latency are more than latency target
    if (read_latency is not None and read_latency > latency_target) or (
        write_latency is not None and write_latency > latency_target
    ):  # latency has unit usec
      right_iodepth = iodepth - 1
      logging.info(
          'Read Latency at iodepth %s is %s usec and write latency is %s usec'
          ' (more than latency target), reducing right iodepth to %s',
          iodepth,
          read_latency,
          write_latency,
          right_iodepth,
      )
    else:
      total_iops = sum(
          iops_sample.value for iops_sample in iops_samples
      )  # looking at combined read and write iops for RW workloads
      if total_iops > max_iops_under_sla:
        latency_under_sla_samples = samples
        max_iops_under_sla = total_iops
      left_iodepth = iodepth + 1
      logging.info(
          'Read Latency at iodepth %s is %s usec and write latency is %s usec'
          ' (less than latency target), increasing left iodepth to %s',
          iodepth,
          read_latency,
          write_latency,
          left_iodepth,
      )
  if not latency_under_sla_samples:
    # latency target was never met for these numjobs
    raise errors.Benchmarks.RunError(
        f'We never reached latency target for {numjobs}, try again by reducing'
        ' the numjobs'
    )
  metadata = copy.deepcopy(latency_under_sla_samples[0].metadata)
  metadata['iodepth_details'] = iodepth_details
  latency_under_sla_samples.append(
      sample.Sample(
          metric='iodepth_details',
          value='',
          unit='',
          metadata=metadata,
          timestamp=latency_under_sla_samples[0].timestamp,
      )
  )
  return latency_under_sla_samples


def GetLatencyPercentileSample(samples):
  """Get latency percentile sample from list of samples.

  Args:
    samples: list of samples.

  Returns:
    latency percentile sample.
  """
  latency_percentile = fio_flags.FIO_LATENCY_PERCENTILE.value
  formatted_percentile = (
      latency_percentile
      if latency_percentile % 1 != 0
      else int(latency_percentile)
  )

  latency_samples = [
      sample
      for sample in samples
      if sample.metric.endswith(f'latency:p{formatted_percentile}')
  ]
  return latency_samples


def GetIopsSamples(samples):
  iops_samples = [
      sample
      for sample in samples
      if sample.metric.endswith(':iops')
  ]
  return iops_samples


def ConstructLatencyIopsMap(latency_samples, iops_samples):
  """Constructs a map of latency and IOPS details."""
  metric_details = {}
  for iops_sample in iops_samples:
    if iops_sample.metric.endswith('read:iops'):
      metric_details['read_iops'] = iops_sample.value
    if iops_sample.metric.endswith('write:iops'):
      metric_details['write_iops'] = iops_sample.value
  for latency_sample in latency_samples:
    if 'read:latency' in latency_sample.metric:
      metric_details['read_latency'] = latency_sample.value
    if 'write:latency' in latency_sample.metric:
      metric_details['write_latency'] = latency_sample.value
  return metric_details


def _ParseIntLatencyTargetAsMicroseconds(latency_target_str):
  if latency_target_str.endswith('ms'):
    return int(latency_target_str[:-2])*1000
  elif latency_target_str.endswith('us'):
    return int(latency_target_str[:-2])
  elif latency_target_str.endswith('s'):
    return int(latency_target_str[:-1])*1000000
  return int(latency_target_str)


def Cleanup(_):
  pass
