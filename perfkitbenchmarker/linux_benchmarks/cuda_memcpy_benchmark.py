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
"""Run CUDA memcpy benchmarks."""

import functools
import logging
from typing import Any, Dict, List
from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_samples
from perfkitbenchmarker.linux_packages import cuda_toolkit
from perfkitbenchmarker.linux_packages import nvidia_driver

_MEMORY = flags.DEFINE_enum(
    'cuda_memcpy_memory', 'pinned', ['pageable', 'pinned'],
    'Specify which memory mode to use. pageable memory or non-pageable system '
    'memory.')
_MODE = flags.DEFINE_enum(
    'cuda_memcpy_mode', 'quick', ['quick', 'shamoo'],
    'Specify the mode to use. performs a quick measurement, or measures a '
    'user-specified range of values, or performs an intense shmoo of a large '
    'range of values.')
_HTOD = flags.DEFINE_boolean(
    'cuda_memcpy_htod', True, 'Measure host to device transfers.')
_DTOH = flags.DEFINE_boolean(
    'cuda_memcpy_dtoh', True, 'Measure device to host transfers.')
_DTOD = flags.DEFINE_boolean(
    'cuda_memcpy_dtod', True, 'Measure device to device transfers.')
_WC = flags.DEFINE_boolean(
    'cuda_memcpy_wc', False, 'Allocate pinned memory as write-combined.')


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cuda_memcpy'
BENCHMARK_CONFIG = """
cuda_memcpy:
  description: Runs CUDA memcpy Benchmark.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: a2-highgpu-1g
          zone: us-central1-a
          image_family: tf-latest-gpu-gvnic
          image_project: deeplearning-platform-release
        AWS:
          machine_type: p4d.24xlarge
          zone: us-east-1a
          image: ami-084e787069ee27fb7
        Azure:
          machine_type: Standard_NC6s_v3
          zone: eastus
          image: microsoft-dsvm:ubuntu-hpc:1804:latest
"""


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Install and set up CUDA memcpy on the target vm.

  Args:
    bm_spec: The benchmark specification
  """
  vm_util.RunThreaded(lambda vm: vm.Install('cuda_samples'), bm_spec.vms)


def _MetadataFromFlags() -> [str, Any]:
  """Returns metadata dictionary from flag settings."""
  return {
      'memory': _MEMORY.value,
      'mode': _MODE.value,
      'htod': _HTOD.value,
      'dtoh': _DTOH.value,
      'dtod': _DTOD.value,
      'wc': _WC.value,
  }


def _CollectGpuSamples(
    vm: virtual_machine.BaseVirtualMachine) -> List[sample.Sample]:
  """Run CUDA memcpy on the cluster.

  Args:
    vm: The virtual machine to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  if not nvidia_driver.CheckNvidiaSmiExists(vm):
    return []
  global_metadata = _MetadataFromFlags()
  global_metadata.update(cuda_toolkit.GetMetadata(vm))

  global_cmd = [cuda_samples.GetBandwidthTestPath(vm), '--csv',
                f'--memory={_MEMORY.value}', f'--mode={_MODE.value}']
  if _HTOD.value:
    global_cmd.append('--htod')
  if _DTOH.value:
    global_cmd.append('--dtoh')
  if _DTOD.value:
    global_cmd.append('--dtod')
  if _WC.value:
    global_cmd.append('--wc')

  num_gpus = nvidia_driver.QueryNumberOfGpus(vm)
  devices = list(range(num_gpus)) + (['all'] if num_gpus > 1 else [])
  samples = []
  for device in devices:
    cmd = ' '.join(global_cmd + [f'--device={device}'])
    stdout, stderr, exit_code = vm.RemoteCommandWithReturnCode(
        cmd, ignore_failure=True)
    if exit_code:
      logging.warning('Error with getting GPU stats: %s', stderr)
      continue
    results = regex_util.ExtractAllMatches(
        r'bandwidthTest-(\S+), '
        r'Bandwidth = ([\d\.]+) (\S+), '
        r'Time = ([\d\.]+) s, '
        r'Size = (\d+) bytes, '
        r'NumDevsUsed = (\d+)', stdout)

    for metric, bandwidth, unit, time, size, num_devs_used in results:
      metadata = {
          'time': float(time),
          'size': int(size),
          'NumDevsUsed': num_devs_used,
          'device': device,
          'command': cmd,
      }
      metadata.update(global_metadata)
      samples.append(
          sample.Sample(metric, float(bandwidth), unit, metadata))
  return samples


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  sample_lists = vm_util.RunThreaded(_CollectGpuSamples, bm_spec.vms)
  return (functools.reduce(lambda a, b: a + b, sample_lists) if sample_lists
          else [])


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleanup CUDA memcpy on the cluster.

  Args:
    bm_spec: The benchmark specification. Contains all data that
      is required to run the benchmark.
  """
  del bm_spec
