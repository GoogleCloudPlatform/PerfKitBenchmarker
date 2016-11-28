# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Runs NVIDIA's CUDA PCI-E bandwidth test
      (https://developer.nvidia.com/cuda-code-samples)
"""

import re

import numpy
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import cuda_toolkit_8


flags.DEFINE_integer('gpu_pcie_bandwidth_iterations', 30,
                     'number of iterations to run',
                     lower_bound=1)

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'gpu_pcie_bandwidth'
BENCHMARK_CONFIG = """
gpu_pcie_bandwidth:
  description: Runs NVIDIA's CUDA bandwidth test.
  flags:
    gce_migrate_on_maintenance: False
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: /ubuntu-os-cloud/ubuntu-1604-xenial-v20161115
          machine_type: n1-standard-4-k80x1
          zone: us-east1-d
          boot_disk_size: 20
        AWS:
          image: ami-a9d276c9
          machine_type: p2.xlarge
          zone: us-west-2b
          boot_disk_size: 20
"""
BENCHMARK_METRICS = ['Host to device bandwidth',
                     'Device to host bandwidth',
                     'Device to device bandwidth']


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  cuda_toolkit_8.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Install CUDA toolkit 8

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  vm = benchmark_spec.vms[0]
  vm.Install('cuda_toolkit_8')


def ParseOutput(output):
  matches = re.findall(r'\d+\s+(\d+\.?\d*)', output)
  results = {}
  for i, metric in enumerate(BENCHMARK_METRICS):
    results[metric] = float(matches[i])
  return results


def CalculateMetrics(samples):
  metadata = {}
  results = []
  for metric in BENCHMARK_METRICS:
    sequence = [x[metric] for x in samples]
    results.append(sample.Sample(
        metric + ', min', min(sequence), 'MB/s', metadata))
    results.append(sample.Sample(
        metric + ', max', max(sequence), 'MB/s', metadata))
    results.append(sample.Sample(
        metric + ', mean', numpy.mean(sequence), 'MB/s', metadata))
    results.append(sample.Sample(
        metric + ', stddev', numpy.std(sequence), 'MB/s', metadata))
  return results


def Run(benchmark_spec):
  """Runs the CUDA PCIe benchmark

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  cuda_toolkit_8.MaximizeGPUClockSpeed(vm)
  run_command = '/usr/local/cuda-8.0/extras/demo_suite/bandwidthTest'
  samples = []
  for i in range(FLAGS.gpu_pcie_bandwidth_iterations):
    stdout, _ = vm.RemoteCommand(run_command, should_log=True)
    samples.append(ParseOutput(stdout))
  return CalculateMetrics(samples)


def Cleanup(benchmark_spec):
  pass
