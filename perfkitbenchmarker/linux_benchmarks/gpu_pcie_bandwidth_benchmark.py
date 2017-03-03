# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

import numpy
import re
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import regex_util
from perfkitbenchmarker.linux_packages import cuda_toolkit_8


flags.DEFINE_integer('gpu_pcie_bandwidth_iterations', 30,
                     'number of iterations to run',
                     lower_bound=1)


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'gpu_pcie_bandwidth'
# Note on the config: gce_migrate_on_maintenance must be false,
# because GCE does not support migrating the user's GPU state.
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
          boot_disk_size: 200
        AWS:
          image: ami-a9d276c9
          machine_type: p2.xlarge
          zone: us-west-2b
          boot_disk_size: 200
        Azure:
          image: Canonical:UbuntuServer:16.04.0-LTS:latest
          machine_type: Standard_NC6
          zone: eastus
"""
BENCHMARK_METRICS = ['Host to device bandwidth',
                     'Device to host bandwidth',
                     'Device to device bandwidth']

EXTRACT_BANDWIDTH_TEST_RESULTS_REGEX = r'\d+\s+(\d+\.?\d*)'
EXTRACT_DEVICE_INFO_REGEX = r'Device\s*(\d):\s*(.*$)'


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  cuda_toolkit_8.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Install CUDA toolkit 8.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('cuda_toolkit_8')


def _ParseDeviceInfo(test_output):
  """Parses the GPU device info from the CUDA device bandwidth test output.

  Args:
    test_output: The resulting output string from the bandwidth
      test application.

  Returns:
    A dictionary mapping the device number to its name, for every
    device available on the system.
  """
  matches = regex_util.ExtractAllMatches(EXTRACT_DEVICE_INFO_REGEX,
                                         test_output, re.MULTILINE)
  devices = {str(i[0]): str(i[1]) for i in matches}
  return devices


def _ParseOutputFromSingleIteration(test_output):
  """Parses the output of the CUDA device bandwidth test.

  Args:
    test_output: The resulting output string from the bandwidth
      test application.

  Returns:
    A dictionary containing the following values as floats:
      * the device to host bandwidth
      * the host to device bandwidth
      * the device to device bandwidth
    All units are in MB/s, as these are the units guaranteed to be output
    by the test.
  """
  matches = regex_util.ExtractAllMatches(EXTRACT_BANDWIDTH_TEST_RESULTS_REGEX,
                                         test_output)
  results = {}
  for i, metric in enumerate(BENCHMARK_METRICS):
    results[metric] = float(matches[i])
  return results


def _CalculateMetricsOverAllIterations(result_dicts, metadata={}):
  """Calculates stats given list of result dictionaries.

    Each item in the list represends the results from a single
    iteration.

  Args:
    result_dicts: a list of result dictionaries. Each result dictionary
      represents a single run of the CUDA device bandwidth test,
      parsed by _ParseOutputFromSingleIteration().

    metadata: metadata dict to be added to each Sample.

  Returns:
    a list of sample.Samples containing the device to host bandwidth,
    host to device bandwidth, and device to device bandwidth for each
    iteration, along with the following stats for each bandwidth type:
      * mean
      * min
      * max
      * stddev
  """
  samples = []
  for metric in BENCHMARK_METRICS:
    sequence = [x[metric] for x in result_dicts]
    # Add a Sample for each iteration, and include the iteration number
    # in the metadata.
    for idx, measurement in enumerate(sequence):
      metadata_copy = metadata.copy()
      metadata_copy['iteration'] = idx
      samples.append(sample.Sample(
          metric, measurement, 'MB/s', metadata_copy))

    samples.append(sample.Sample(
        metric + ', min', min(sequence), 'MB/s', metadata))
    samples.append(sample.Sample(
        metric + ', max', max(sequence), 'MB/s', metadata))
    samples.append(sample.Sample(
        metric + ', mean', numpy.mean(sequence), 'MB/s', metadata))
    samples.append(sample.Sample(
        metric + ', stddev', numpy.std(sequence), 'MB/s', metadata))
  return samples


def Run(benchmark_spec):
  """Sets the GPU clock speed and runs the CUDA PCIe benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vm = benchmark_spec.vms[0]
  # Note:  The clock speed is set in this function rather than Prepare()
  # so that the user can perform multiple runs with a specified
  # clock speed without having to re-prepare the VM.
  cuda_toolkit_8.SetAndConfirmGpuClocks(vm)
  num_iterations = FLAGS.gpu_pcie_bandwidth_iterations
  raw_results = []
  metadata = {}
  metadata['num_iterations'] = num_iterations
  metadata['num_gpus'] = cuda_toolkit_8.QueryNumberOfGpus(vm)
  metadata['memory_clock_MHz'] = FLAGS.gpu_clock_speeds[0]
  metadata['graphics_clock_MHz'] = FLAGS.gpu_clock_speeds[1]
  run_command = ('%s/extras/demo_suite/bandwidthTest --device=all'
                 % cuda_toolkit_8.CUDA_TOOLKIT_INSTALL_DIR)
  for i in range(num_iterations):
    stdout, _ = vm.RemoteCommand(run_command, should_log=True)
    raw_results.append(_ParseOutputFromSingleIteration(stdout))
    if 'device_info' not in metadata:
      metadata['device_info'] = _ParseDeviceInfo(stdout)
  return _CalculateMetricsOverAllIterations(raw_results, metadata)


def Cleanup(benchmark_spec):
  """Uninstalls CUDA toolkit 8

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Uninstall('cuda_toolkit_8')
