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
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import regex_util
from perfkitbenchmarker.linux_packages import cuda_toolkit_8

DEFAULT_RANGE_START = 1 << 26  # 64 MB
DEFAULT_RANGE_STEP = 1 << 26  # 64 MB
DEFAULT_RANGE_END = 1 << 30  # 1 GB

flags.DEFINE_integer(
    'gpu_pcie_bandwidth_iterations',
    30,
    'number of iterations to run',
    lower_bound=1)

flags.DEFINE_enum('gpu_pcie_bandwidth_mode', 'quick', ['quick', 'range'],
                  'bandwidth test mode to use. '
                  'If range is selected, provide desired range '
                  'in flag gpu_pcie_bandwidth_transfer_sizes. '
                  'Additionally, if range is selected, the resulting '
                  'bandwidth will be averaged over all provided transfer '
                  'sizes.')

flag_util.DEFINE_integerlist(
    'gpu_pcie_bandwidth_transfer_sizes',
    flag_util.IntegerList(
        [DEFAULT_RANGE_START, DEFAULT_RANGE_END,
         DEFAULT_RANGE_STEP]), 'range of transfer sizes to use in bytes. '
    'Only used if gpu_pcie_bandwidth_mode is set to range')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'gpu_pcie_bandwidth'
BENCHMARK_CONFIG = """
gpu_pcie_bandwidth:
  description: Runs NVIDIA's CUDA bandwidth test.
  vm_groups:
    default:
      vm_spec:
        GCP:
          image: ubuntu-1604-xenial-v20161115
          image_project: ubuntu-os-cloud
          machine_type: n1-standard-4
          gpu_type: k80
          gpu_count: 1
          zone: us-east1-d
          boot_disk_size: 200
        AWS:
          image: ami-d15a75c7
          machine_type: p2.xlarge
          zone: us-east-1
          boot_disk_size: 200
        Azure:
          image: Canonical:UbuntuServer:16.04.0-LTS:latest
          machine_type: Standard_NC6
          zone: eastus
"""
BENCHMARK_METRICS = [
    'Host to device bandwidth', 'Device to host bandwidth',
    'Device to device bandwidth'
]

EXTRACT_BANDWIDTH_TEST_RESULTS_REGEX = r'\d+\s+(\d+\.?\d*)'
EXTRACT_DEVICE_INFO_REGEX = r'Device\s*(\d):\s*(.*$)'


class InvalidBandwidthTestOutputFormat(Exception):
  pass


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
  matches = regex_util.ExtractAllMatches(EXTRACT_DEVICE_INFO_REGEX, test_output,
                                         re.MULTILINE)
  devices = {str(i[0]): str(i[1]) for i in matches}
  return devices


def _AverageResultsForSection(lines, results_section_header_index):
  """Return the average bandwidth for a specific section of results

  Args:
    lines: output of bandwidthTest, split by lines and stripped of whitespace
    results_section_header_index: line number of results section header.
      The actual results, in MB/s, should begin three lines after the header.

  Returns:
    average bandwidth, in MB/s, for the section beginning at
      results_section_header_index
  """
  RESULTS_OFFSET_FROM_HEADER = 3
  results = []
  for line in lines[results_section_header_index + RESULTS_OFFSET_FROM_HEADER:]:
    if not line:
      break  # done with this section if line is empty
    results.append(float(line.split()[1]))
  return numpy.mean(results)


def _FindIndexOfLineThatStartsWith(lines, str):
  """Return the index of the line that startswith str.

  Args:
    lines: iterable
    str: predicate to find in lines

  Returns:
    first index of the element in lines that startswith str

  Raises:
    InvalidBandwidthTestOutputFormat if str is not found
  """
  for idx, line in enumerate(lines):
    if line.startswith(str):
      return idx
  raise InvalidBandwidthTestOutputFormat(
      'Unable to find {0} in bandwidthTest output'.format(str))


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
  lines = [line.strip() for line in test_output.splitlines()]
  host_to_device_results_start = _FindIndexOfLineThatStartsWith(
      lines, 'Host to Device Bandwidth')
  device_to_host_results_start = _FindIndexOfLineThatStartsWith(
      lines, 'Device to Host Bandwidth')
  device_to_device_results_start = _FindIndexOfLineThatStartsWith(
      lines, 'Device to Device Bandwidth')

  host_to_device_mean = _AverageResultsForSection(lines,
                                                  host_to_device_results_start)
  device_to_host_mean = _AverageResultsForSection(lines,
                                                  device_to_host_results_start)
  device_to_device_mean = _AverageResultsForSection(
      lines, device_to_device_results_start)

  results = {
      'Host to device bandwidth': host_to_device_mean,
      'Device to host bandwidth': device_to_host_mean,
      'Device to device bandwidth': device_to_device_mean,
  }
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
      samples.append(sample.Sample(metric, measurement, 'MB/s', metadata_copy))

    samples.append(
        sample.Sample(metric + ', min', min(sequence), 'MB/s', metadata))
    samples.append(
        sample.Sample(metric + ', max', max(sequence), 'MB/s', metadata))
    samples.append(
        sample.Sample(metric + ', mean', numpy.mean(sequence), 'MB/s',
                      metadata))
    samples.append(
        sample.Sample(metric + ', stddev', numpy.std(sequence), 'MB/s',
                      metadata))
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
  mode = FLAGS.gpu_pcie_bandwidth_mode
  transfer_size_range = FLAGS.gpu_pcie_bandwidth_transfer_sizes
  raw_results = []
  metadata = {}
  metadata.update(cuda_toolkit_8.GetMetadata(vm))
  metadata['num_iterations'] = num_iterations
  metadata['mode'] = mode
  if mode == 'range':
    metadata['range_start'] = transfer_size_range[0]
    metadata['range_stop'] = transfer_size_range[1]
    metadata['range_step'] = transfer_size_range[2]

  run_command = ('%s/extras/demo_suite/bandwidthTest --device=all' %
                 cuda_toolkit_8.CUDA_TOOLKIT_INSTALL_DIR)
  if mode == 'range':
    run_command += (' --mode=range --start={0} --end={1} --increment={2}'
                    .format(transfer_size_range[0], transfer_size_range[1],
                            transfer_size_range[2]))

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
