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
"""Module containing tailbench installation and cleanup functions."""

import os
from typing import List

from perfkitbenchmarker import sample
PACKAGE_NAME = 'tailbench'
TAILBENCH = 'tailbench-v0.9'

# TODO(user): Find a way to get this data from a https source.

TAILBENCH_TAR_URL = 'http://tailbench.csail.mit.edu/tailbench-v0.9.tgz'
TAILBENCH_TAR = 'tailbench-v0.9.tgz'
TAILBENCH_URL = 'http://tailbench.csail.mit.edu/' + TAILBENCH_TAR
TAILBENCH_INPUT_TAR = 'tailbench.inputs.tgz'
TAILBENCH_INPUT_URL = 'http://tailbench.csail.mit.edu/' + TAILBENCH_INPUT_TAR
PREPROVISIONED_DATA = {
    TAILBENCH_TAR:
        'b26ead61a857e4f6cb904d0b0d1af07b3b509ea0c62685d696f8a26883ee94a5',
    TAILBENCH_INPUT_TAR:
        '783b743e3d0d0b162bf92b93e219f67baa7c99666cb528a353d95721019817dd'
}
PACKAGE_DATA_URL = {
    TAILBENCH_TAR: TAILBENCH_URL,
    TAILBENCH_INPUT_TAR: TAILBENCH_INPUT_URL
}

BENCHMARK_NAME = 'tailbench'

INSTALL_DIR = '/scratch_ts'

BENCHMARK_MASSTREE = 'masstree'
BENCHMARK_SPECJBB = 'specjbb'
BENCHMARK_IMGDNN = 'img-dnn'

CONFIGS_SH_CONTENTS = f"""
# Set this to point to the top level of the TailBench data directory
DATA_ROOT={INSTALL_DIR}/tailbench.inputs

# Set this to point to the top level installation directory of the Java
# Development Kit. Only needed for Specjbb
JDK_PATH=/usr/lib/jvm/java-8-openjdk-amd64

# This location is used by applications to store scratch data during execution.
SCRATCH_DIR={INSTALL_DIR}/scratch
"""
MAKEFILE_CONFIG_CONTENTS = """
# Set this to point to the top level installation directory of the Java
#Development Kit. Only needed for Specjbb
JDK_PATH=/usr/lib/jvm/java-8-openjdk-amd64
"""
HEADER_LINES = 3


class _TestResult():

  def __init__(self, values: List[float], name: str, subname: str):
    self.histogram: sample._Histogram = sample.MakeHistogram(values, 0.95, 2)
    self.name = name
    self.subname = subname


def Install(vm):
  """Installs the tailbench dependencies and sets up the package on the VM."""
  # TODO(user): Rework all of this to use vm.Install to make more robust
  vm.InstallPackages(
      'libopencv-dev autoconf ant libtcmalloc-minimal4 swig google-perftools '
      'bzip2 libnuma-dev libjemalloc-dev libgoogle-perftools-dev '
      'libdb5.3++-dev libmysqld-dev libaio-dev uuid-dev libbz2-dev '
      'python-numpy python-scipy libgtop2-dev make g++ zlib1g-dev pkg-config '
      'pocketsphinx libboost-all-dev'
  )
  vm.Install('openjdk')

  vm.InstallPreprovisionedPackageData(PACKAGE_NAME,
                                      PREPROVISIONED_DATA.keys(), INSTALL_DIR)

  vm.RemoteCommand(f'cd {INSTALL_DIR} && tar xf {TAILBENCH_TAR}')
  vm.RemoteCommand(f'cd {INSTALL_DIR} && tar xf {TAILBENCH_INPUT_TAR}')
  vm.RemoteCommand(f'cd {INSTALL_DIR} && mkdir scratch')
  vm.RemoteCommand(f'cd {INSTALL_DIR} && mkdir results')


def PrepareTailBench(vm):
  """Set up tailbench on the VM by setting the config files.

  Args:
    vm: The VM in which we are setting up the TailBench config.
  """
  vm.RemoteCommand(f'echo "{CONFIGS_SH_CONTENTS}" > '
                   f'"{INSTALL_DIR}"/{TAILBENCH}/configs.sh')
  vm.RemoteCommand(f'echo "{MAKEFILE_CONFIG_CONTENTS}" > '
                   f'"{INSTALL_DIR}"/{TAILBENCH}/Makefile.config')


def _ParseResultsFile(input_file, name='') -> List[_TestResult]:
  """Reads from the file and returns a histogram.

  Args:
    input_file: a string input file name.
    name: name of the test run that generated the file

  Returns:
    A list of TestResults, each with a histogram representing different types
    of latency values mapped to their number of occurrences in the results file
  """
  if not os.path.exists(input_file):
    return []
  queue_values = []
  service_values = []
  sojourn_values = []
  i = 0
  with open(input_file, 'r') as f:
    for line in f:
      i = i + 1
      if i < HEADER_LINES:
        continue  # Don't add the headers as data.
        # See perfkitbenchmarker/tests/data/tailbench-img-dnnResult.txt
      else:
        line_values = line.split('|')
        queue_values.append(float(line_values[0].strip()))
        service_values.append(float(line_values[1].strip()))
        sojourn_values.append(float(line_values[2].strip()))
  test_results = []
  test_results.append(_TestResult(queue_values, name, 'queue'))
  test_results.append(_TestResult(service_values, name, 'service'))
  test_results.append(_TestResult(sojourn_values, name, 'sojourn'))
  return test_results


def BuildHistogramSamples(input_file,
                          name='',
                          metric='',
                          additional_metadata=None) -> List[sample.Sample]:
  """Builds a list of samples for a the results of a test.

  Args:
    input_file: a string input file name.
    name: name of the test run that generated the file
    metric: String. Metric name to use.
    additional_metadata: dict. Additional metadata attaching to Sample.

  Returns:
    samples: List of sample objects that reports the results from a tailbench
      test.
  """

  test_results = _ParseResultsFile(input_file, name)
  return [
      sample.CreateHistogramSample(result.histogram, result.name,
                                   result.subname, 'ms', additional_metadata,
                                   metric) for result in test_results
  ]


def RunTailbench(vm, tailbench_tests):
  """Runs up tailbench on the VM.

  Args:
    vm: The VM that we wish to run tailbench on.
    tailbench_tests: A command line flag that determines which tailbench tests
      we wish to run.
  """
  vm.RemoteCommand(f'{INSTALL_DIR}/{TAILBENCH}/build.sh harness')
  for test in tailbench_tests:
    vm.RemoteCommand(f'{INSTALL_DIR}/{TAILBENCH}/build.sh {test}')
    vm.RemoteCommand(f'cd {INSTALL_DIR}/{TAILBENCH}/{test} && sudo ./run.sh')
    vm.RemoteCommand(
        f'cd {INSTALL_DIR} && '
        f'sudo python2 {INSTALL_DIR}/{TAILBENCH}/utilities/parselats.py '
        f'{INSTALL_DIR}/{TAILBENCH}/{test}/lats.bin')
    vm.RemoteCommand(f'sudo mv {INSTALL_DIR}/lats.txt '
                     f'{INSTALL_DIR}/results/{test}.txt')
