# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs coremark.

From Coremark's documentation:
CoreMark's primary goals are simplicity and providing a method for benchmarking
only a processor's core features.

Coremark homepage: http://www.eembc.org/coremark/
"""

import posixpath
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import coremark

BENCHMARK_NAME = 'coremark'
BENCHMARK_CONFIG = """
coremark:
  description: Run Coremark a simple processor benchmark
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

COREMARK_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'coremark-1.01')
COREMARK_BUILDFILE = 'linux64/core_portme.mak'

# The number of iterations per CPU was chosen such that the runtime will always
# be greater than 10 seconds as specified in the run rules at
# https://www.eembc.org/coremark/CoreMarkRunRules.pdf.
ITERATIONS_PER_CPU = 1000000

# Methods of parallelism supported by Coremark.
PARALLELISM_PTHREAD = 'PTHREAD'
PARALLELISM_FORK = 'FORK'
PARALLELISM_SOCKET = 'SOCKET'
_COREMARK_PARALLELISM_METHOD = flags.DEFINE_enum(
    'coremark_parallelism_method', PARALLELISM_PTHREAD,
    [PARALLELISM_PTHREAD, PARALLELISM_FORK, PARALLELISM_SOCKET],
    'Method to use for parallelism in the Coremark benchmark.')
flag_util.DEFINE_integerlist(
    'coremark_thread_counts', [0, 1],
    'Runs n-threaded Coremark for each element n in the list. If n=0, '
    'vm.NumCpusForBenchmark() is used.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present."""
  del benchmark_config


def PrepareCoremark(remote_command):
  """Prepares coremark on a VM.

  Args:
    remote_command: Function to run a remote command on the VM.
  """
  if _COREMARK_PARALLELISM_METHOD.value == PARALLELISM_PTHREAD:
    remote_command('sed -i -e "s/LFLAGS_END += -lrt/LFLAGS_END += -lrt '
                   '-lpthread/g" %s/%s' % (COREMARK_DIR, COREMARK_BUILDFILE))


def Prepare(benchmark_spec):
  """Install Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('coremark')
  PrepareCoremark(vm.RemoteCommand)


def RunCoremark(remote_command, thread_count):
  """Runs coremark on the VM.

  Args:
    remote_command: Function to run a remote command on the VM.
    thread_count: Number of threads to use.

  Returns:
    A list of sample.Sample objects with the performance results.
  """
  remote_command(
      'cd %s;make PORT_DIR=linux64 clean; '
      'make PORT_DIR=linux64 ITERATIONS=%s '
      'XCFLAGS="-DMULTITHREAD=%d -DUSE_%s -DPERFORMANCE_RUN=1"' %
      (COREMARK_DIR, ITERATIONS_PER_CPU, thread_count,
       _COREMARK_PARALLELISM_METHOD.value))
  output, _ = remote_command('cat %s/run1.log' % COREMARK_DIR)

  return _ParseOutputForSamples(output, thread_count)


def _ParseOutputForSamples(output, thread_count):
  """Parses the output from running Coremark to get performance samples.

  Args:
    output: The output from running Coremark.
    thread_count: Number of threads used.

  Returns:
    A list of sample.Sample objects.

  Raises:
    Benchmarks.RunError: If correct operation is not validated.
  """
  if 'Correct operation validated' not in output:
    raise errors.Benchmarks.RunError('Correct operation not validated.')
  value = regex_util.ExtractFloat(r'CoreMark 1.0 : ([0-9]*\.[0-9]*)', output)
  metadata = {
      'summary':
          output.splitlines()[-1],  # Last line of output is a summary.
      'size':
          regex_util.ExtractInt(r'CoreMark Size\s*:\s*([0-9]*)', output),
      'total_ticks':
          regex_util.ExtractInt(r'Total ticks\s*:\s*([0-9]*)', output),
      'total_time_sec':
          regex_util.ExtractFloat(r'Total time \(secs\)\s*:\s*([0-9]*\.[0-9]*)',
                                  output),
      'iterations':
          regex_util.ExtractInt(r'Iterations\s*:\s*([0-9]*)', output),
      'iterations_per_cpu':
          ITERATIONS_PER_CPU,
      'parallelism_method':
          _COREMARK_PARALLELISM_METHOD.value,
      'thread_count':
          thread_count,
  }
  return [sample.Sample('Coremark Score', value, '', metadata)]


def Run(benchmark_spec):
  """Runs Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    Benchmarks.RunError: If correct operation is not validated.
  """
  vm = benchmark_spec.vms[0]
  output_samples = []
  for thread_count in FLAGS.coremark_thread_counts:
    thread_count_arg = vm.NumCpusForBenchmark() if (thread_count
                                                    == 0) else thread_count
    output_samples += RunCoremark(vm.RemoteCommand, thread_count_arg)

  return output_samples


def CleanupCoremark(remote_command):
  """Cleans up the coremark installation.

  Args:
    remote_command: Function to run a remote command on the VM.
  """
  remote_command('rm -rf %s' % COREMARK_DIR)
  remote_command('rm -f %s' % coremark.COREMARK_TAR)


def Cleanup(benchmark_spec):
  """Cleanup Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vm = benchmark_spec.vms[0]
  CleanupCoremark(vm.RemoteCommand)
