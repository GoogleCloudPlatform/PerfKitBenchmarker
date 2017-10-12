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

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'coremark'
BENCHMARK_CONFIG = """
coremark:
  description: Run Coremark a simple processor benchmark
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

COREMARK_TAR = 'coremark_v1.0.tgz'
COREMARK_DIR = 'coremark_v1.0'
COREMARK_BUILDFILE = 'linux64/core_portme.mak'
ITERATIONS_PER_CPU = 200000


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(COREMARK_TAR)


def Prepare(benchmark_spec):
  """Install Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('prepare Coremark on %s', vm)
  vm.Install('build_tools')
  try:
    file_path = data.ResourcePath(COREMARK_TAR)
  except data.ResourceNotFound:
    logging.error('Please provide %s under perfkitbenchmarker/data directory '
                  'before running coremark benchmark.', COREMARK_TAR)
    raise errors.Benchmarks.PrepareException('%s not found' % COREMARK_TAR)
  vm.PushFile(file_path)
  vm.RemoteCommand('tar xvfz %s' % COREMARK_TAR)
  vm.RemoteCommand('sed -i -e "s/LFLAGS_END += -lrt/LFLAGS_END += -lrt '
                   '-lpthread/g" %s/%s' % (COREMARK_DIR, COREMARK_BUILDFILE))


def Run(benchmark_spec):
  """Run Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Coremark running on %s', vm)
  num_cpus = vm.num_cpus
  vm.RemoteCommand('cd %s;make PORT_DIR=linux64 ITERATIONS=%s XCFLAGS="-g -O2 '
                   '-DMULTITHREAD=%d -DUSE_PTHREAD -DPERFORMANCE_RUN=1"'
                   % (COREMARK_DIR, ITERATIONS_PER_CPU, num_cpus))
  logging.info('Coremark Results:')
  stdout, _ = vm.RemoteCommand(
      'cat %s/run1.log' % COREMARK_DIR, should_log=True)
  value = regex_util.ExtractFloat(r'CoreMark 1.0 : ([0-9]*\.[0-9]*)', stdout)
  metadata = {'num_cpus': vm.num_cpus}
  return [sample.Sample('Coremark Score', value, '', metadata)]


def Cleanup(benchmark_spec):
  """Cleanup Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  vm.RemoteCommand('rm -rf %s' % COREMARK_DIR)
  vm.RemoteCommand('rm -f %s' % COREMARK_TAR)
