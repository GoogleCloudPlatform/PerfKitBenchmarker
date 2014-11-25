# Copyright 2014 Google Inc. All rights reserved.
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

import os.path
import re

import logging
from perfkitbenchmarker import errors

BENCHMARK_INFO = {'name': 'coremark',
                  'description': 'Run Coremark a simple processor benchmark',
                  'scratch_disk': False,
                  'num_machines': 1}

DATA_DIR = 'data/'
COREMARK_TAR = 'coremark_v1.0.tgz'
COREMARK_DIR = 'coremark_v1.0'
COREMARK_BUILDFILE = 'linux64/core_portme.mak'
ITERATIONS_PER_CPU = 200000


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Coremark on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('prepare Coremark on %s', vm)
  vm.InstallPackage('build-essential')
  file_path = DATA_DIR + COREMARK_TAR
  if not os.path.isfile(file_path):
    logging.error('Please provide %s under %s directory before'
                  ' running coremark benchmarks.', COREMARK_TAR, DATA_DIR)
    raise errors.Benchmarks.PrepareException('%s not found' % file_path)
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
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
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
  match = re.search(r'CoreMark 1.0 : ([0-9]*\.[0-9]*)', stdout).group(1)
  value = float(match)
  metadata = {'machine_type': vm.machine_type, 'num_cpus': vm.num_cpus}
  return [('Coremark Score', value, '', metadata)]


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
  vm.UninstallPackage('build-essential')
