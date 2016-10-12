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

"""Run Blazemark benchmark."""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import blazemark

flags.DEFINE_list('blazemark_set', ['all'],
                  'A set of blazemark benchmarks to run.'
                  'See following link for a complete list of benchmarks to run:'
                  ' https://bitbucket.org/blaze-lib/blaze/wiki/Blazemark.')
# TODO: Support other library.
flags.DEFINE_list('blazemark_kernels', ['-only-blaze'], 'A list of additional '
                  'flags send to blazemark, in order to '
                  'enable/disable kernels/libraries. '
                  'To run all libraries, set the flag to "all" '
                  'See following link for more details: '
                  'https://bitbucket.org/blaze-lib/blaze/wiki/'
                  'Blazemark#!command-line-parameters')
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'blazemark'
BENCHMARK_CONFIG = """
blazemark:
  description: >
    Run blazemark. See:
    https://bitbucket.org/blaze-lib/blaze/wiki/Blazemark
  vm_groups:
    default:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepare vm for blazemark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('blazemark')


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with individual machine boot times.
  """
  vm = benchmark_spec.vms[0]
  all_tests = blazemark.GetBinaries(vm)
  requested = set(FLAGS.blazemark_set)
  run = set(all_tests)
  if 'all' not in FLAGS.blazemark_set:
    if requested - run:
      logging.warning(
          'Binaries %s not available, skipping. Options are %s',
          requested - run, run)
    run &= requested
  results = []
  for test in run:
    logging.info('Running %s', test)
    results.extend([
        sample.Sample(*result) for result in blazemark.RunTest(
            vm, test, FLAGS.blazemark_kernels)])
  return results


def Cleanup(benchmark_spec):
  pass
