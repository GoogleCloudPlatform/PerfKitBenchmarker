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

"""Run Blazemark benchmark."""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import blazemark

flags.DEFINE_list('blazemark_set', ['all'],
                  'A set of blazemark benchmarks to run.'
                  'See following link for a complete list of benchmarks to run:'
                  ' https://bitbucket.org/blaze-lib/blaze/wiki/Blazemark.')
# TODO: Support other library.
flags.DEFINE_list('blazemark_kernels', ['-only-blaze'], 'A list of additional '
                  'flags send to blazemark, in order to '
                  'enable/disable kernels/libraries. '
                  'Currently only support blaze. '
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


def CheckPrerequisites(benchmark_config):
  """Perform flag checks."""
  if 'all' not in FLAGS.blazemark_set:
    requested = frozenset(FLAGS.blazemark_set)
    binaries = blazemark.GetBinaries()
    if requested - binaries:
      raise errors.Benchmarks.PrepareException(
          'Binaries %s not available. Options are %s' % (
              requested - binaries, binaries))


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
  if 'all' not in FLAGS.blazemark_set:
    run = FLAGS.blazemark_set
  else:
    run = blazemark.GetBinaries()
  results = []
  for test in run:
    logging.info('Running %s', test)
    results.extend(blazemark.RunTest(vm, test))
  return results


def Cleanup(benchmark_spec):
  pass
