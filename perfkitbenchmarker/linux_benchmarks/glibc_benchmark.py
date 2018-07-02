# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs Glibc Microbenchmark.

The glibc microbenchmark suite automatically generates code for specified
functions, builds and calls them repeatedly for given inputs to give some
basic performance properties of the function.

Homepage: https://fossies.org/linux/glibc/benchtests/README

Installs glibc-2.27. The benchmark needs python 2.7 or later in addition to the
dependencies required to build the GNU C Library.
"""

import json
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'glibc'
BENCHMARK_CONFIG = """
glibc:
  description: Runs Glibc Microbenchmark.
  vm_groups:
    default:
      vm_spec: *default_dual_core
      vm_count: null
"""

glibc_default_benchset = ['bench-math',
                          'bench-pthread',
                          'bench-string',
                          'string-benchset',
                          'wcsmbs-benchset',
                          'stdlib-benchset',
                          'stdio-common-benchset',
                          'math-benchset',
                          'malloc-thread']
flags.DEFINE_multi_enum(
    'glibc_benchset', glibc_default_benchset, glibc_default_benchset,
    'By default, it will run the whole set of benchmarks. To run only a subset '
    'of benchmarks, one may set "glibc_benchset = bench-math bench-pthread" by '
    'using the flag on the command line multiple times.')

GLIBC_BENCH = ['bench-math', 'bench-pthread', 'bench-string']
GLIBC_BENCH_MALLOC = ['malloc-thread']
# TODO(yanfeiren): Parse other *-benchset benchmarks.
GLIBC_MATH_BENCHSET = ['math-benchset']

RESULTS_DIR = '%s/glibc/glibc-build/benchtests' % linux_packages.INSTALL_DIR


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def PrepareGlibc(vm):
  """Building glibc on a single vm."""
  logging.info('Installing Glibc on %s', vm)
  vm.Install('glibc')


def Prepare(benchmark_spec):
  """Install glibc on the target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  PrepareGlibc(vm)


def UpdateMetadata(metadata):
  """Update metadata with glibc-related flag values."""
  metadata['gcc'] = '5.5.0-12'
  metadata['glibc_benchset'] = FLAGS.glibc_benchset


# The reason to write this helper function is that glibc_benchset_output.json
# contains duplicate keys.
def HelperParseOutput(lst):
  """Self defined parameter function for 'object_pairs_hook' of json.loads().

  Purpose:
      This function helps to parse json text with duplicate keys, for instance:
      json text: {"key":1, "key":2, "key2":3, "key2":4}
      With this function, we will have the following dict:
      {"key":[1, 2],
       "key2":[3, 4]
      }

  Args:
    lst: A list of tuples contains the output of benchmark tests.
  Returns:
    A dict contains the output of benchmark tests.
  """

  result = {}
  for key, val in lst:
    if key in result:
      if isinstance(result[key], list):
        result[key].append(val)
      else:
        result[key] = [result[key], val]
    else:
      result[key] = val
  return result


def ParseOutput(glibc_output, benchmark_spec, upper_key, results):
  """Parses the output from glibc.

  Args:
    glibc_output: A json format string containing the output of benchmark tests.
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    upper_key: The first dimension key of the glibc_output dict.
    results:
      A list to which this function will append new samples based on the glibc
      output. (in the same format as Run() returns).
  """

  metadata = dict()
  metadata['num_machines'] = len(benchmark_spec.vms)
  UpdateMetadata(metadata)

  jsondata = json.loads(glibc_output, object_pairs_hook=HelperParseOutput)
  for function, items in jsondata[upper_key].iteritems():
    # handle the jsondata with duplicate keys
    if isinstance(items, list):
      for item in items:
        current_metadata = metadata.copy()
        for key, val in item.iteritems():
          metric = '{0}:{1}'.format(function, key)
          for subitem, value in val.iteritems():
            current_metadata[subitem] = value
          results.append(sample.Sample(metric, -1, '', current_metadata))
    # handle the jsondata with unique keys
    else:
      for item in items:
        current_metadata = metadata.copy()
        metric = '{0}:{1}'.format(function, item)
        for subitem, value in items[item].iteritems():
          current_metadata[subitem] = value
        results.append(sample.Sample(metric, -1, '', current_metadata))


def Run(benchmark_spec):
  """Run Glibc on the cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  results = []

  glibc_user_benchset = ' '.join(FLAGS.glibc_benchset)
  vm.RobustRemoteCommand('cd %s/glibc/glibc-build && '
                         'make bench BENCHSET="%s"' % (
                             linux_packages.INSTALL_DIR, glibc_user_benchset))

  logging.info('Glibc Benchmark Tests Results:')
  # Parse the output for "bench-math", "bench-string" and "bench-pthread".
  if any(i in GLIBC_BENCH for i in FLAGS.glibc_benchset):
    stdout, _ = vm.RemoteCommand(
        'cat {0}/bench.out'.format(RESULTS_DIR), should_log=True)
    ParseOutput(stdout, benchmark_spec, 'functions', results)
  # Parse the output for "malloc-thread".
  if any(i in GLIBC_BENCH_MALLOC for i in FLAGS.glibc_benchset):
    thread_num = ['1', '8', '16', '32']
    for num in thread_num:
      stdout, _ = vm.RemoteCommand(
          'cat {0}/bench-malloc-thread-{1}.out'.format(RESULTS_DIR, num),
          should_log=True)
      ParseOutput(stdout, benchmark_spec, 'functions', results)
  # Parse the output for "math-benchset".
  if any(i in GLIBC_MATH_BENCHSET for i in FLAGS.glibc_benchset):
    stdout, _ = vm.RemoteCommand(
        'cat {0}/bench-math-inlines.out'.format(RESULTS_DIR), should_log=True)
    ParseOutput('{%s}' % stdout, benchmark_spec, 'math-inlines', results)
  return results


def Cleanup(unused_benchmark_spec):
  """Cleanup Glibc on the cluster."""
  pass
