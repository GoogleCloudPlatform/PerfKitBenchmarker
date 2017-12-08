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
"""Runs DaCapo benchmarks.

This benchmark runs the various DaCapo benchmarks. More information can be found
at: http://dacapobench.org/
"""

import os
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import sample

flags.DEFINE_string('dacapo_jar_filename', 'dacapo-9.12-bach.jar',
                    'Filename of DaCapo jar file.')
flags.DEFINE_enum('dacapo_benchmark', 'luindex', ['luindex', 'lusearch'],
                  'Name of specific DaCapo benchmark to execute.')
flags.DEFINE_integer('dacapo_num_iters', 1, 'Number of iterations to execute.')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'dacapo'
BENCHMARK_CONFIG = """
dacapo:
  description: Runs DaCapo benchmarks
  vm_groups:
    default:
      vm_spec: *default_single_core
"""
_PASS_PATTERN = re.compile(r'^=====.*PASSED in (\d+) msec =====$')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install the DaCapo benchmark suite on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.vms[0].Install('dacapo')


def Run(benchmark_spec):
  """Run the DaCapo benchmark on the vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A singleton list of sample.Sample objects containing the DaCapo benchmark
    run time (in msec).

  Raises:
    errors.Benchmarks.RunError if the DaCapo benchmark didn't succeed.
  """
  _, stderr = benchmark_spec.vms[0].RemoteCommand(
      'java -jar %s %s -n %i --scratch-directory=%s' %
      (os.path.join(linux_packages.INSTALL_DIR, FLAGS.dacapo_jar_filename),
       FLAGS.dacapo_benchmark, FLAGS.dacapo_num_iters,
       os.path.join(linux_packages.INSTALL_DIR, 'dacapo_scratch')))
  for line in stderr.splitlines():
    m = _PASS_PATTERN.match(line)
    if m:
      return [sample.Sample('run_time', float(m.group(1)), 'ms')]
  raise errors.Benchmarks.RunError(
      'DaCapo benchmark %s failed.' % FLAGS.dacapo_benchmark)


def Cleanup(benchmark_spec):
  """Cleanup the DaCapo benchmark on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  benchmark_spec.vms[0].RemoteCommand(
      'rm -rf %s' % os.path.join(linux_packages.INSTALL_DIR, 'dacapo_scratch'))
