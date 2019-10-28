# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing wrk2 installation, cleanup, and parsing functions.

wrk2 is a fork of wrk, supporting improved latency stats and fixed throughput.
"""

import logging
import posixpath
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

WRK2_URL = ('https://github.com/giltene/wrk2/archive/'
            'c4250acb6921c13f8dccfc162d894bd7135a2979.tar.gz')
WRK2_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'wrk2')
WRK2_PATH = posixpath.join(WRK2_DIR, 'wrk')

FLAGS = flags.FLAGS

flags.DEFINE_bool('wrk2_corrected_latency', True,
                  'Whether or not response latency is corrected.\n'
                  'If True, wrk2 measure response latency from the time the '
                  'transmission should have occurred according to the constant '
                  'throughput configured for the run.\n'
                  'If False, response latency is the time that actual '
                  'transmission of a request occured.')


def _Install(vm):
  vm.Install('curl')
  vm.Install('build_tools')
  vm.Install('openssl')
  vm.RemoteCommand(
      ('mkdir -p {0} && '
       'curl -L {1} | tar -xzf - -C {0} --strip-components 1').format(
           WRK2_DIR, WRK2_URL))
  vm.RemoteCommand('make -C {}'.format(WRK2_DIR))


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)


def _ParseOutput(output_text):
  """Parses the output of wrk2.

  Args:
    output_text: str. Output for wrk2

  Yields:
    (variable_name, value, unit) tuples.

  Raises:
    ValueError: When requests / latency statistics cannot be found.
  """
  inner_pat = r'^\s*(\d+\.\d+)%\s+(\d+\.\d+)([um]s)\n'
  regex = re.compile(
      r'^\s*Latency Distribution \(HdrHistogram - Recorded Latency\)\n'
      r'((?:^' + inner_pat + ')+)', re.MULTILINE)
  m = regex.search(output_text)
  if not m:
    raise ValueError('No match for {} in\n{}'.format(regex, output_text))
  matches = re.findall(inner_pat, m.group(1), re.MULTILINE)
  for percentile, value, unit in matches:
    variable = 'p{} latency'.format(percentile.rstrip('0').rstrip('.'))
    value = float(value)
    if unit == 'us':
      unit = 'ms'
      value /= 1000.
    if unit != 'ms':
      logging.warn('Expected "ms", got %s for "%s"', unit, m.group(1))
    yield variable, value, unit

  # Errors, requests
  m = re.search(r'(\d+) requests in \d', output_text)
  if not m:
    raise ValueError('Request count not found in:\n' + output_text)
  requests = int(m.group(1))
  yield 'requests', requests, ''
  m = re.search(r'Non-2xx or 3xx responses: (\d+)', output_text)
  if m:
    errors = int(m.group(1))
    error_rate = int(m.group(1)) / float(requests)
  else:
    errors = 0
    error_rate = 0
  yield 'error_rate', error_rate, ''
  yield 'errors', errors, ''

  if error_rate > 0.1:
    raise ValueError('More than 10% of requests failed.')


def Run(vm, target, rate, connections=1, duration=60, script_path=None,
        threads=None):
  """Runs wrk against a given target.

  Args:
    vm: Virtual machine.
    target: URL to fetch.
    rate: int. Target request rate, in QPS.
    connections: Number of concurrent connections.
    duration: Duration of the test, in seconds.
    script_path: If specified, a lua script to execute.
    threads: Number of threads. Defaults to min(connections, num_cores).
  Yields:
    sample.Sample objects with results.
  """
  if threads is None:
    threads = min(connections, vm.NumCpusForBenchmark())
  cmd = ('{wrk} '
         '--rate={rate} '
         '--connections={connections} '
         '--threads={threads} '
         '--duration={duration} '
         '--{corrected}').format(
             wrk=WRK2_PATH, connections=connections, threads=threads,
             rate=rate, duration=duration,
             corrected=(
                 'latency' if FLAGS.wrk2_corrected_latency else 'u_latency'))
  if script_path:
    cmd += ' --script ' + script_path
  cmd += ' ' + target
  stdout, _ = vm.RemoteCommand(cmd)
  for variable, value, unit in _ParseOutput(stdout):
    yield sample.Sample(variable, value, unit,
                        metadata={'connections': connections,
                                  'threads': threads,
                                  'duration': duration,
                                  'rate': rate,
                                  'corrected': False})
