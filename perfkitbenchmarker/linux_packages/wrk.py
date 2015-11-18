# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing wrk installation and cleanup functions.

WRK is an extremely scalable HTTP benchmarking tool.
https://github.com/wg/wrk
"""

import csv
import io
import posixpath

from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


WRK_URL = 'https://github.com/wg/wrk/archive/4.0.1.tar.gz'
WRK_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'wrk')
WRK_PATH = posixpath.join(WRK_DIR, 'wrk')

# Rather than parse WRK's free text output, this script is used to generate a
# CSV report
_LUA_SCRIPT_NAME = 'wrk_latency.lua'
_LUA_SCRIPT_PATH = posixpath.join(WRK_DIR, _LUA_SCRIPT_NAME)

# Default socket / request timeout.
_TIMEOUT = '10s'
# WRK always outputs a free text report. _LUA_SCRIPT_NAME (above)
# writes this prefix before the CSV output begins.
_CSV_PREFIX = '==CSV==\n'


def _Install(vm):
  vm.Install('build_tools')
  vm.Install('curl')
  vm.Install('openssl')

  vm.RemoteCommand(('mkdir -p {0} && curl -L {1} '
                    '| tar --strip-components=1 -C {0} -xzf -').format(
                        WRK_DIR,
                        WRK_URL))
  vm.RemoteCommand('cd {} && make'.format(WRK_DIR))
  vm.PushDataFile(_LUA_SCRIPT_NAME, _LUA_SCRIPT_PATH)


def YumInstall(vm):
  """Installs wrk on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs wrk on the VM."""
  _Install(vm)


def _ParseOutput(output_text):
  """Parses the output of _LUA_SCRIPT_NAME.

  Yields:
    (variable_name, value, unit) tuples.
  """
  if _CSV_PREFIX not in output_text:
    raise ValueError('{0} not found in\n{1}'.format(_CSV_PREFIX, output_text))
  csv_fp = io.BytesIO(str(output_text).rsplit(_CSV_PREFIX, 1)[-1])
  reader = csv.DictReader(csv_fp)
  if (frozenset(reader.fieldnames) !=
      frozenset(['variable', 'value', 'unit'])):
    raise ValueError('Unexpected fields: {}'.format(reader.fieldnames))
  for row in reader:
    yield row['variable'], float(row['value']), row['unit']


def Run(vm, target, connections=1, duration=60):
  """Runs wrk against a given target.

  Args:
    vm: Virtual machine.
    target: URL to fetch.
    connections: Number of concurrent connections.
    duration: Duration of the test, in seconds.
  Yields:
    sample.Sample objects with results.
  """
  threads = min(connections, vm.num_cpus)
  cmd = ('{wrk} --connections={connections} --threads={threads} '
         '--duration={duration} '
         '--timeout={timeout} '
         '--script={script} {target}').format(
             wrk=WRK_PATH, connections=connections, threads=threads,
             script=_LUA_SCRIPT_PATH, target=target,
             duration=duration, timeout=_TIMEOUT)
  stdout, _ = vm.RemoteCommand(cmd)
  for variable, value, unit in _ParseOutput(stdout):
    yield sample.Sample(variable, value, unit,
                        metadata={'connections': connections,
                                  'threads': threads,
                                  'duration': duration})
