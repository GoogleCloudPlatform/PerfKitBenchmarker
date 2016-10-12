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

"""Module containing blazemark installation and cleanup functions."""

import copy
import logging
import os

from perfkitbenchmarker import regex_util
from perfkitbenchmarker.linux_packages import blaze
from perfkitbenchmarker.linux_packages import fortran

BLAZEMARK_FOLDER = 'blazemark'
BLAZEMARK_DIR = os.path.join(blaze.BLAZE_DIR, BLAZEMARK_FOLDER)
CONFIG = 'Configfile_blazemark'

THROUGHPUT_HEADER_REGEX = (
    r'(\w+[\w\- ]+\w+)\s*(\([0-9]+% filled\))*\s*\[([\w/]+)\]:([0-9\s.]+)')
THROUGHPUT_RESULT_REGEX = r'([0-9]+)\s*([0-9.]+)'
FILLED_REGEX = r'([0-9.]+)% filled'

LIBS = frozenset([
    'C-like', 'Classic', 'Blaze', 'Boost uBLAS', 'Blitz++',
    'GMM++', 'Armadillo', 'MTL', 'Eigen'])


def GetBinaries(vm):
  """Find available blazemark binaries."""
  out, _ = vm.RemoteCommand('ls %s' % (
      os.path.join(BLAZEMARK_DIR, 'bin')))
  return out.split()


def _SimplfyLibName(name):
  """Simply library name parsed from output.

  Args:
    name: string. Name parsed from blazemark output.

  Returns:
    A simplified name defined in LIBS.
  """
  for lib in LIBS:
    if lib in name:
      return lib
  return name


def _ParseThroughput(out, test):
  """Parse throughput section of blazemark results.

  Args:
    out: string. Blazemark output in raw string format.
    test: string. Name of the test ran.

  Returns:
    A list of samples. Each sample if a 4-tuple of (benchmark_name, value, unit,
    metadata).
  """
  matches = regex_util.ExtractAllMatches(THROUGHPUT_HEADER_REGEX, out)
  results = []
  for m in matches:
    lib = _SimplfyLibName(m[0])
    metadata = {}
    filled = m[1]
    if filled:
      metadata['% filled'] = regex_util.ExtractFloat(FILLED_REGEX, filled)
    unit = m[-2]
    for v in regex_util.ExtractAllMatches(THROUGHPUT_RESULT_REGEX, m[-1]):
      metadata['N'] = int(v[0])
      results.append((
          '_'.join([test, lib, 'Throughput']),  # Metric name
          float(v[1]),  # Value
          unit,  # Unit
          copy.deepcopy(metadata)))  # Metadata
  logging.info('Results for %s:\n %s', test, results)
  return results


def _ParseResult(out, test):
  """Parse blazemark results.

  Sample output:
  https://bitbucket.org/blaze-lib/blaze/wiki/Blazemark#!command-line-parameters
  Dense Vector/Dense Vector Addition:
   C-like implementation [MFlop/s]:
     100         1115.44
     10000000    206.317
   Classic operator overloading [MFlop/s]:
     100         415.703
     10000000    112.557
   Blaze [MFlop/s]:
     100         2602.56
     10000000    292.569
   Boost uBLAS [MFlop/s]:
     100         1056.75
     10000000    208.639
   Blitz++ [MFlop/s]:
     100         1011.1
     10000000    207.855
   GMM++ [MFlop/s]:
     100         1115.42
     10000000    207.699
   Armadillo [MFlop/s]:
     100         1095.86
     10000000    208.658
   MTL [MFlop/s]:
     100         1018.47
     10000000    209.065
   Eigen [MFlop/s]:
     100         2173.48
     10000000    209.899
   N=100, steps=55116257
     C-like      = 2.33322  (4.94123)
     Classic     = 6.26062  (13.2586)
     Blaze       = 1        (2.11777)
     Boost uBLAS = 2.4628   (5.21565)
     Blitz++     = 2.57398  (5.4511)
     GMM++       = 2.33325  (4.94129)
     Armadillo   = 2.3749   (5.0295)
     MTL         = 2.55537  (5.41168)
     Eigen       = 1.19742  (2.53585)
   N=10000000, steps=8
     C-like      = 1.41805  (0.387753)
     Classic     = 2.5993   (0.710753)
     Blaze       = 1        (0.27344)
     Boost uBLAS = 1.40227  (0.383437)
     Blitz++     = 1.40756  (0.384884)
     GMM++       = 1.40862  (0.385172)
     Armadillo   = 1.40215  (0.383403)
     MTL         = 1.39941  (0.382656)
     Eigen       = 1.39386  (0.381136)

  Args:
    out: string. Blazemark output in raw string format.
    test: string. Name of the test ran.

  Returns:
    A list of samples. Each sample if a 4-tuple of (benchmark_name, value, unit,
    metadata).
  """
  return _ParseThroughput(out, test)


def RunTest(vm, test, args):
  """Run blazemark test on vm.

  Args:
    vm: VirtualMachine. The VM to run blazemark.
    test: string. The test name to run.
    args: List of string. Additional flags passed to test.

  Returns:
    A list of samples. Each sample if a 4-tuple of (benchmark_name, value, unit,
    metadata).
  """
  additional_args = ''
  if 'all' not in args:
    additional_args = ' '.join(args)
  out, _ = vm.RemoteCommand(
      'cd %s; ./%s %s' % (
          os.path.join(BLAZEMARK_DIR, 'bin'), test, additional_args))
  return _ParseResult(out, test)


def _Configure(vm):
  """Configure and build blazemark on vm."""
  vm.PushDataFile(CONFIG, BLAZEMARK_DIR)
  vm.RemoteCommand(
      'cd %s;sed -i \'s|FORTRAN_PATH|%s|\' %s' % (
          BLAZEMARK_DIR,
          os.path.dirname(fortran.GetLibPath(vm)),
          CONFIG))
  vm.RemoteCommand('cd %s; ./configure %s; make' % (BLAZEMARK_DIR, CONFIG))


def _Install(vm):
  """Install blazemark."""
  vm.Install('g++5')
  vm.Install('build_tools')
  vm.Install('boost')
  vm.Install('blaze')
  vm.Install('lapack')
  _Configure(vm)


def YumInstall(vm):
  """Installs the blazemark package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the blazemark package on the VM."""
  _Install(vm)
