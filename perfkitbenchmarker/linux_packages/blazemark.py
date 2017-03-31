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

"""Module containing blazemark installation and cleanup functions."""

import copy
import logging
import os

from perfkitbenchmarker import data
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import blaze
from perfkitbenchmarker.linux_packages import fortran

BLAZEMARK_FOLDER = 'blazemark'
BLAZEMARK_DIR = os.path.join(blaze.BLAZE_DIR, BLAZEMARK_FOLDER)
CONFIG_TEMPLATE = 'blazemark_config.j2'
CONFIG = 'config'

THROUGHPUT_HEADER_REGEX = (
    r'(\w+[\w\- ]+\w+)\s*(\([0-9.]+% filled\))*\s*[\[\(]'
    '([\w/]+)[\]\)]:([0-9\s.e\-+]+)')
THROUGHPUT_RESULT_REGEX = r'([0-9]+)\s*([0-9.e\-+]+)'
FILLED_REGEX = r'([0-9.]+)% filled'

LIBS = frozenset([
    'C-like', 'Classic', 'Blaze', 'Boost uBLAS', 'Blitz++',
    'GMM++', 'Armadillo', 'MTL', 'Eigen'])

BLAZEMARK_BINARIES = frozenset([
    'cg', 'daxpy', 'dmatsvecmult', 'dvecdvecsub', 'mat3mat3mult',
    'smatdmatmult', 'smattsmatadd', 'svectdvecmult', 'tdmattdmatmult',
    'tmat3mat3mult', 'tsmatdmatmult', 'tsvecdmatmult', 'tvec6tmat6mult',
    'complex1', 'dmatdmatadd', 'dmattdmatadd', 'dvecnorm', 'mat3tmat3mult',
    'smatdvecmult', 'smattsmatmult', 'svectsvecmult', 'tdmattsmatadd',
    'tmat3tmat3add', 'tsmatdvecmult', 'tsvecdvecmult', 'vec3vec3add',
    'complex2', 'dmatdmatmult', 'dmattdmatmult', 'dvecscalarmult',
    'mat3vec3mult', 'smatscalarmult', 'svecdvecadd', 'tdmatdmatadd',
    'tdmattsmatmult', 'tmat3tmat3mult', 'tsmatsmatadd', 'tsvecsmatmult',
    'vec6vec6add', 'complex3', 'dmatdmatsub', 'dmattrans', 'dvecsvecadd',
    'mat6mat6add', 'smatsmatadd', 'svecdveccross', 'tdmatdmatmult',
    'tdvecdmatmult', 'tmat3vec3mult', 'tsmatsmatmult', 'tsvecsvecmult',
    'complex4', 'dmatdvecmult', 'dmattsmatadd', 'dvecsveccross', 'mat6mat6mult',
    'smatsmatmult', 'svecdvecmult', 'tdmatdvecmult', 'tdvecdvecmult',
    'tmat6mat6mult', 'tsmatsvecmult', 'tsvectdmatmult', 'complex5', 'dmatinv',
    'dmattsmatmult', 'dvecsvecmult', 'mat6tmat6mult', 'smatsvecmult',
    'svecscalarmult', 'tdmatsmatadd', 'tdvecsmatmult', 'tmat6tmat6add',
    'tsmattdmatadd', 'tsvectsmatmult', 'complex6', 'dmatscalarmult',
    'dvecdvecadd', 'dvectdvecmult', 'mat6vec6mult', 'smattdmatadd',
    'svecsvecadd', 'tdmatsmatmult', 'tdvecsvecmult', 'tmat6tmat6mult',
    'tsmattdmatmult', 'tvec3mat3mult', 'complex7', 'dmatsmatadd',
    'dvecdveccross', 'dvectsvecmult', 'memorysweep', 'smattdmatmult',
    'svecsveccross', 'tdmatsvecmult', 'tdvectdmatmult', 'tmat6vec6mult',
    'tsmattsmatadd', 'tvec3tmat3mult', 'complex8', 'dmatsmatmult',
    'dvecdvecmult', 'mat3mat3add', 'smatdmatadd', 'smattrans',
    'svecsvecmult', 'tdmattdmatadd', 'tdvectsmatmult', 'tsmatdmatadd',
    'tsmattsmatmult', 'tvec6mat6mult'])


def GetBinaries():
  """Find available blazemark binaries."""
  return BLAZEMARK_BINARIES


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
      results.append(sample.Sample(
          '_'.join([test, lib, 'Throughput']),  # Metric name
          float(v[1]),  # Value
          unit,  # Unit
          copy.deepcopy(metadata)))  # Metadata
  logging.info('Results for %s:\n %s', test, results)
  return results


def RunTest(vm, test):
  """Run blazemark test on vm.

  Args:
    vm: VirtualMachine. The VM to run blazemark.
    test: string. The test name to run.

  Returns:
    A list of samples. Each sample if a 4-tuple of (benchmark_name, value, unit,
    metadata).
  """
  out, _ = vm.RemoteCommand(
      'cd %s; export BLAZE_NUM_THREADS=%s; ./%s -only-blaze' % (
          os.path.join(BLAZEMARK_DIR, 'bin'), vm.num_cpus, test))
  ret = []
  try:
    ret = _ParseResult(out, test)
  except regex_util.NoMatchError:
    logging.exception('Parsing failed for %s.\n', test)
  return ret


def _Configure(vm):
  """Configure and build blazemark on vm."""
  vm.RenderTemplate(
      data.ResourcePath(CONFIG_TEMPLATE),
      os.path.join(BLAZEMARK_DIR, CONFIG),
      {'compiler': '"g++-5"',
       'compiler_flags': (
           '"-Wall -Wextra -Werror -Wshadow -Woverloaded-virtual -ansi -O3 '
           '-mavx -DNDEBUG -fpermissive -ansi -O3 -DNDEBUG '
           '-DBLAZE_USE_BOOST_THREADS --std=c++14"'),
       'lapack_path': '"/tmp/pkb/lapack-3.6.1/lib"',
       'lapack_libs': '"-llapack -lblas -L%s -lgfortran"'
       % os.path.dirname(fortran.GetLibPath(vm))})
  vm.RemoteCommand('cd %s; ./configure %s; make -j %s' % (
      BLAZEMARK_DIR, CONFIG, vm.num_cpus))


def _Install(vm):
  """Install blazemark."""
  for package in ['g++5', 'build_tools', 'boost', 'blaze', 'lapack']:
    vm.Install(package)
  _Configure(vm)


def YumInstall(vm):
  """Installs the blazemark package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the blazemark package on the VM."""
  _Install(vm)
