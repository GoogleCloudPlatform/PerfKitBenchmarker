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


"""Module containing HPCC installation and cleanup functions.

The HPC Challenge is a collection of High Performance Computing benchmarks,
including High Performance Linpack (HPL). More information can be found here:
http://icl.cs.utk.edu/hpcc/
"""

import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker.linux_packages import openblas

HPCC_TAR = 'hpcc-1.4.3.tar.gz'
HPCC_URL = 'http://icl.cs.utk.edu/projectsfiles/hpcc/download/' + HPCC_TAR
HPCC_DIR = '%s/hpcc-1.4.3' % INSTALL_DIR

MAKE_FLAVOR_CBLAS = 'Linux_PII_CBLAS'
MAKE_FLAVOR_MKL = 'intel64'
MAKE_FLAVOR_OPEN_BLAS = 'OPEN_BLAS'

HPCC_MAKEFILE_CBLAS = 'Make.%s' % MAKE_FLAVOR_CBLAS
HPCC_MAKEFILE_MKL = 'Make.%s' % MAKE_FLAVOR_MKL
HPCC_MAKEFILE_OPEN_BLAS = 'Make.%s' % MAKE_FLAVOR_OPEN_BLAS

HPCC_MAKEFILE_PATH_MKL = '%s/hpl/%s' % (HPCC_DIR, HPCC_MAKEFILE_MKL)
HPCC_MAKEFILE_PATH_OPEN_BLAS = '%s/hpl/%s' % (HPCC_DIR, HPCC_MAKEFILE_OPEN_BLAS)

HPCC_MATH_LIBRARY_OPEN_BLAS = 'openblas'
HPCC_MATH_LIBRARY_MKL = 'mkl'

flags.DEFINE_enum(
    'hpcc_math_library', HPCC_MATH_LIBRARY_OPEN_BLAS,
    [HPCC_MATH_LIBRARY_OPEN_BLAS, HPCC_MATH_LIBRARY_MKL],
    'The math library to use when compiling hpcc: openBlas or MKL. '
    'The default is openBlas')
FLAGS = flags.FLAGS


def _Install(vm):
  """Installs the HPCC package on the VM."""
  vm.Install('wget')
  vm.Install('openmpi')
  vm.RemoteCommand('wget %s -P %s' % (HPCC_URL, INSTALL_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (INSTALL_DIR, HPCC_TAR))
  if FLAGS.hpcc_math_library == HPCC_MATH_LIBRARY_OPEN_BLAS:
    _CompileHpccOpenblas(vm)
  elif FLAGS.hpcc_math_library == HPCC_MATH_LIBRARY_MKL:
    _CompileHpccMKL(vm)
  else:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Unexpected hpcc_math_library option encountered.')


def _CompileHpccOpenblas(vm):
  """Compile HPCC with OpenBlas."""
  vm.Install('openblas')
  vm.RemoteCommand(
      'cp %s/hpl/setup/%s %s' %
      (HPCC_DIR, HPCC_MAKEFILE_CBLAS, HPCC_MAKEFILE_PATH_OPEN_BLAS))
  sed_cmd = (
      'sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
      '-e "s/\\$(HOME)\\/netlib\\/ARCHIVES\\/Linux_PII/%s/" '
      '-e "s/libcblas.*/libopenblas.a/" '
      '-e "s/\\-lm/\\-lgfortran \\-lm/" %s' %
      (re.escape(openblas.OPENBLAS_DIR), HPCC_MAKEFILE_PATH_OPEN_BLAS))
  vm.RemoteCommand(sed_cmd)
  vm.RemoteCommand('cd %s; make arch=OPEN_BLAS' % HPCC_DIR)


def _CompileHpccMKL(vm):
  """Compiling HPCC with Intel MKL.

  The following link provides instructions of using intel MKL in hpcc_benchmark.
  https://software.intel.com/en-us/articles/performance-tools-for-software-developers-use-of-intel-mkl-in-hpcc-benchmark
  TODO(yanfeiren):The free version MKL pacakage does not have
  'interfaces/fftw2x_cdft' which is the MPI FFTW 2.x interfaces to the
  Intel MKL Cluster FFT. Such that we have to at first install OpenBlas and
  build hpcc binary using OpenBlas. Need to investigate how to build hpcc
  binary without 'interfaces/fftw2x_cdft'.
  """
  _CompileHpccOpenblas(vm)
  vm.RemoteCommand('cd %s; rm hpcc' % HPCC_DIR)
  vm.Install('mkl')
  vm.RemoteCommand(
      'cp %s/hpl/setup/%s %s' %
      (HPCC_DIR, HPCC_MAKEFILE_CBLAS, HPCC_MAKEFILE_PATH_MKL))
  mkl_lalib = (
      '-Wl,--start-group $(LAdir)/libfftw2xc_double_gnu.a '
      '$(LAdir)/libfftw2xf_double_gnu.a '
      '$(LAdir)/libmkl_intel_lp64.a '
      '$(LAdir)/libmkl_intel_thread.a '
      '$(LAdir)/libmkl_core.a '
      '$(LAdir)/libmkl_blas95_lp64.a '
      '-Wl,--end-group')
  mkl_ccflags = (
      ' -Wl,--no-as-needed -ldl -lmpi -liomp5 -lpthread -lm '
      '-DUSING_FFTW -DMKL_INT=long -DLONG_IS_64BITS')
  sed_cmd_mkl = (
      'sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
      '-e "s/\\$(HOME)\\/netlib\\/ARCHIVES\\/Linux_PII/'
      '\\/opt\\/intel\\/mkl\\/lib\\/intel64/" '
      '-e "s/\\$(LAdir)\\/libcblas.*/%s/" '
      '-e "s/\\-lm/\\-lgfortran \\-lm/" '
      '-e "/CCFLAGS / s/$/%s/" %s' %
      (re.escape(mkl_lalib), re.escape(mkl_ccflags), HPCC_MAKEFILE_PATH_MKL))
  vm.RemoteCommand(sed_cmd_mkl)
  vm.RemoteCommand('source /opt/intel/compilers_and_libraries/linux/bin/'
                   'compilervars.sh -arch intel64 -platform linux && '
                   'cd %s; make arch=intel64' % HPCC_DIR)


def YumInstall(vm):
  """Installs the HPCC package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the HPCC package on the VM."""
  _Install(vm)
