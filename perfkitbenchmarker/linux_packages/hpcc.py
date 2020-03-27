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

import os
import posixpath
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import amdblis
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from perfkitbenchmarker.linux_packages import openblas


PACKAGE_NAME = 'hpcc'
HPCC_TAR = 'hpcc-1.5.0.tar.gz'
HPCC_URL = 'https://icl.cs.utk.edu/projectsfiles/hpcc/download/' + HPCC_TAR
PREPROVISIONED_DATA = {
    HPCC_TAR: '0a6fef7ab9f3347e549fed65ebb98234feea9ee18aea0c8f59baefbe3cf7ffb8'
}
PACKAGE_DATA_URL = {
    HPCC_TAR: HPCC_URL
}

HPCC_DIR = '%s/hpcc-1.5.0' % INSTALL_DIR
HPCC_VERSION = '1.5.0'

MAKE_FLAVOR_CBLAS = 'Linux_PII_CBLAS'
MAKE_FLAVOR_MKL = 'intel64'
MAKE_FLAVOR_OPEN_BLAS = 'OPEN_BLAS'
MAKE_FLAVOR_AMD_BLIS = 'AMD_BLIS'

HPCC_MAKEFILE_CBLAS = 'Make.%s' % MAKE_FLAVOR_CBLAS
HPCC_MAKEFILE_MKL = 'Make.%s' % MAKE_FLAVOR_MKL
HPCC_MAKEFILE_OPEN_BLAS = 'Make.%s' % MAKE_FLAVOR_OPEN_BLAS
HPCC_MAKEFILE_AMD_BLIS = 'Make.%s' % MAKE_FLAVOR_AMD_BLIS

HPCC_MAKEFILE_PATH_MKL = '%s/hpl/%s' % (HPCC_DIR, HPCC_MAKEFILE_MKL)
HPCC_MAKEFILE_PATH_OPEN_BLAS = '%s/hpl/%s' % (HPCC_DIR, HPCC_MAKEFILE_OPEN_BLAS)
HPCC_MAKEFILE_PATH_AMD_BLIS = '%s/hpl/%s' % (HPCC_DIR, HPCC_MAKEFILE_AMD_BLIS)

HPCC_MATH_LIBRARY_OPEN_BLAS = 'openblas'
HPCC_MATH_LIBRARY_AMD_BLIS = 'amdblis'
HPCC_MATH_LIBRARY_MKL = 'mkl'

# A dict mapping HPCC benchmarks to dicts mapping summary result names to units.
# The name of the summary result is added as a metric with that name and the
# specified units.
HPCC_METRIC_MAP = {
    'MPI RandomAccess': {
        'MPIRandomAccess_time': 'seconds',
        'MPIRandomAccess_CheckTime': 'seconds',
        'MPIRandomAccess_ExeUpdates': 'updates',
        'MPIRandomAccess_GUPs': 'GUP/s',
    },
    'StarRandomAccess': {
        'StarRandomAccess_GUPs': 'GUP/s',
    },
    'SingleRandomAccess': {
        'SingleRandomAccess_GUPs': 'GUP/s',
    },
    'MPI RandomAccess LCG': {
        'MPIRandomAccess_LCG_time': 'seconds',
        'MPIRandomAccess_LCG_CheckTime': 'seconds',
        'MPIRandomAccess_LCG_ExeUpdates': 'updates',
        'MPIRandomAccess_LCG_GUPs': 'GUP/s',
    },
    'StarRandomAccess LCG': {
        'StarRandomAccess_LCG_GUPs': 'GUP/s',
    },
    'SingleRandomAccess LCG': {
        'SingleRandomAccess_LCG_GUPs': 'GUP/s',
    },
    'PTRANS': {
        'PTRANS_GBs': 'GB/s',
        'PTRANS_time': 'seconds',
    },
    'StarDGEMM': {
        'StarDGEMM_Gflops': 'Gflop/s',
    },
    'SingleDGEMM': {
        'SingleDGEMM_Gflops': 'Gflop/s',
    },
    'StarSTREAM': {
        'StarSTREAM_Copy': 'GB/s',
        'StarSTREAM_Scale': 'GB/s',
        'StarSTREAM_Add': 'GB/s',
        'StarSTREAM_Triad': 'GB/s',
    },
    'SingleSTREAM': {
        'SingleSTREAM_Copy': 'GB/s',
        'SingleSTREAM_Scale': 'GB/s',
        'SingleSTREAM_Add': 'GB/s',
        'SingleSTREAM_Triad': 'GB/s',
    },
    'MPIFFT': {
        'MPIFFT_Gflops': 'Gflop/s',
        'MPIFFT_time0': 'seconds',
        'MPIFFT_time1': 'seconds',
        'MPIFFT_time2': 'seconds',
        'MPIFFT_time3': 'seconds',
        'MPIFFT_time4': 'seconds',
        'MPIFFT_time5': 'seconds',
        'MPIFFT_time6': 'seconds',
    },
    'StarFFT': {
        'StarFFT_Gflops': 'Gflop/s',
    },
    'SingleFFT': {
        'SingleFFT_Gflops': 'Gflop/s',
    },
    'Latency/Bandwidth': {
        'MaxPingPongLatency_usec': 'usec',
        'RandomlyOrderedRingLatency_usec': 'usec',
        'MinPingPongBandwidth_GBytes': 'GB',
        'NaturallyOrderedRingBandwidth_GBytes': 'GB',
        'RandomlyOrderedRingBandwidth_GBytes': 'GB',
        'MinPingPongLatency_usec': 'usec',
        'AvgPingPongLatency_usec': 'usec',
        'MaxPingPongBandwidth_GBytes': 'GB',
        'AvgPingPongBandwidth_GBytes': 'GB',
        'NaturallyOrderedRingLatency_usec': 'usec',
    },
    'HPL': {
        'HPL_Tflops': 'Tflop/s',
        'HPL_time': 'seconds',
    },
}

# A dict mapping HPCC benchmarks to sets of summary result names that should be
# added to the metadata for a benchmark.
HPCC_METADATA_MAP = {
    'MPI RandomAccess': {
        'MPIRandomAccess_N',
        'MPIRandomAccess_Errors',
        'MPIRandomAccess_ErrorsFraction',
        'MPIRandomAccess_TimeBound',
        'MPIRandomAccess_Algorithm',
    },
    'StarRandomAccess': {'RandomAccess_N'},
    'SingleRandomAccess': {'RandomAccess_N'},
    'MPI RandomAccess LCG': {
        'MPIRandomAccess_LCG_N',
        'MPIRandomAccess_LCG_Errors',
        'MPIRandomAccess_LCG_ErrorsFraction',
        'MPIRandomAccess_LCG_TimeBound',
        'MPIRandomAccess_LCG_Algorithm',
    },
    'StarRandomAccess LCG': {'RandomAccess_LCG_N'},
    'SingleRandomAccess LCG': {'RandomAccess_LCG_N'},
    'PTRANS': {
        'PTRANS_residual',
        'PTRANS_n',
        'PTRANS_nb',
        'PTRANS_nprow',
        'PTRANS_npcol',
    },
    'StarDGEMM': {'DGEMM_N'},
    'SingleDGEMM': {'DGEMM_N'},
    'StarSTREAM': {
        'STREAM_Threads',
        'STREAM_VectorSize',
    },
    'SingleSTREAM': {
        'STREAM_Threads',
        'STREAM_VectorSize',
    },
    'MPIFFT': {
        'MPIFFT_N',
        'MPIFFT_maxErr',
        'MPIFFT_Procs',
    },
    'StarFFT': {'FFT_N'},
    'SingleFFT': {'FFT_N'},
    'Latency/Bandwidth': {},
    'HPL': {
        'HPL_N',
        'HPL_NB',
        'HPL_nprow',
        'HPL_npcol',
        'HPL_depth',
        'HPL_nbdiv',
        'HPL_nbmin',
        'HPL_ctop',
    },
}

# The names of the benchmarks.
HPCC_BENCHMARKS = sorted(HPCC_METRIC_MAP)


flags.DEFINE_enum(
    'hpcc_math_library', HPCC_MATH_LIBRARY_OPEN_BLAS, [
        HPCC_MATH_LIBRARY_OPEN_BLAS, HPCC_MATH_LIBRARY_MKL,
        HPCC_MATH_LIBRARY_AMD_BLIS
    ], 'The math library to use when compiling hpcc: openblas, mkl, or '
    'amdblis. The default is openblas.')
flags.DEFINE_list(
    'hpcc_benchmarks', [], 'A list of benchmarks in HPCC to run. If none are '
    'specified (the default), then all of the benchmarks are run. In 1.5.0, '
    'the benchmarks may include the following: %s' % ', '.join(HPCC_BENCHMARKS))
flags.register_validator(
    'hpcc_benchmarks',
    lambda hpcc_benchmarks: set(hpcc_benchmarks).issubset(set(HPCC_BENCHMARKS)))
FLAGS = flags.FLAGS


def _LimitBenchmarksToRun(vm, selected_hpcc_benchmarks):
  """Limits the benchmarks to run.

  This function copies hpcc.c to the local machine, comments out code that runs
  benchmarks not listed in selected_hpcc_benchmarks, and then copies hpcc.c back
  to the remote machine.

  Args:
    vm: The machine where hpcc.c was installed.
    selected_hpcc_benchmarks: A set of benchmarks to run.
  """
  remote_hpcc_path = posixpath.join(HPCC_DIR, 'src', 'hpcc.c')
  local_hpcc_path = os.path.join(vm_util.GetTempDir(), 'hpcc.c')
  vm.PullFile(local_hpcc_path, remote_hpcc_path)
  with open(local_hpcc_path) as f:
    lines = f.readlines()

  # Process the main file, commenting out benchmarks that should not be run.
  commenting = False
  with open(local_hpcc_path, 'w') as f:
    for line in lines:
      # Decide whether to continue commenting out code for each benchmark. This
      # is determined by searching for the comment that starts each benchmark.
      match = re.search(r'\/\*\s+(.*?)\s+\*\/', line)
      if match and match.group(1) in HPCC_BENCHMARKS:
        commenting = match.group(1) not in selected_hpcc_benchmarks

      # Start writing once the per-benchmark code is complete. This happens at
      # the hpcc_end: label.
      if re.search('hpcc_end:', line):
        commenting = False

      f.write('// %s' % line if commenting else line)

  vm.PushFile(local_hpcc_path, remote_hpcc_path)


def _Install(vm):
  """Installs the HPCC package on the VM."""
  vm.Install('wget')
  vm.Install('openmpi')
  vm.InstallPreprovisionedPackageData(
      PACKAGE_NAME, PREPROVISIONED_DATA.keys(), INSTALL_DIR)
  vm.RemoteCommand('cd %s && tar xvfz %s' % (INSTALL_DIR, HPCC_TAR))

  if FLAGS.hpcc_benchmarks:
    _LimitBenchmarksToRun(vm, set(FLAGS.hpcc_benchmarks))

  if FLAGS.hpcc_math_library == HPCC_MATH_LIBRARY_OPEN_BLAS:
    _CompileHpccOpenblas(vm)
  elif FLAGS.hpcc_math_library == HPCC_MATH_LIBRARY_MKL:
    _CompileHpccMKL(vm)
  elif FLAGS.hpcc_math_library == HPCC_MATH_LIBRARY_AMD_BLIS:
    _CompileHpccAmdBlis(vm)
  else:
    raise errors.Setup.InvalidFlagConfigurationError(
        'Unexpected hpcc_math_library option encountered.')


def _CompileHpccOpenblas(vm):
  """Compile HPCC with OpenBlas."""
  vm.Install('openblas')
  vm.RemoteCommand(
      'cp %s/hpl/setup/%s %s' %
      (HPCC_DIR, HPCC_MAKEFILE_CBLAS, HPCC_MAKEFILE_PATH_OPEN_BLAS))
  sed_cmd = ('sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
             '-e "s/\\$(HOME)\\/netlib\\/ARCHIVES\\/Linux_PII/%s/" '
             '-e "s/libcblas.*/libopenblas.a/" '
             '-e "s/-funroll-loops/-funroll-loops -std=c99/" '
             '-e "s/\\-lm/\\-lgfortran \\-lm/" %s' %
             (re.escape(openblas.OPENBLAS_DIR), HPCC_MAKEFILE_PATH_OPEN_BLAS))
  vm.RemoteCommand(sed_cmd)
  vm.RemoteCommand('cd %s; make arch=OPEN_BLAS' % HPCC_DIR)


def _CompileHpccAmdBlis(vm):
  """Compile HPCC with AMD BLIS."""
  vm.Install('amdblis')
  vm.RemoteCommand('cp %s/hpl/setup/%s %s' %
                   (HPCC_DIR, HPCC_MAKEFILE_CBLAS, HPCC_MAKEFILE_PATH_AMD_BLIS))
  sed_cmd = ('sed -i -e "/^MP/d" -e "s/gcc/mpicc/" -e "s/g77/mpicc/" '
             '-e "s/\\$(HOME)\\/netlib\\/ARCHIVES\\/Linux_PII/%s/" '
             '-e "s/libcblas.*/lib\\/zen\\/libblis.a/" '
             '-e "s/-funroll-loops/-funroll-loops -std=c99/" '
             '-e "s/\\-lm/\\-lgfortran \\-lm/" %s' %
             (re.escape(amdblis.AMDBLIS_DIR), HPCC_MAKEFILE_PATH_AMD_BLIS))
  vm.RemoteCommand(sed_cmd)
  vm.RemoteCommand('cd %s; make arch=AMD_BLIS' % HPCC_DIR)


def _CompileHpccMKL(vm):
  """Compiling HPCC with Intel MKL.

  The following link provides instructions of using intel MKL in hpcc_benchmark.
  https://software.intel.com/en-us/articles/performance-tools-for-software-developers-use-of-intel-mkl-in-hpcc-benchmark
  TODO(user):The free version MKL pacakage does not have
  'interfaces/fftw2x_cdft' which is the MPI FFTW 2.x interfaces to the
  Intel MKL Cluster FFT. Such that we have to at first install OpenBlas and
  build hpcc binary using OpenBlas. Need to investigate how to build hpcc
  binary without 'interfaces/fftw2x_cdft'.

  Args:
    vm: VirtualMachine object. The VM to install hpcc.
  """
  _CompileHpccOpenblas(vm)
  vm.RemoteCommand('cd %s; rm hpcc' % HPCC_DIR)
  vm.Install('mkl')
  vm.RemoteCommand('cp %s/hpl/setup/%s %s' % (HPCC_DIR, HPCC_MAKEFILE_CBLAS,
                                              HPCC_MAKEFILE_PATH_MKL))
  mkl_lalib = ('-Wl,--start-group $(LAdir)/libfftw2xc_double_gnu.a '
               '$(LAdir)/libfftw2xf_double_gnu.a '
               '$(LAdir)/libmkl_intel_lp64.a '
               '$(LAdir)/libmkl_intel_thread.a '
               '$(LAdir)/libmkl_core.a '
               '$(LAdir)/libmkl_blas95_lp64.a '
               '-Wl,--end-group')
  mkl_ccflags = (' -Wl,--no-as-needed -ldl -lmpi -liomp5 -lpthread -lm '
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
