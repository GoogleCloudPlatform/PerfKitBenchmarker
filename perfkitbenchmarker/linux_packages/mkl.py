# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing Intel MKL installation and cleanup functions."""

from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker.linux_packages import intel_repo


MKL_DIR = '%s/MKL' % linux_packages.INSTALL_DIR
MKL_TAG = 'l_mkl_2018.2.199'
MKL_TGZ = 'l_mkl_2018.2.199.tgz'
MKL_VERSION = '2018.2.199'
# While some of the dependencies are "-199" the intel-mkl package is "-046"
_MKL_VERSION_REPO = '2018.2-046'

# TODO(user): InstallPreprovisionedBenchmarkData currently assumes that
# BENCHMARK_NAME is associated with a benchmark. Once it is expanded to include
# packages, we can associate the preprovisioned data for MKL with this package.
BENCHMARK_NAME = 'hpcc'

# Default installs MKL as it was previously done via preprovisioned data
USE_MKL_REPO = flags.DEFINE_bool('mkl_install_from_repo', False,
                                 'Whether to install MKL from the Intel repo.')

# File contains MKL specific environment variables
_MKL_VARS_FILE = '/opt/intel/mkl/bin/mklvars.sh'

# Dedicated path to the MKL 2018 root directory
_MKL_ROOT_2018 = '/opt/intel/compilers_and_libraries_2018/linux/mkl'

# Command to source for Intel64 based VMs to set environment variables
SOURCE_MKL_INTEL64_CMD = f'MKLVARS_ARCHITECTURE=intel64 . {_MKL_VARS_FILE}'


def _Install(vm):
  """Installs the MKL package on the VM."""
  if USE_MKL_REPO.value:
    vm.InstallPackages(f'intel-mkl-{_MKL_VERSION_REPO}')
  else:
    _InstallFromPreprovisionedData(vm)
  # Restore the /opt/intel/mkl/bin/mklvars.sh symlink that is missing if
  # Intel MPI > 2018 installed.
  if not vm.TryRemoteCommand(f'test -e {_MKL_VARS_FILE}'):
    if vm.TryRemoteCommand(f'test -d {_MKL_ROOT_2018}'):
      vm.RemoteCommand(f'sudo ln -s {_MKL_ROOT_2018} /opt/intel/mkl')
  _CompileInterfaces(vm)


def _InstallFromPreprovisionedData(vm):
  """Installs the MKL package from preprovisioned data on the VM."""
  vm.RemoteCommand('cd {0} && mkdir MKL'.format(linux_packages.INSTALL_DIR))
  vm.InstallPreprovisionedBenchmarkData(
      BENCHMARK_NAME, [MKL_TGZ], MKL_DIR)
  vm.RemoteCommand('cd {0} && tar zxvf {1}'.format(MKL_DIR, MKL_TGZ))
  vm.RemoteCommand(('cd {0}/{1} && '
                    'sed -i "s/decline/accept/g" silent.cfg && '
                    'sudo ./install.sh --silent ./silent.cfg').format(
                        MKL_DIR, MKL_TAG))
  vm.RemoteCommand('sudo chmod +w /etc/bash.bashrc && '
                   'sudo chmod 777 /etc/bash.bashrc && '
                   'echo "source /opt/intel/mkl/bin/mklvars.sh intel64" '
                   '>>/etc/bash.bashrc && '
                   'echo "export PATH=/opt/intel/bin:$PATH" '
                   '>>/etc/bash.bashrc && '
                   'echo "export LD_LIBRARY_PATH=/opt/intel/lib/intel64:'
                   '/opt/intel/mkl/lib/intel64:$LD_LIBRARY_PATH" '
                   '>>/etc/bash.bashrc && '
                   'echo "source /opt/intel/compilers_and_libraries/linux/bin/'
                   'compilervars.sh -arch intel64 -platform linux" '
                   '>>/etc/bash.bashrc')


def _CompileInterfaces(vm):
  """Compiles the MKL FFT interfaces.

  Args:
    vm: Virtual Machine to compile on.
  """
  mpi_lib = 'openmpi'
  make_options = ('PRECISION=MKL_DOUBLE '
                  'interface=ilp64 '
                  f'mpi={mpi_lib} '
                  'compiler=gnu')
  for interface in ('fftw2xc', 'fftw2xf', 'fftw3xc', 'fftw3xf'):
    cmd = (f'cd /opt/intel/mkl/interfaces/{interface} && '
           f'sudo make libintel64 {make_options}')
    vm.RemoteCommand(cmd)


def YumInstall(vm):
  """Installs the MKL package on the VM."""
  if USE_MKL_REPO.value:
    intel_repo.YumPrepare(vm)
  _Install(vm)


def AptInstall(vm):
  """Installs the MKL package on the VM."""
  if USE_MKL_REPO.value:
    intel_repo.AptPrepare(vm)
  _Install(vm)
