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

"""Module containing Intel MKL installation and cleanup functions."""

from perfkitbenchmarker.linux_packages import INSTALL_DIR

MKL_DIR = '%s/MKL' % INSTALL_DIR
MKL_TAG = 'l_mkl_2018.2.199'
MKL_TGZ = 'l_mkl_2018.2.199.tgz'
MKL_VERSION = '2018.2.199'

# TODO(user): InstallPreprovisionedBenchmarkData currently assumes that
# BENCHMARK_NAME is associated with a benchmark. Once it is expanded to include
# packages, we can associate the preprovisioned data for MKL with this package.
BENCHMARK_NAME = 'hpcc'


def _Install(vm):
  """Installs the MKL package on the VM."""
  vm.RemoteCommand('cd {0} && mkdir MKL'.format(INSTALL_DIR))
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
  vm.RemoteCommand('cd /opt/intel/mkl/interfaces/fftw2xc && '
                   'sudo make libintel64 PRECISION=MKL_DOUBLE interface='
                   'ilp64 mpi=openmpi compiler=gnu')
  vm.RemoteCommand('cd /opt/intel/mkl/interfaces/fftw2xf && '
                   'sudo make libintel64 PRECISION=MKL_DOUBLE interface='
                   'ilp64 mpi=openmpi compiler=gnu')
  vm.RemoteCommand('cd /opt/intel/mkl/interfaces/fftw3xc && '
                   'sudo make libintel64 PRECISION=MKL_DOUBLE interface='
                   'ilp64 mpi=openmpi compiler=gnu')
  vm.RemoteCommand('cd /opt/intel/mkl/interfaces/fftw3xf && '
                   'sudo make libintel64 PRECISION=MKL_DOUBLE interface='
                   'ilp64 mpi=openmpi compiler=gnu')
  vm.RemoteCommand(
      'sudo ln -s /opt/intel/compilers_and_libraries_2018.2.199/linux/compiler/'
      'lib/intel64/libiomp5.so /lib/libiomp5.so')


def YumInstall(vm):
  """Installs the MKL package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the MKL package on the VM."""
  _Install(vm)
