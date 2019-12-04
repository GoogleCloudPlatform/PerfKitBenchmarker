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


"""Module containing OpenMPI installation and cleanup functions."""

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS

flags.DEFINE_bool('openmpi_enable_shared', False,
                  'Whether openmpi should build shared libraries '
                  'in addition to static ones.')
flags.DEFINE_bool('openmpi_with_cuda_support', False,
                  'Compile with CUDA support')

MPI_DIR = '%s/openmpi-3.1.2' % INSTALL_DIR
MPI_TAR = 'openmpi-3.1.2.tar.gz'
MPI_URL = 'https://download.open-mpi.org/release/open-mpi/v3.1/' + MPI_TAR
MPI_VERSION = '3.1.2'
REMOVE_MPI_CMD = 'autoremove -y libopenmpi-dev openmpi-bin openmpi-common'


def _Install(vm):
  """Installs the OpenMPI package on the VM."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.RemoteCommand('wget %s -P %s' % (MPI_URL, INSTALL_DIR))
  vm.RemoteCommand('cd %s && tar xvfz %s' % (INSTALL_DIR, MPI_TAR))
  make_jobs = vm.NumCpusForBenchmark()
  shared_lib_command = ('--enable-shared' if FLAGS.openmpi_enable_shared
                        else '--disable-shared')
  if FLAGS.openmpi_with_cuda_support:
    cuda_cmd = ('--with-cuda=/usr/local/cuda-{version}/ '
                '--with-cuda-libdir=/usr/local/cuda-{version}/lib64/'.format(
                    version=FLAGS.cuda_toolkit_version))
  else:
    cuda_cmd = ''
  config_cmd = (
      './configure --enable-static {shared_lib_cmd} --prefix=/usr '
      '{cuda_cmd}'.format(shared_lib_cmd=shared_lib_command,
                          cuda_cmd=cuda_cmd))
  vm.RobustRemoteCommand(
      'cd %s && %s && make -j %s && sudo make install' %
      (MPI_DIR, config_cmd, make_jobs))


def YumInstall(vm):
  """Installs the OpenMPI package on the VM."""
  vm.RobustRemoteCommand('sudo yum {}'.format(REMOVE_MPI_CMD))
  _Install(vm)


def AptInstall(vm):
  """Installs the OpenMPI package on the VM."""
  vm.RobustRemoteCommand('sudo apt-get {}'.format(REMOVE_MPI_CMD))
  _Install(vm)


def _Uninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  vm.RemoteCommand('cd {0} && sudo make uninstall'.format(MPI_DIR))


def YumUninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  _Uninstall(vm)


def AptUninstall(vm):
  """Uninstalls the OpenMPI package on the VM."""
  _Uninstall(vm)
