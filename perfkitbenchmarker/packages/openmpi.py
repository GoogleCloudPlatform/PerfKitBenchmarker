# Copyright 2014 Google Inc. All rights reserved.
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


"""Module containing iperf installation and cleanup functions."""

MPI_DIR = 'pkb/openmpi-1.6.5'
MPI_TAR = 'openmpi-1.6.5.tar.gz'
MPI_URL = 'http://www.open-mpi.org/software/ompi/v1.6/downloads/' + MPI_TAR


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('wget %s -P pkb' % MPI_URL)
  vm.RemoteCommand('cd pkb && tar xvfz %s' % MPI_TAR)
  make_jobs = vm.num_cpus
  config_cmd = ('./configure --enable-static --disable-shared --disable-dlopen '
                '--prefix=/usr')
  vm.RemoteCommand('cd %s && %s && make -j %s && sudo make install' %
                   (MPI_DIR, config_cmd, make_jobs))


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
