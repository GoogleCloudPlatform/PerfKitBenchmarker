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


"""Module containing NCCL installation function."""

from perfkitbenchmarker import flags

flags.DEFINE_string('nccl_version', '2.5.6-1',
                    'NCCL version to install')

FLAGS = flags.FLAGS


def _Build(vm):
  """Installs the OpenMPI package on the VM."""
  vm.RemoteCommand('git clone https://github.com/NVIDIA/nccl.git --branch '
                   'v{}'.format(FLAGS.nccl_version))
  vm.RemoteCommand('cd nccl && make -j src.build')


def AptInstall(vm):
  """Installs the NCCL package on the VM."""
  _Build(vm)
  vm.InstallPackages('build-essential devscripts debhelper fakeroot')
  vm.RemoteCommand('cd nccl && make pkg.debian.build')
  vm.InstallPackages('{build}libnccl2_{nccl}+cuda{cuda}_amd64.deb '
                     '{build}libnccl-dev_{nccl}+cuda{cuda}_amd64.deb'.format(
                         build='./nccl/build/pkg/deb/', nccl=FLAGS.nccl_version,
                         cuda=FLAGS.cuda_toolkit_version))
