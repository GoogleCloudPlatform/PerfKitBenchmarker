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
import posixpath
from absl import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import cuda_toolkit

flags.DEFINE_string('nccl_version', '2.9.9-1',
                    'NCCL version to install. '
                    'Input "None" to bypass installation.')
flags.DEFINE_string('nccl_net_plugin', None, 'NCCL network plugin name')
flags.DEFINE_string('nccl_mpi', '/usr/bin/mpirun', 'MPI binary path')
flags.DEFINE_string('nccl_mpi_home', '/usr/lib/x86_64-linux-gnu/openmpi',
                    'MPI home')
flags.DEFINE_string('nccl_home', '$HOME/nccl/build', 'NCCL home')

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/NVIDIA/nccl.git'


def _Build(vm):
  """Installs the NCCL package on the VM."""
  vm.RemoteCommand('[ -d "nccl" ] || git clone {git_repo} --branch v{version}'
                   .format(git_repo=GIT_REPO, version=FLAGS.nccl_version))
  cuda_home = cuda_toolkit.CUDA_HOME
  vm.InstallPackages('build-essential devscripts debhelper fakeroot')

  env_vars = {}
  env_vars['PATH'] = (r'{cuda_bin_path}:$PATH'
                      .format(cuda_bin_path=posixpath.join(cuda_home, 'bin')))
  env_vars['CUDA_HOME'] = (r'{cuda_home}'.format(cuda_home=cuda_home))
  env_vars['LD_LIBRARY_PATH'] = (r'{lib_path}:$LD_LIBRARY_PATH'
                                 .format(lib_path=posixpath.join(
                                     cuda_home, 'lib64')))

  vm.RemoteCommand('cd nccl && {env} make -j 20 pkg.debian.build'
                   .format(env=vm_util.DictionaryToEnvString(env_vars)))


def AptInstall(vm):
  """Installs the NCCL package on the VM."""
  if FLAGS.nccl_version == 'None' or not FLAGS.nccl_version:
    return

  vm.Install('cuda_toolkit')
  _Build(vm)
  vm.InstallPackages('--allow-downgrades '
                     '{build}libnccl2_{nccl}+cuda{cuda}_amd64.deb '
                     '{build}libnccl-dev_{nccl}+cuda{cuda}_amd64.deb'
                     .format(
                         build='./nccl/build/pkg/deb/',
                         nccl=FLAGS.nccl_version,
                         cuda=FLAGS.cuda_toolkit_version))

  if FLAGS.nccl_net_plugin:
    vm.RemoteCommand(
        'echo "deb https://packages.cloud.google.com/apt google-fast-socket main" '
        '| sudo tee /etc/apt/sources.list.d/google-fast-socket.list'
    )
    vm.RemoteCommand(
        'curl -s -L https://packages.cloud.google.com/apt/doc/apt-key.gpg '
        '| sudo apt-key add -'
    )
    vm.AptUpdate()
    vm.InstallPackages(f'{FLAGS.nccl_net_plugin}')

  vm.RemoteCommand('sudo rm -rf /usr/local/nccl2')  # Preexisting NCCL in DLVM
  vm.RemoteCommand('sudo ldconfig')  # Refresh LD cache

  if FLAGS.aws_efa:
    vm.InstallPackages('libudev-dev libtool autoconf')
    vm.RemoteCommand('git clone https://github.com/aws/aws-ofi-nccl.git -b aws')
    vm.RemoteCommand('cd aws-ofi-nccl && ./autogen.sh && ./configure '
                     '--with-mpi={mpi} '
                     '--with-libfabric=/opt/amazon/efa '
                     '--with-nccl={nccl} '
                     '--with-cuda={cuda} && sudo make && '
                     'sudo make install'.format(
                         mpi=FLAGS.nccl_mpi_home,
                         nccl=FLAGS.nccl_home,
                         cuda='/usr/local/cuda-{}'.format(
                             FLAGS.cuda_toolkit_version)))
