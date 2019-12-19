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
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

flags.DEFINE_string('nccl_version', '2.5.6-1',
                    'NCCL version to install')

FLAGS = flags.FLAGS

GIT_REPO = 'https://github.com/NVIDIA/nccl.git'


def _Build(vm):
  """Installs the OpenMPI package on the VM."""
  vm.RemoteCommand('[ -d "nccl" ] || git clone {git_repo} --branch v{version}'
                   .format(git_repo=GIT_REPO, version=FLAGS.nccl_version))
  cuda_home = '/usr/local/cuda'
  vm.InstallPackages('build-essential devscripts debhelper fakeroot')

  env_vars = {}
  env_vars['PATH'] = (r'{cuda_bin_path}:$PATH'
                      .format(cuda_bin_path=posixpath.join(cuda_home, 'bin')))
  env_vars['CUDA_HOME'] = (r'{cuda_home}'.format(cuda_home=cuda_home))
  env_vars['LD_LIBRARY_PATH'] = (r'{lib_path}:$LD_LIBRARY_PATH'
                                 .format(lib_path=posixpath.join(
                                     cuda_home, 'lib64')))

  vm.RemoteCommand('cd nccl && {env} make -j 20 pkg.debian.build'
                   .format(env=vm_util.DictonaryToEnvString(env_vars)))


def AptInstall(vm):
  """Installs the NCCL package on the VM."""
  _Build(vm)

  vm.InstallPackages('{build}libnccl2_{nccl}+cuda{cuda}_amd64.deb '
                     '{build}libnccl-dev_{nccl}+cuda{cuda}_amd64.deb'
                     .format(
                         build='./nccl/build/pkg/deb/',
                         nccl=FLAGS.nccl_version,
                         cuda=FLAGS.cuda_toolkit_version))
