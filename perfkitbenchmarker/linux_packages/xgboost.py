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


"""Module containing XGBoost installation and cleanup functions."""
import posixpath
from absl import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker.linux_packages import nvidia_driver

_ENV = flags.DEFINE_string('xgboost_env', 'PATH=/opt/conda/bin:$PATH',
                           'The xboost install environment.')
_VERSION = flags.DEFINE_string('xgboost_version', '1.4.2',
                               'The XGBoost version.')
FLAGS = flags.FLAGS


def GetXgboostVersion(vm):
  """Returns the XGBoost version installed on the vm.

  Args:
    vm: the target vm on which to check the XGBoost version

  Returns:
    Installed python XGBoost version as a string
  """
  stdout, _ = vm.RemoteCommand(
      'echo -e "import xgboost\nprint(xgboost.__version__)" | '
      f'{_ENV.value} python3'
  )
  return stdout.strip()


def Install(vm):
  """Installs XGBoost on the VM."""
  vm.Install('build_tools')
  install_dir = posixpath.join(linux_packages.INSTALL_DIR, 'xgboost')
  vm.RemoteCommand('git clone --recursive https://github.com/dmlc/xgboost '
                   f'--branch v{_VERSION.value} {install_dir}')
  nccl_make_option = ''
  nccl_install_option = ''
  if nvidia_driver.QueryNumberOfGpus(vm) > 1:
    nccl_make_option = '-DUSE_NCCL=ON -DNCCL_ROOT=/usr/local/nccl2'
    nccl_install_option = '--use-nccl'
  cuda_env = ''
  cuda_make_option = ''
  cuda_install_option = ''
  if nvidia_driver.CheckNvidiaGpuExists:
    cuda_make_option = '-DUSE_CUDA=ON'
    cuda_env = 'CUDACXX=/usr/local/cuda/bin/nvcc'
    cuda_install_option = '--use-cuda'
  build_dir = posixpath.join(install_dir, 'build')
  package_dir = posixpath.join(install_dir, 'python-package')
  vm.RemoteCommand(f'mkdir -p {build_dir}')
  vm.RemoteCommand(f'cd {build_dir} && '
                   f'{cuda_env} cmake .. {cuda_make_option} {nccl_make_option}')
  vm.RemoteCommand(f'cd {build_dir} && make -j4')
  vm.RemoteCommand(f'cd {package_dir} && '
                   f'{_ENV.value} python3 setup.py install '
                   f'{cuda_install_option} {nccl_install_option}')
