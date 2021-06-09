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


"""Module containing PyTorch installation and cleanup functions.

See https://pytorch.org/ for details
"""
from absl import flags
from perfkitbenchmarker.linux_packages import nvidia_driver


FLAGS = flags.FLAGS
flags.DEFINE_string('torch_version', '1.7.1', 'The torch version.')
flags.DEFINE_string('torchvision_version', '0.8.2', 'The torchvision version.')
flags.DEFINE_string('torchaudio_version', '0.7.2', 'The torchaudio version.')
flags.DEFINE_string('torch_env', 'PATH=/opt/conda/bin:$PATH',
                    'The torch install environment.')

_PYTORCH_WHL = 'https://download.pytorch.org/whl/torch_stable.html'


def Install(vm):
  """Installs PyTorch on the VM."""
  vm.Install('pip3')
  toolkit = 'cpu'
  if nvidia_driver.CheckNvidiaGpuExists(vm):
    # Translates --cuda_toolkit_version=10.2 to "cu102" for the toolkit to
    # install
    toolkit = f'cu{"".join(FLAGS.cuda_toolkit_version.split("."))}'
  vm.RemoteCommand(
      f'{FLAGS.torch_env} python3 -m pip install '
      f'torch=={FLAGS.torch_version}+{toolkit} '
      f'torchvision=={FLAGS.torchvision_version}+{toolkit} '
      f'torchaudio=={FLAGS.torchaudio_version} '
      f'-f {_PYTORCH_WHL}')


def Uninstall(vm):
  """Uninstalls TensorFlow on the VM."""
  vm.RemoteCommand(f'{FLAGS.torch_env} pip uninstall '
                   'torch torchvision torchaudio')
