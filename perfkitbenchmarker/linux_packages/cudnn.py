# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing CUDA Deep Neural Network library installation functions."""

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import cuda_toolkit

CUDNN_7_4_9 = 'libcudnn7=7.4.2.24-1+cuda9.0'
CUDNN_7_4_10 = 'libcudnn7=7.4.2.24-1+cuda10.0'
CUDNN_7_6_1 = 'libcudnn7=7.6.1.34-1+cuda10.1'

FLAGS = flags.FLAGS


def AptInstall(vm):
  """Installs the cudnn package on the VM."""
  if not cuda_toolkit.CheckNvidiaSmiExists(vm):
    raise Exception('CUDA Toolkit is a prerequisite for installing CUDNN.')
  if FLAGS.cuda_toolkit_version == '9.0':
    cudnn_version = CUDNN_7_4_9
  elif FLAGS.cuda_toolkit_version == '10.0':
    cudnn_version = CUDNN_7_4_10
  elif FLAGS.cuda_toolkit_version == '10.1':
    cudnn_version = CUDNN_7_6_1
  else:
    raise Exception('No CUDNN version found for given CUDA version.')
  vm.RemoteCommand(
      'sudo bash -c \'echo "deb https://developer.download.nvidia.com/compute/'
      'machine-learning/repos/ubuntu1604/x86_64 /" > /etc/apt/sources.list.d/'
      'nvidia-ml.list\'', should_log=True)
  vm.RemoteCommand('sudo apt-get update', should_log=True)
  vm.RemoteCommand('sudo apt-get install -y --no-install-recommends '
                   '{}'.format(cudnn_version), should_log=True)
