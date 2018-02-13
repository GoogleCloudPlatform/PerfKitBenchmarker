# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

import posixpath
from perfkitbenchmarker import data
from perfkitbenchmarker import flags

CUDNN_6 = 'libcudnn6_6.0.21-1+cuda8.0_amd64.deb'
CUDNN_7 = 'libcudnn7_7.0.5.15-1+cuda9.0_amd64.deb'

flags.DEFINE_string('cudnn', CUDNN_7,
                    'The NVIDIA CUDA Deep Neural Network library. '
                    'Please put in data directory and specify the name')
FLAGS = flags.FLAGS


def _Install(vm, dest_path):
  vm.RemoteCommand('tar -zxf %s' % dest_path, should_log=True)
  vm.RemoteCommand('sudo cp -P cuda/include/cudnn.h %s/include/' %
                   FLAGS.cuda_toolkit_installation_dir)
  vm.RemoteCommand('sudo cp -P cuda/lib64/libcudnn* %s/lib64/' %
                   FLAGS.cuda_toolkit_installation_dir)


def _CopyLib(vm):
  # If the cudnn flag was passed on the command line,
  # use that value for the cudnn path. Otherwise, chose
  # an intelligent default given the cuda toolkit version
  # specified.
  if FLAGS['cudnn'].present:
    cudnn_path = FLAGS.cudnn
  else:
    if FLAGS.cuda_toolkit_version == '8.0':
      cudnn_path = CUDNN_6
    elif FLAGS.cuda_toolkit_version == '9.0':
      cudnn_path = CUDNN_7

  src_path = data.ResourcePath(cudnn_path)
  dest_path = posixpath.join('/tmp', cudnn_path)
  vm.RemoteCopy(src_path, dest_path)
  return dest_path


def AptInstall(vm):
  dest_path = _CopyLib(vm)
  if dest_path.endswith('.deb'):
    vm.RemoteCommand('sudo dpkg -i %s' % dest_path, should_log=True)
  else:
    _Install(vm, dest_path)


def YumInstall(vm):
  _Install(vm, _CopyLib(vm))
