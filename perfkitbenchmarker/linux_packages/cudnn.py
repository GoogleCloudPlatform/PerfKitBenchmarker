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

import os
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import cuda_toolkit_8

flags.DEFINE_string('cudnn', 'cudnn-8.0-linux-x64-v5.1.tgz',
                    '''The NVIDIA CUDA Deep Neural Network library.
                    Please put in data directory and specify the name''')
FLAGS = flags.FLAGS
CUDA_TOOLKIT_INSTALL_DIR = cuda_toolkit_8.CUDA_TOOLKIT_INSTALL_DIR


def Install(vm):
  """Installs NVIDIA CUDA Deep Neural Network library."""
  src_path = data.ResourcePath(FLAGS.cudnn)
  dest_path = os.path.join('/tmp', FLAGS.cudnn)
  vm.RemoteCopy(src_path, dest_path)
  vm.RemoteCommand('tar -zxf %s' % dest_path, should_log=True)
  vm.RemoteCommand('sudo cp cuda/lib64/* %s/lib64/' % CUDA_TOOLKIT_INSTALL_DIR)
  vm.RemoteCommand('sudo cp cuda/include/cudnn.h %s/include/' %
                   CUDA_TOOLKIT_INSTALL_DIR)
