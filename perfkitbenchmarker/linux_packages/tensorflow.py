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


"""Module containing TensorFlow 1.3 installation and cleanup functions."""
from perfkitbenchmarker import flags
FLAGS = flags.FLAGS


def GetTensorFlowVersion(vm):
  """Returns the version of tensorflow installed on the vm.

  Args:
    vm: the target vm on which to check the tensorflow version

  Returns:
    installed python tensorflow version as a string
  """
  stdout, _ = vm.RemoteCommand(
      'echo -e "import tensorflow\nprint(tensorflow.__version__)" | python'
  )
  return stdout.strip()


def Install(vm):
  """Installs TensorFlow on the VM."""
  vm.Install('pip')
  if FLAGS.tf_device == 'gpu':
    vm.Install('cuda_toolkit_8')
    vm.Install('cudnn')
    vm.RemoteCommand('sudo pip install --upgrade tensorflow-gpu==1.3',
                     should_log=True)
  elif FLAGS.tf_device == 'cpu':
    vm.RemoteCommand('sudo pip install --upgrade tensorflow==1.3',
                     should_log=True)


def Uninstall(vm):
  """Uninstalls TensorFlow on the VM."""
  vm.RemoteCommand('sudo pip uninstall --upgrade tensorflow',
                   should_log=True)
