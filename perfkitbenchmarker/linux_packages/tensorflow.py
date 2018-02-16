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


"""Module containing TensorFlow installation and cleanup functions."""
import posixpath
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import cuda_toolkit


FLAGS = flags.FLAGS
flags.DEFINE_string('tf_cpu_pip_package',
                    'https://anaconda.org/intel/tensorflow/1.4.0/download/'
                    'tensorflow-1.4.0-cp27-cp27mu-linux_x86_64.whl',
                    'TensorFlow CPU pip package to install. By default, PKB '
                    'will install an Intel-optimized CPU build when using CPUs.'
                   )
flags.DEFINE_string('tf_gpu_pip_package', 'tensorflow-gpu==1.3',
                    'TensorFlow GPU pip package to install. By default, PKB '
                    'will install tensorflow-gpu==1.3 when using GPUs.')


def GetEnvironmentVars(vm):
  """Return a string containing TensorFlow-related environment variables.

  Args:
    vm: vm to get environment varibles

  Returns:
    string of environment variables
  """
  if not cuda_toolkit.CheckNvidiaGpuExists(vm):
    return ''
  output, _ = vm.RemoteCommand('getconf LONG_BIT', should_log=True)
  long_bit = output.strip()
  lib_name = 'lib' if long_bit == '32' else 'lib64'
  return ' '.join([
      'PATH=%s${PATH:+:${PATH}}' %
      posixpath.join(FLAGS.cuda_toolkit_installation_dir, 'bin'),
      'CUDA_HOME=%s' % FLAGS.cuda_toolkit_installation_dir,
      'LD_LIBRARY_PATH=%s${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}' %
      posixpath.join(FLAGS.cuda_toolkit_installation_dir, lib_name),
  ])


def GetTensorFlowVersion(vm):
  """Returns the version of tensorflow installed on the vm.

  Args:
    vm: the target vm on which to check the tensorflow version

  Returns:
    installed python tensorflow version as a string
  """
  stdout, _ = vm.RemoteCommand(
      ('echo -e "import tensorflow\nprint(tensorflow.__version__)" | {0} python'
       .format(GetEnvironmentVars(vm)))
  )
  return stdout.strip()


def Install(vm):
  """Installs TensorFlow on the VM."""
  has_gpu = cuda_toolkit.CheckNvidiaGpuExists(vm)
  tf_pip_package = (FLAGS.tf_gpu_pip_package if has_gpu else
                    FLAGS.tf_cpu_pip_package)

  if has_gpu:
    vm.Install('cuda_toolkit')
    vm.Install('cudnn')

  vm.Install('pip')
  vm.RemoteCommand('sudo pip install --upgrade %s' % tf_pip_package,
                   should_log=True)


def Uninstall(vm):
  """Uninstalls TensorFlow on the VM."""
  vm.RemoteCommand('sudo pip uninstall tensorflow',
                   should_log=True)
