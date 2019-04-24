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
"""Module containing MXNet installation and cleanup functions."""
import posixpath
from perfkitbenchmarker import flags

flags.DEFINE_string('mx_version', '1.4.0', 'mxnet pip package version')
FLAGS = flags.FLAGS


def GetEnvironmentVars(vm):
  """Return a string containing MXNet-related environment variables.

  Args:
    vm: vm to get environment varibles

  Returns:
    string of environment variables
  """
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


def GetMXNetVersion(vm):
  """Returns the version of MXNet installed on the vm.

  Args:
    vm: the target vm on which to check the MXNet version

  Returns:
    installed python MXNet version as a string
  """
  stdout, _ = vm.RemoteCommand(
      ('echo -e "import mxnet\nprint(mxnet.__version__)" | {0} python'
       .format(GetEnvironmentVars(vm)))
  )
  return stdout.strip()


def Install(vm):
  """Installs MXNet on the VM."""
  vm.Install('pip')
  vm.InstallPackages('libatlas-base-dev')
  if FLAGS.mx_device == 'gpu':
    vm.Install('cuda_toolkit')
    if FLAGS.cuda_toolkit_version == '8.0':
      vm.RemoteCommand('sudo pip install mxnet-cu80=={}'.format(
          FLAGS.mx_version), should_log=True)
    elif FLAGS.cuda_toolkit_version == '9.0':
      vm.RemoteCommand('sudo pip install mxnet-cu90=={}'.format(
          FLAGS.mx_version), should_log=True)
  elif FLAGS.mx_device == 'cpu':
    vm.RemoteCommand('sudo pip install mxnet=={}'.format(
        FLAGS.mx_version), should_log=True)


def Uninstall(vm):
  """Uninstalls MXNet on the VM."""
  vm.RemoteCommand('sudo pip uninstall mxnet', should_log=True)
