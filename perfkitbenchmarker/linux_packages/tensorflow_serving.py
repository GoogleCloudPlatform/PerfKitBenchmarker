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
"""Module containing TensorFlow Serving installation functions.

  At the moment some of TensorFlow Serving is broken on the master branch
  (https://github.com/tensorflow/serving/issues/684), so this module builds
  the r1.4 from source.
"""
import posixpath
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FLAGS = flags.FLAGS
VM_TMP_DIR = vm_util.VM_TMP_DIR

BAZEL_INSTALLER_URL = 'https://github.com/bazelbuild/bazel/releases/download/0.6.0/bazel-0.6.0-installer-linux-x86_64.sh'
TF_SERVING_BIN_DIRECTORY = posixpath.join(
    INSTALL_DIR, 'serving/bazel-bin/tensorflow_serving')
TF_SERVING_BASE_DIRECTORY = posixpath.join(INSTALL_DIR, 'serving')


def InstallBazel(vm):
  """Installs Bazel 0.6.0."""
  # TODO(ferneyhough): Consider moving this to its own package.
  basename = posixpath.basename(BAZEL_INSTALLER_URL)
  vm.RemoteCommand('cd {0} && wget {1}'.format(VM_TMP_DIR, BAZEL_INSTALLER_URL))
  vm.RemoteCommand('cd {0} && chmod +x {1}'.format(VM_TMP_DIR, basename))
  vm.RemoteCommand('cd {0} && sudo ./{1}'.format(
      VM_TMP_DIR, basename))  # This installs to /usr/local/bin


def BuildAndInstallPipPackage(vm):
  """Builds a TensorFlowServing pip package and install it on the vm.

  Currently this is only useful so that the clients can run python
  scripts that import tensorflow_serving. The server vms make no use
  of it.

  Args:
    vm: VM to operate on.
  """
  build_pip_package_tool = posixpath.join(
      TF_SERVING_BIN_DIRECTORY, 'tools/pip_package/build_pip_package')
  pip_package_output_dir = posixpath.join(VM_TMP_DIR, 'tf_serving_pip_package')
  pip_package = posixpath.join(pip_package_output_dir,
                               'tensorflow_serving_api-1.4.0-py2-none-any.whl')

  # The following command must be run from root of the tf serving build tree.
  vm.RemoteCommand('cd {0} && {1} {2}'.format(TF_SERVING_BASE_DIRECTORY,
                                              build_pip_package_tool,
                                              pip_package_output_dir))
  vm.RemoteCommand('sudo pip install {0}'.format(pip_package))


def BuildTfServing(vm):
  """Builds the Tensorflow Serving r1.4 from source."""
  vm.RemoteCommand('cd {0} && git clone -b r1.4 --recurse-submodules '
                   'https://github.com/tensorflow/serving'.format(INSTALL_DIR))
  # Run the configure script using default for all prompts.
  vm.RemoteCommand('cd {0} && yes "" | ./configure'.format(
      posixpath.join(TF_SERVING_BASE_DIRECTORY, 'tensorflow')))

  # Note: This took 45 minutes to build on an n1-standard-8 but is much
  # faster on an n1-standard-64.
  vm.RemoteCommand(
      'cd {0} && /usr/local/bin/bazel '
      'build -c opt tensorflow_serving/...'.format(TF_SERVING_BASE_DIRECTORY))

  BuildAndInstallPipPackage(vm)


def InstallFromSource(vm):
  """Downloads and builds Tensorflow Serving."""
  vm.InstallPackages('build-essential curl libcurl3-dev libfreetype6-dev '
                     'libpng12-dev libzmq3-dev pkg-config python-dev '
                     'python-numpy software-properties-common swig '
                     'zip zlib1g-dev')
  vm.RemoteCommand('sudo pip install grpcio')
  InstallBazel(vm)
  BuildTfServing(vm)


def AptInstall(vm):
  """Installs TensorFlowServing on the VM."""
  vm.Install('pip')
  vm.InstallPackages('python-numpy')
  InstallFromSource(vm)


def Uninstall(vm):
  """Uninstalls TensorFlow Serving on the VM."""
  del vm
