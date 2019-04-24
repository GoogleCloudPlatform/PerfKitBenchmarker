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

"""
import posixpath
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import INSTALL_DIR

VM_TMP_DIR = vm_util.VM_TMP_DIR
TF_SERVING_BASE_DIRECTORY = posixpath.join(INSTALL_DIR, 'serving')

FLAGS = flags.FLAGS

# Versions supported including TF Serving 1.11.0 and above
flags.DEFINE_string('tf_serving_branch', 'master', 'GitHub branch to pull from')


def InstallTensorFlowServingAPI(vm):
  """Installs TF Serving API on the vm.

  Currently this is only useful so that the clients can run python
  scripts that import tensorflow_serving. The server vms make no use
  of it.

  Args:
    vm: VM to operate on.
  """

  pip_package_output_dir = posixpath.join(VM_TMP_DIR, 'tf_serving_pip_package')
  pip_package = posixpath.join(pip_package_output_dir,
                               'tensorflow_serving_api*.whl')

  vm.Install('pip')

  # Build the pip package from the same source as the serving binary
  vm.RemoteCommand('sudo docker run --rm -v {0}:{0} '
                   'benchmarks/tensorflow-serving-devel '
                   'bash -c "bazel build --config=nativeopt '
                   'tensorflow_serving/tools/pip_package:build_pip_package && '
                   'bazel-bin/tensorflow_serving/tools/pip_package/'
                   'build_pip_package {0}"'.format(pip_package_output_dir))

  vm.RemoteCommand('sudo pip install {0}'.format(pip_package))


def BuildDockerImages(vm):
  """Builds the Docker images from source Dockerfiles for a pre-built env."""

  vm.InstallPackages('git')
  vm.RemoteHostCommand('cd {0} && git clone -b {1} '
                       'https://github.com/tensorflow/serving'.format(
                           INSTALL_DIR, FLAGS.tf_serving_branch))

  # Build an optimized binary for TF Serving, and keep all the build artifacts
  vm.RemoteHostCommand(
      'sudo docker build --target binary_build '
      '-t benchmarks/tensorflow-serving-devel '
      '-f {0}/tensorflow_serving/tools/docker/Dockerfile.devel '
      '{0}/tensorflow_serving/tools/docker/'.format(TF_SERVING_BASE_DIRECTORY))

  # Create a serving image with the optimized model_server binary
  vm.RemoteHostCommand(
      'sudo docker build '
      '-t benchmarks/tensorflow-serving '
      '--build-arg '
      'TF_SERVING_BUILD_IMAGE=benchmarks/tensorflow-serving-devel '
      '-f {0}/tensorflow_serving/tools/docker/Dockerfile '
      '{0}/tensorflow_serving/tools/docker/'.format(TF_SERVING_BASE_DIRECTORY))


def InstallFromDocker(vm):
  """Installs Docker and TF Serving."""

  vm.Install('docker')
  BuildDockerImages(vm)


def AptInstall(vm):
  """Installs TensorFlow Serving on the VM."""

  InstallFromDocker(vm)
  InstallTensorFlowServingAPI(vm)


def Uninstall(vm):
  """Uninstalls TensorFlow Serving on the VM."""

  vm.RemoteCommand(
      'sudo pip uninstall -y tensorflow_serving_api', should_log=True)
  vm.RemoteHostCommand(
      'sudo docker rmi benchmarks/tensorflow-serving', should_log=True)
  vm.RemoteHostCommand(
      'sudo docker rmi benchmarks/tensorflow-serving-devel', should_log=True)
  del vm
