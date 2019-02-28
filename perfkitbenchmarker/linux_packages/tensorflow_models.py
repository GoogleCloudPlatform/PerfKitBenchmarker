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
"""Module containing TensorFlow models installation and cleanup functions."""
from perfkitbenchmarker import flags
FLAGS = flags.FLAGS
TF_MODELS_GIT = 'https://github.com/tensorflow/models.git'

flags.DEFINE_string('tensorflow_models_commit_hash',
                    '57e075203f8fba8d85e6b74f17f63d0a07da233a',
                    'git commit hash of desired TensorFlow models commit.')


def Install(vm):
  """Installs TensorFlow models on the VM."""
  vm.InstallPackages('git')
  vm.RemoteCommand('git clone {}'.format(TF_MODELS_GIT), should_log=True)
  vm.RemoteCommand('cd models && git checkout {}'.format(
      FLAGS.tensorflow_models_commit_hash))


def Uninstall(vm):
  """Uninstalls TensorFlow models on the VM."""
  vm.RemoteCommand('rm -rf models', should_log=True)
