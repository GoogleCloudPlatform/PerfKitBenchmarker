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
"""Module containing cloud TPU models installation and cleanup functions."""
from absl import flags
FLAGS = flags.FLAGS
CLOUD_TPU_GIT = 'https://github.com/tensorflow/tpu.git'

flags.DEFINE_string('cloud_tpu_commit_hash',
                    '45f33771f9d9aef6d37386d07b086d7e20be0254',
                    'git commit hash of desired cloud TPU models commit.')


def Install(vm):
  """Installs cloud TPU models on the VM."""
  vm.InstallPackages('git')
  vm.RemoteCommand('git clone {}'.format(CLOUD_TPU_GIT), should_log=True)
  vm.RemoteCommand('cd tpu && git checkout {}'.format(
      FLAGS.cloud_tpu_commit_hash), should_log=True)
  vm.Install('pip')
  vm.RemoteCommand('sudo pip install absl-py', should_log=True)


def Uninstall(vm):
  """Uninstalls cloud TPU models on the VM."""
  vm.RemoteCommand('rm -rf tpu', should_log=True)


def GetCommit(vm):
  stdout, _ = vm.RemoteCommand('cd tpu && git rev-parse HEAD',
                               should_log=True)
  return stdout
