# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing google cloud storage installation function."""

from absl import flags

FLAGS = flags.FLAGS


def Install(vm):
  """Installs google cloud sdk on the VM."""
  vm.Install('pip')
  if FLAGS.gcs_client == 'python_grpc':
    # google-cloud-storage[grpc] is new and depends on the latest grpcio.
    # grpcio depends on specific versions of typing-extensions, which conflicts
    # with the version in Ubuntu used by cloud-init.
    # The grpc clients are in base google-cloud-storage, but lack the following
    # dependencies.
    # TODO(pclay): Move to a venv to avoid conflicts with base Ubuntu packages.
    if vm.TryRemoteCommand('dpkg -s python3-typing-extensions'):
      vm.RemoteCommand('sudo apt remove -y python3-typing-extensions')
    vm.RemoteCommand(
        'sudo pip install google-cloud-storage[grpc]'
    )
  else:
    vm.RemoteCommand('sudo pip install google-cloud-storage')
