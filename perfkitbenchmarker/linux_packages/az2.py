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

"""Contains az 2.0 installation functions."""


def _Install(vm):
  """Installs az2.0 on a VM.

  Args:
    vm: BaseVirtualMachine. The VM on which to install boto3.
  """
  vm.RemoteCommand(
      'echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ '
      'wheezy main" | sudo tee /etc/apt/sources.list.d/azure-cli.list')
  vm.RemoteCommand(
      'sudo apt-key adv --keyserver packages.microsoft.com '
      '--recv-keys 417A0893')
  vm.RemoteCommand('sudo apt-get install apt-transport-https')
  vm.RemoteCommand(
      'sudo apt-get update && '
      'sudo apt-get install azure-cli -y --allow-unauthenticated')


# TODO(user): Add install instructions for yum.
def YumInstall(_):
  pass


def AptInstall(vm):
  vm.InstallPackages('libssl-dev libffi-dev python-dev')
  _Install(vm)
