# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Package for installing the Azure CLI."""

# installation instructions:
# https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest


def Install(vm):
  """Installs the azure-cli package on the VM."""
  vm.Install('python')
  vm.RemoteCommand(
      'echo "deb [arch=amd64] '
      'https://packages.microsoft.com/repos/azure-cli/ '
      'wheezy main" | '
      'sudo tee /etc/apt/sources.list.d/azure-cli.list')
  vm.RemoteCommand('sudo apt-key adv --keyserver packages.microsoft.com '
                   '--recv-keys 52E16F86FEE04B979B07E28DB02C46DF417A0893')
  vm.RemoteCommand('sudo apt-get install -y apt-transport-https')
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y azure-cli')
