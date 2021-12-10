# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Package for installing the AWS CLI."""

AWSCLI_URL = 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip'
AWSCLI_ZIP = 'awscliv2.zip'


def Install(vm):
  """Installs the awscli package on the VM."""
  vm.InstallPackages('unzip')
  vm.RemoteCommand(f'curl {AWSCLI_URL} -o {AWSCLI_ZIP} && unzip {AWSCLI_ZIP}')
  vm.RemoteCommand('sudo ./aws/install')
  # Clean up unused files
  vm.RemoteCommand(f'rm -rf aws {AWSCLI_ZIP}')


def Uninstall(vm):
  vm.RemoteCommand('sudo rm -rf /usr/local/aws-cli')
  vm.RemoteCommand('sudo rm /usr/local/bin/aws')
