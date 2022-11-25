# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing Neuron installation."""

import posixpath
from absl import flags
from perfkitbenchmarker import virtual_machine

ENV = flags.DEFINE_string(
    'neuron_env', 'aws_neuron_venv_pytorch',
    'The Python virtual environment to install Neuron pip package.')
FLAGS = flags.FLAGS


def YumInstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Install Neuron Drivers and tools.

  Args:
    vm: The virtual machine to install Neuron drivers and tools.
  """
  # Configure Linux for Neuron repository updates
  vm.RemoteCommand("""sudo tee /etc/yum.repos.d/neuron.repo > /dev/null <<EOF
[neuron]
name=Neuron YUM Repository
baseurl=https://yum.repos.neuron.amazonaws.com
enabled=1
metadata_expire=0
EOF""")
  vm.RemoteCommand(
      'sudo rpm --import https://yum.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB'
  )

  # Update OS packages
  vm.RemoteCommand('sudo yum update -y')

  # Install git
  vm.InstallPackages('git')

  # Install OS headers
  vm.InstallPackages('kernel-devel-$(uname -r) kernel-headers-$(uname -r)')

  # Remove preinstalled packages and Install Neuron Driver and Runtime
  vm.RemoteCommand('sudo yum remove aws-neuron-dkms -y')
  vm.RemoteCommand('sudo yum remove aws-neuronx-dkms -y')
  vm.RemoteCommand('sudo yum remove aws-neuronx-oci-hook -y')
  vm.RemoteCommand('sudo yum remove aws-neuronx-runtime-lib -y')
  vm.RemoteCommand('sudo yum remove aws-neuronx-collectives -y')
  vm.InstallPackages('aws-neuronx-dkms-2.*')
  vm.InstallPackages('aws-neuronx-oci-hook-2.*')
  vm.InstallPackages('aws-neuronx-runtime-lib-2.*')
  vm.InstallPackages('aws-neuronx-collectives-2.*')

  # Remove pre-installed package and Install Neuron Tools
  vm.RemoteCommand('sudo yum remove aws-neuron-tools  -y')
  vm.RemoteCommand('sudo yum remove aws-neuronx-tools  -y')
  vm.InstallPackages('aws-neuronx-tools-2.*')

  vm.RemoteCommand('export PATH=/opt/aws/neuron/bin:$PATH')

  # Install Python venv and activate Python virtual environment to install
  # Neuron pip packages.
  vm.RemoteCommand(f'python3.7 -m venv {ENV.value}')

  path = f'PATH={posixpath.join(ENV.value, "bin")}:$PATH'
  # source aws_neuron_venv_pytorch/bin/activate
  vm.RemoteCommand(f'{path} pip3 install -U pip')

  # Install packages from repos
  vm.RemoteCommand(
      f'{path} pip3 config set global.extra-index-url "https://pip.repos.neuron.amazonaws.com"'
  )

  # Install Neuron packages
  vm.RemoteCommand(f'{path} pip3 install torch-neuronx==1.11.0.1.*')
  vm.RemoteCommand(f'{path} pip3 install neuronx-cc==2.*')
