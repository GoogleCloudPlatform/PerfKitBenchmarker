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

"""Package for installing the AWS CLI."""

from perfkitbenchmarker import errors

# TODO(user): Migrate to awscli v2, see
# https://docs.aws.amazon.com/cli/latest/userguide/cliv2-migration.html.
# TODO(user): Updating to v1 head will not work since awscli stopped
# support of python 3.5 the OS sometimes installs. We should find a
# way to pin this version to 3.7 which would probably involve changing the
# python 3 linux package.
_VERSION = '1.19.75'


def Install(vm):
  """Installs the awscli package on the VM."""
  version, _ = vm.RemoteCommand('aws --version', ignore_failure=True)
  for package_with_versions in version.split():
    if ('aws-cli' in package_with_versions and
        package_with_versions.split('/')[1] >= _VERSION):
      # awscli is already installed and at a later version.
      return
  vm.Install('pip3')
  vm.RemoteCommand(
      f"sudo pip3 install 'awscli=={_VERSION}' "
      # awscli 0.18 depends on a specific PyYAML version, and the AWS Ubuntu AMI
      # ships with an old python-yaml Deb package that pip3 can't upgrade so we
      # ignore it.
      # TODO(user): remove version when we update AWS-cli
      '--ignore-installed --force-reinstall')


def YumInstall(vm):
  """Installs the awscli package on the VM."""
  # amazon linux 2 has awscli pre-installed. Check to see if it exists and
  # install it if it does not.
  try:
    vm.RemoteCommand('yum list installed awscli')
  except errors.VirtualMachine.RemoteCommandError:
    Install(vm)


def Uninstall(vm):
  vm.RemoteCommand('/usr/bin/yes | sudo pip3 uninstall awscli')
