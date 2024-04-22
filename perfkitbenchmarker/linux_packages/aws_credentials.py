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

"""Module for extracting AWS credentials from a the local configuration."""

import logging
import os
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import util


FLAGS = flags.FLAGS


def _GetLocalPath():
  """Gets the expanded local path of the credential files.

  Returns:
    string. Path to the credential files on the local machine.
  """
  return os.path.expanduser(FLAGS.aws_credentials_local_path)


# TODO(pclay): Move to AWS provider since this is no longer an installable
# package.
def GetCredentials(profile: str):
  """Gets the credentials from the AWS CLI config.

  Prefer --aws_ec2_instance_profile when using EC2 VMs.

  Args:
    profile: name of the profile to get the credentials from.

  Returns:
    A string, string tuple of access_key and secret_access_key
  """
  key_id = util.GetConfigValue('aws_access_key_id', profile=profile)
  secret = util.GetConfigValue(
      'aws_secret_access_key', profile=profile, suppress_logging=True
  )
  # TODO(pclay): consider adding some validation here:
  # 1. key and secret are in the correct account.
  # 2. profile does not use a session token.
  return key_id, secret


def Install(vm):
  """Copies credential files to the specified VM.

  Args:
    vm: BaseVirtualMachine. VM that receives the credential files.

  Raises:
    errors.Error: If the file destination on the VM already exists, and the
        overwrite behavior is not specified via --aws_credentials_overwrite.
  """
  local_path = _GetLocalPath()
  remote_path = FLAGS.aws_credentials_remote_path
  overwrite = FLAGS.aws_credentials_overwrite
  try:
    vm.RemoteCommand('[[ ! -e {0} ]]'.format(remote_path))
  except errors.VirtualMachine.RemoteCommandError:
    err_msg = 'File {0} already exists on VM {1}.'.format(remote_path, vm)
    if overwrite:
      logging.info('%s Overwriting.', err_msg)
    else:
      raise errors.Error(err_msg)
  remote_dir = os.path.dirname(remote_path)
  if remote_dir:
    vm.RemoteCommand('mkdir -p {0}'.format(remote_dir))
  vm.PushFile(local_path, remote_path)


def Uninstall(vm):
  """Deletes the credential files from the specified VM.

  Args:
    vm: BaseVirtualMachine. VM that has the credential files.
  """
  vm.RemoveFile(FLAGS.aws_credentials_remote_path)
