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

from perfkitbenchmarker.providers.aws import util


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
  """Deprecated means of copying local credentials into a VM."""
  # This should never be called, but in case a fork calls
  # Install('aws_credentsials into a VM.').
  raise NotImplementedError('Do not install AWS local credentials. Use '
                            '--aws_ec2_instance_profile instead.')
