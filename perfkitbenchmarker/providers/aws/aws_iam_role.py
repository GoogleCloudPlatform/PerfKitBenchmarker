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
"""Module containing class for AWS' dynamodb tables.

Tables can be created and deleted.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import time

from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

# https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
_POLICY_VERSION = '2012-10-17'

_ROLE_ARN_TEMPLATE = 'arn:aws:iam::{account}:role/{role_name}'
_POLICY_ARN_TEMPLATE = 'arn:aws:iam::{account}:policy/{policy_name}'

_TRUST_RELATIONSHIP_FILE = 'service-trust-relationship.json'
_ROLE_POLICY_FILE = 'service-role-policy.json'
_ROLE_CREATION_DELAY = 30

_TRUST_RELATIONSHIP_TEMPLATE = """{{
    "Version": "{version}",
    "Statement": [
       {{
            "Effect": "Allow",
            "Principal": {{
                "Service": "{service}"
            }},
            "Action": "sts:AssumeRole"
        }}
    ]
}}"""

_ROLE_POLICY_TEMPLATE = """{{
    "Version": "{version}",
    "Statement": [
        {{
            "Action": [
                "{action}"
            ],
            "Effect": "Allow",
            "Resource": [
                "{resource_arn}"
            ]
        }}
    ]
}}"""


class AwsIamRole(resource.BaseResource):
  """Class representing an AWS IAM role."""

  def __init__(self,
               account,
               role_name,
               policy_name,
               service,
               action,
               resource_arn,
               policy_version=None):
    super(AwsIamRole, self).__init__()
    self.account = account
    self.role_name = role_name
    self.policy_name = policy_name
    self.service = service
    self.action = action
    self.resource_arn = resource_arn
    self.policy_version = policy_version or _POLICY_VERSION
    self.role_arn = _ROLE_ARN_TEMPLATE.format(
        account=self.account, role_name=self.role_name)
    self.policy_arn = _POLICY_ARN_TEMPLATE.format(
        account=self.account, policy_name=self.policy_name)

  def _Create(self):
    """See base class."""
    if not self._RoleExists():
      with open(_TRUST_RELATIONSHIP_FILE, 'w+') as relationship_file:
        relationship_file.write(
            _TRUST_RELATIONSHIP_TEMPLATE.format(
                version=self.policy_version, service=self.service))

      cmd = util.AWS_PREFIX + [
          'iam', 'create-role', '--role-name', self.role_name,
          '--assume-role-policy-document',
          'file://{}'.format(_TRUST_RELATIONSHIP_FILE)
      ]

      _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
      if retcode != 0:
        logging.warn('Failed to create role! %s', stderror)

    if not self._PolicyExists():
      with open(_ROLE_POLICY_FILE, 'w+') as policy_file:
        policy_file.write(
            _ROLE_POLICY_TEMPLATE.format(
                version=self.policy_version,
                action=self.action,
                resource_arn=self.resource_arn))
      cmd = util.AWS_PREFIX + [
          'iam', 'create-policy', '--policy-name', 'PolicyFor' + self.role_name,
          '--policy-document', 'file://{}'.format(_ROLE_POLICY_FILE)
      ]

      _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
      if retcode != 0:
        logging.warn('Failed to create policy! %s', stderror)

    cmd = util.AWS_PREFIX + [
        'iam', 'attach-role-policy', '--role-name', self.role_name,
        '--policy-arn', self.policy_arn
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=True)
    if retcode != 0:
      logging.warn('Failed to attach role policy! %s', stderror)

    # Make sure the role is available for the downstream users (e.g., DAX).
    # Without this, the step of creating DAX cluster may fail.
    # TODO(b/144769073): use a more robust way to handle this.
    time.sleep(_ROLE_CREATION_DELAY)

  def _Delete(self):
    """See base class."""
    cmd = util.AWS_PREFIX + [
        'iam', 'detach-role-policy', '--role-name', self.role_name,
        '--policy-arn', self.policy_arn
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to delete role policy! %s', stderror)

    cmd = util.AWS_PREFIX + [
        'iam', 'delete-policy', '--policy-arn', self.policy_arn
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to delete policy! %s', stderror)

    cmd = util.AWS_PREFIX + [
        'iam', 'delete-role', '--role-name', self.role_name
    ]

    _, stderror, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      logging.warn('Failed to delete role! %s', stderror)

  def GetRoleArn(self):
    """Returns the role's Amazon Resource Name (ARN)."""
    return self.role_arn

  def _RoleExists(self):
    """Returns true if the IAM role exists."""
    cmd = util.AWS_PREFIX + ['iam', 'get-role', '--role-name', self.role_name]
    stdout, _, retcode = vm_util.IssueCommand(
        cmd, suppress_warning=True, raise_on_failure=False)
    return retcode == 0 and stdout and json.loads(stdout)['Role']

  def _PolicyExists(self):
    """Returns true if the IAM policy used by the role exists."""
    cmd = util.AWS_PREFIX + [
        'iam', 'get-policy', '--policy-arn', self.policy_arn
    ]
    stdout, _, retcode = vm_util.IssueCommand(
        cmd, suppress_warning=True, raise_on_failure=False)
    return retcode == 0 and stdout and json.loads(stdout)['Policy']
