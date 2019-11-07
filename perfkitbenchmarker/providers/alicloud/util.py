# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Utilities for working with AliCloud Web Services resources."""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import shlex

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
import six

ALI_PREFIX = ['aliyun']
ROOT = 'root'
FLAGS = flags.FLAGS
PASSWD_LEN = 20


REGION_HZ = 'cn-hangzhou'


ADD_USER_TEMPLATE = """#!/bin/bash
echo "{user_name} ALL = NOPASSWD: ALL" >> /etc/sudoers
useradd {user_name} --home /home/{user_name} --shell /bin/bash -m
mkdir /home/{user_name}/.ssh
echo "{public_key}" >> /home/{user_name}/.ssh/authorized_keys
chown -R {user_name}:{user_name} /home/{user_name}/.ssh
chmod 700 /home/{user_name}/.ssh
chmod 600 /home/{user_name}/.ssh/authorized_keys
"""


def GetEncodedCmd(cmd):
  cmd_line = ' '.join(cmd)
  cmd_args = shlex.split(cmd_line)
  return cmd_args


def GetRegionByZone(zone):
  if zone.find(REGION_HZ) != -1:
    return REGION_HZ
  s = zone.split('-')
  if s[0] == 'cn':
    s.pop()
    return '-'.join(s)
  else:
    return zone[:-1]


def AddTags(resource_id, resource_type, region, **kwargs):
  """Adds tags to an AliCloud resource created by PerfKitBenchmarker.

  Args:
    resource_id: An extant AliCloud resource to operate on.
    resource_type: The type of the resource.
    region: The AliCloud region 'resource_id' was created in.
    **kwargs: dict. Key-value pairs to set on the instance.
  """
  if not kwargs:
    return

  tag_cmd = ALI_PREFIX + [
      'ecs', 'AddTags',
      '--RegionId', region,
      '--ResourceId', resource_id,
      '--ResourceType', resource_type
  ]
  for index, (key, value) in enumerate(six.iteritems(kwargs)):
    tag_cmd.extend([
        '--Tag.{0}.Key'.format(index + 1), str(key),
        '--Tag.{0}.Value'.format(index + 1), str(value)
    ])
  vm_util.IssueRetryableCommand(tag_cmd)


def AddDefaultTags(resource_id, resource_type, region):
  """Adds tags to an AliCloud resource created by PerfKitBenchmarker.

  By default, resources are tagged with "owner" and "perfkitbenchmarker-run"
  key-value
  pairs.

  Args:
    resource_id: An extant AliCloud resource to operate on.
    resource_type: The type of the 'resource_id'
    region: The AliCloud region 'resource_id' was created in.
  """
  tags = {'owner': FLAGS.owner, 'perfkitbenchmarker-run': FLAGS.run_uri}
  AddTags(resource_id, resource_type, region, **tags)


def GetDrivePathPrefix():
  if FLAGS.ali_io_optimized is None:
    return '/dev/xvd'
  elif FLAGS.ali_io_optimized:
    return '/dev/vd'
