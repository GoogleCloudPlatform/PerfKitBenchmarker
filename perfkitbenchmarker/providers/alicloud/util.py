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


import shlex
import string
import random
import os
try:
    import paramiko
except:
    paramiko = None

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

ALI_PREFIX = ['aliyuncli']
ROOT = 'root'
FLAGS = flags.FLAGS
PASSWD_LEN = 20


REGION_HZ = 'cn-hangzhou'


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
      'ecs',
      'AddTags',
      '--RegionId %s' % region,
      '--ResourceId %s' % resource_id,
      '--ResourceType %s' % resource_type]
  for index, (key, value) in enumerate(kwargs.iteritems()):
    tag_cmd.append('--Tag{0}Key {1} --Tag{0}Value {2}'
                   .format(index + 1, key, value))
  tag_cmd = GetEncodedCmd(tag_cmd)
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


@vm_util.Retry(max_retries=10)
def AddPubKeyToHost(host_ip, password, keyfile, username):
  public_key = str()
  if keyfile:
    keyfile = os.path.expanduser(keyfile)
    if not paramiko:
      raise Exception('`paramiko` is required for pushing keyfile to ecs.')
    with open(keyfile, 'r') as f:
      public_key = f.read()
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  try:
    ssh.connect(
        host_ip,
        username=ROOT,  # We should use root to add pubkey to host
        password=password,
    )
    if username == ROOT:
      command_list = [
          'mkdir .ssh; echo "%s" >> ~/.ssh/authorized_keys' % public_key
      ]
    else:
      command_list = [
          'echo "{0} ALL = NOPASSWD: ALL" >> /etc/sudoers'.format(username),
          'useradd {0} --home /home/{0} --shell /bin/bash -m'.format(username),
          'mkdir /home/{0}/.ssh'.format(username),
          'echo "{0}" >> /home/{1}/.ssh/authorized_keys'
          .format(public_key, username),
          'chown -R {0}:{0} /home/{0}/.ssh'.format(username)]
    command = ';'.join(command_list)
    ssh.exec_command(command)

    ssh.close()
    return True
  except IOError:
    raise IOError


def GeneratePassword(length=PASSWD_LEN):
  digit_len = length / 2
  letter_len = length - digit_len
  return ''.join(random.sample(string.letters, digit_len)) + \
         ''.join(random.sample(string.digits, letter_len))


def GetDrivePathPrefix():
  if FLAGS.ali_io_optimized is None:
    return '/dev/xvd'
  elif FLAGS.ali_io_optimized:
    return '/dev/vd'
