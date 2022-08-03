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

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util



class AwsElasticIP(resource.BaseResource):
  """An object representing an AWS Elastic IP address"""

  def __init__(self, region, domain='vpc'):
    super(AwsElasticIP, self).__init__()
    assert (domain in ('vpc', 'standard')), "Elastic IP domain type, %s, must be either vpc or standard" % domain
    self.domain = domain
    self.public_ip = None
    self.region = region
    self.allocation_id = None
    self.attached = False
    self.attached_resource_id = None
    self.association_id = None

  def _Create(self):
    """Creates the elastic IP address."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'allocate-address',
        '--domain', self.domain,
        '--region', self.region]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.allocation_id = response['AllocationId']
    self.public_ip = response['PublicIp']
    #util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the elastic IP."""

    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'release-address',
        '--region', self.region,
        '--allocation-id', self.allocation_id]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the elastic IP exists."""

    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-addresses',
        '--region', self.region,
        '--allocation-ids', self.allocation_id]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    addresses = response['Addresses']
    return len(addresses) > 0

  def AssociateAddress(self, attached_resource_id, is_network_interface=False):
    """Associates elastic IP with an EC2 instance in a VPC"""
    if not self.attached:
      self.attached_resource_id = attached_resource_id
      attach_cmd = util.AWS_PREFIX + [
          'ec2',
          'associate-address',
          '--region', self.region,
          '--allocation-id', self.allocation_id]
      if is_network_interface:
        attach_cmd = attach_cmd + [
            '--network-interface-id',
            self.attached_resource_id]
      else:
        attach_cmd = attach_cmd + [
            '--instance-id',
            self.attached_resource_id]
      stdout, _ = util.IssueRetryableCommand(attach_cmd)
      response = json.loads(stdout)
      self.association_id = response['AssociationId']
      self.attached = True

      return self.association_id

  def DisassociateAddress(self):
    """Disssociates elastic from EC2 Instance"""
    if self.attached:
      detach_cmd = util.AWS_PREFIX + [
          'ec2',
          'disassociate-address',
          '--region', self.region,
          '--association-id', self.association_id]
      util.IssueRetryableCommand(detach_cmd)
      self.attached = False 