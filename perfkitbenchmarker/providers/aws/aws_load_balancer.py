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
"""Module containing classes related to AWS's load balancers."""

import json

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class TargetGroup(resource.BaseResource):
  """Class represeting an AWS target group."""

  def __init__(self, vpc, port):
    """Initializes the TargetGroup object.

    Args:
      vpc: AwsVpc object which contains the targets for load balancing.
      port: The internal port that the load balancer connects to.
    """
    super(TargetGroup, self).__init__()
    self.arn = None
    self.region = vpc.region
    self.name = 'pkb-%s' % FLAGS.run_uri
    self.protocol = 'TCP'
    self.port = port
    self.vpc_id = vpc.id

  def _Create(self):
    """Create the target group."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'elbv2', 'create-target-group',
        '--target-type', 'ip',
        '--name', self.name,
        '--protocol', self.protocol,
        '--port', str(self.port),
        '--vpc-id', self.vpc_id
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    response = json.loads(stdout)
    self.arn = response['TargetGroups'][0]['TargetGroupArn']

  def _Delete(self):
    """Delete the target group."""
    if self.arn is None:
      return
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'elbv2', 'delete-target-group',
        '--target-group-arn', self.arn
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)


class LoadBalancer(resource.BaseResource):
  """Class representing an AWS load balancer."""

  def __init__(self, subnets):
    """Initializes the LoadBalancer object.

    Args:
      subnets: List of AwsSubnet objects.
    """
    super(LoadBalancer, self).__init__()
    self.region = subnets[0].region
    self.name = 'pkb-%s' % FLAGS.run_uri
    self.subnet_ids = [subnet.id for subnet in subnets]
    self.type = 'network'
    self.arn = None
    self.dns_name = None

  def _Create(self):
    """Create the load balancer."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'elbv2', 'create-load-balancer',
        '--name', self.name,
        '--type', self.type,
        '--tags'] + util.MakeFormattedDefaultTags()
    # Add --subnets argument to the command.
    create_cmd.append('--subnets')
    create_cmd.extend(self.subnet_ids)

    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    load_balancer = json.loads(stdout)['LoadBalancers'][0]
    self.arn = load_balancer['LoadBalancerArn']
    self.dns_name = load_balancer['DNSName']

  def _Delete(self):
    """Delete the load balancer."""
    if self.arn is None:
      return
    delete_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'elbv2', 'delete-load-balancer',
        '--load-balancer-arn', self.arn
    ]
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)


class Listener(resource.BaseResource):
  """Class representing an AWS listener."""

  def __init__(self, load_balancer, target_group, port):
    super(Listener, self).__init__()
    self.load_balancer_arn = load_balancer.arn
    self.target_group_arn = target_group.arn
    self.port = port
    self.protocol = target_group.protocol
    self.region = target_group.region

  def _GetDefaultActions(self):
    """Returns a JSON representation of the default actions for the listener."""
    actions = [{
        'Type': 'forward',
        'TargetGroupArn': self.target_group_arn
    }]
    return json.dumps(actions)

  def _Create(self):
    """Create the listener."""
    create_cmd = util.AWS_PREFIX + [
        '--region', self.region,
        'elbv2', 'create-listener',
        '--load-balancer-arn', self.load_balancer_arn,
        '--protocol', self.protocol,
        '--port', str(self.port),
        '--default-actions', self._GetDefaultActions()
    ]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Listeners will be deleted along with their associated load balancers."""
    pass
