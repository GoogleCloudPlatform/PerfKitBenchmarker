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
import uuid
import random
import string

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class AwsGlobalAccelerator(resource.BaseResource):
  """An object representing an Aws Global Accelerator.
  https://docs.aws.amazon.com/global-accelerator/latest/dg/getting-started.html
  """

  def __init__(self):
    super(AwsGlobalAccelerator, self).__init__()
    # all global accelerators must be located in us-west-2
    self.region = 'us-west-2'
    self.idempotency_token = None

    #The name can have a maximum of 32 characters, 
    #must contain only alphanumeric characters or hyphens (-), 
    #and must not begin or end with a hyphen.
    self.name = None
    self.accelerator_arn = None
    self.enabled = False
    self.ip_addresses = []
    self.listeners = []

  def _Create(self):
    """Create a global accelerator"""
    if not self.idempotency_token:
      self.idempotency_token = str(uuid.uuid4())[-50:]

    self.name = 'pkb-ga-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4())[-12:])

    create_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'create-accelerator',
        '--name', self.name,
        '--region', self.region,
        '--idempotency-token', self.idempotency_token]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    print("ACCELERATOR CREATE STDOUT")
    print(stdout)
    response = json.loads(stdout)
    self.accelerator_arn = response['Accelerator']['AcceleratorArn']
    self.ip_addresses = response['Accelerator']['IpSets'][0]['IpAddresses']
    #util.AddDefaultTags(self.id, self.region)

  def _Delete(self):
    """Deletes the Accelerator"""

    # need to disable accelerator before it can be deleted
    self.Update(enabled=False)
    status = self.Describe()
    print("ACCELERATOR STATUS")
    print(status)
    while status['Accelerator']['Enabled'] == True:
      status = self.Describe()

    # need to delete listeners before accelerator can be deleted
    for listener in self.listeners:
      listener.Delete()

    delete_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'delete-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    stdout, stderr, _ = vm_util.IssueCommand(delete_cmd, raise_on_failure=False)
    print("ACCELERATOR DELETE STDOUT")
    print(stdout)
    exists = self._Exists()
    while exists:
      stdout, stderr, _ = vm_util.IssueCommand(delete_cmd, raise_on_failure=False)
      if "AcceleratorNotFoundException" in stderr:
        break
      exists = self._Exists()

  def Update(self, enabled: bool):
    """Updates the accelerator."""
    update_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'update-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    if enabled:
      update_cmd += ['--enabled']
    else:
      update_cmd += ['--no-enabled']
    stdout, _ = util.IssueRetryableCommand(update_cmd)
    print("ACCELERATOR UPDATE STDOUT")
    print(stdout)
    response = json.loads(stdout)
    accelerator = response['Accelerator']
    # assert accelerator['Enabled'] == enabled, 'Accelerator not updated'

  def _Exists(self):
    """Returns true if the accelerator exists."""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    try:
      stdout, _, _ = vm_util.IssueCommand(describe_cmd, raise_on_failure=False)
      print("ACCELERATOR EXISTS STDOUT")
      print(stdout)
      response = json.loads(stdout)
      accelerator = response['Accelerator']
      return len(accelerator) > 0
    except ValueError as e:
      return False

  def Describe(self):
    """Returns json description of accelerator"""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    return response

  def Status(self):
    """Returns status of accelerator"""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    status = response['Accelerator']['Status']
    return status

  def isUp(self):
    """Returns true if the accelerator is functioning"""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-accelerator',
        '--region', self.region,
        '--accelerator-arn', self.accelerator_arn]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    status = response['Accelerator']['Status']
    return status

  def AddListener(self, protocol:str, start_port:int, end_port:int):
    """Adds a new listener to the accelerator"""   
    new_listener = AwsGlobalAcceleratorListener(self,
                                            protocol,
                                            start_port,
                                            end_port)
    new_listener.Create()
    self.listeners.append(new_listener)


class AwsGlobalAcceleratorListener(resource.BaseResource):
  """Class representing an AWS Global Accelerator listener."""

  def __init__(self, accelerator, protocol, start_port, end_port):
    super(AwsGlobalAcceleratorListener, self).__init__()
    self.accelerator_arn = accelerator.accelerator_arn
    #self.target_group_arn = target_group.arn
    self.start_port = start_port
    self.end_port = end_port
    self.protocol = protocol
    self.region = accelerator.region
    self.idempotency_token = None
    self.arn = None
    self.endpoint_groups = []

  def _Create(self):
    """Create the listener."""
    if not self.idempotency_token:
      self.idempotency_token = str(uuid.uuid4())[-50:]
    create_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'create-listener',
        '--accelerator-arn', self.accelerator_arn,
        '--region', self.region,
        '--protocol', self.protocol,
        '--port-ranges', 
        'FromPort=%s,ToPort=%s' % (str(self.start_port), str(self.end_port)),
        '--idempotency-token', self.idempotency_token
    ]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    print("LISTENER CREATE STDOUT")
    print(stdout)
    response = json.loads(stdout)
    self.listener_arn = response['Listener']['ListenerArn']
    logging.info("LISTENER ARN")
    logging.info(self.listener_arn)

  def _Exists(self):
    """Returns true if the accelerator listener exists."""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-listener',
        '--region', self.region,
        '--listener-arn', self.listener_arn]
    stdout, stderr, return_code = vm_util.IssueCommand(describe_cmd, raise_on_failure=False)
    print("LISTENER DESCRIBE STDOUT")
    print(stdout)
    
    if return_code == 255:
      return False

    response = json.loads(stdout)

    if 'Listener' in response:
      return True



  def _Delete(self):
    """Deletes Listeners"""
    for endpoint_group in self.endpoint_groups:
      endpoint_group.Delete()

    delete_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'delete-listener',
        '--region', self.region,
        '--listener-arn', self.listener_arn]
    vm_util.IssueCommand(delete_cmd)

  def AddEndpointGroup(self, region, endpoint, weight):
    """Add end point group to listener."""
    self.endpoint_groups.append(AwsEndpointGroup(self, region))
    self.endpoint_groups[-1].Create()
    self.endpoint_groups[-1].Update(endpoint, weight)
    print("update done")
    return


class AwsEndpointGroup(resource.BaseResource):
  """An object representing an Endpoint Group for a Aws Global Accelerator 
     listener endpoint group.
  """

  def __init__(self, listener, endpoint_group_region):
    super(AwsEndpointGroup, self).__init__()
    # all global accelerators must be located in us-west-2
    self.region = 'us-west-2'
    self.idempotency_token = None
    self.listener_arn = listener.listener_arn
    self.endpoint_group_region = endpoint_group_region
    self.endpoint_group_arn = None
    self.endpoints = []

  def _Create(self):
    """Creates the endpoint group."""
    if not self.idempotency_token:
      self.idempotency_token = str(uuid.uuid4())[-50:]

    create_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'create-endpoint-group',
        '--listener-arn', self.listener_arn,
        '--endpoint-group-region', self.endpoint_group_region,
        '--region', self.region,
        '--idempotency-token', self.idempotency_token]
    stdout, _, _ = vm_util.IssueCommand(create_cmd)
    print("ENDPOINT GROUP CREATE STDOUT")
    print(stdout)
    response = json.loads(stdout)
    self.endpoint_group_arn = response['EndpointGroup']['EndpointGroupArn']

  def Update(self, endpoint, weight=128):
    """Update the endpoint group."""
    if not self.idempotency_token:
      self.idempotency_token = ''.join(
        random.choice(string.ascii_lowercase + 
                      string.ascii_uppercase +  
                      string.digits) for i in range(50))

    update_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'update-endpoint-group',
        '--region', self.region,
        '--endpoint-group-arn', self.endpoint_group_arn,
        '--endpoint-configurations', 
        'EndpointId=%s,Weight=%s' % (endpoint, str(weight))]
    stdout, _, _ = vm_util.IssueCommand(update_cmd)
    print("ENDPOINT GROUP UPDATE STDOUT")
    print(stdout)

  def _Delete(self):
    """Deletes the endpoint group."""
    delete_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'delete-endpoint-group',
        '--region', self.region,
        '--endpoint-group-arn', self.endpoint_group_arn]
    vm_util.IssueCommand(delete_cmd)

  def _Exists(self):
    """Returns true if the endpoint group exists."""
    describe_cmd = util.AWS_PREFIX + [
        'globalaccelerator',
        'describe-endpoint-group',
        '--region', self.region,
        '--endpoint-group-arn', self.endpoint_group_arn]
    stdout, stderr, return_code = vm_util.IssueCommand(describe_cmd, 
                                                       raise_on_failure=False)

    print("ENDPOINT GROUP EXISTS STDOUT/ERR")
    print(stdout)
    print(stderr)

    if return_code == 255:
      return False

    response = json.loads(stdout)

    if 'EndpointGroup' in response:
      return True
  
    return False
