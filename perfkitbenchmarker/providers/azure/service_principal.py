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

"""Contains classes/functions related to Azure Service Principals."""

import json
import uuid
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure


class ServicePrincipal(resource.BaseResource):
  """Class representing an Azure service principal."""

  _instance = None

  @classmethod
  def GetInstance(cls):
    """Returns the service principal instance."""
    if cls._instance is None:
      cls._instance = cls()
    return cls._instance

  def __init__(self):
    super(ServicePrincipal, self).__init__()
    self.app_id = None
    self.name = str(uuid.uuid4())
    self.password = ''

  def _Create(self):
    """Creates the service principal."""
    cmd = [
        azure.AZURE_PATH, 'ad', 'sp', 'create-for-rbac', '--name', self.name,
        '--skip-assignment'
    ]
    stdout, _ = vm_util.IssueCommand(cmd)
    response = json.loads(stdout)
    if response:
      self.app_id = response['appId']
      self.password = response['password']
      return True
    return False

  def _Exists(self):
    """Returns True if the service principal exists."""
    cmd = [
        azure.AZURE_PATH, 'ad', 'sp', 'list', '--spn', 'http://' + self.name,
    ]
    stdout, _ = vm_util.IssueRetryableCommand(cmd)
    return bool(json.loads(stdout))

  def _Delete(self):
    """Deletes the service principal."""
    cmd = [
        azure.AZURE_PATH, 'ad', 'sp', 'delete', '--id', 'http://' + self.name,
    ]
    vm_util.IssueCommand(cmd)
