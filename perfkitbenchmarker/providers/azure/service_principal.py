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
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import azure_credentials
from perfkitbenchmarker.providers import azure

FLAGS = flags.FLAGS

SERVICE_PRINCIPAL_FILE = 'aksServicePrincipal.json'


class ServicePrincipal(resource.BaseResource):
  """Class representing an Azure service principal."""

  _instance = None

  @classmethod
  def GetInstance(cls):
    """Returns the service principal instance."""
    if cls._instance is None:
      if FLAGS.bootstrap_azure_service_principal:
        cls._instance = cls.LoadFromFile()
      else:
        cls._instance = cls()
    return cls._instance

  @classmethod
  def LoadFromFile(cls):
    """Loads a service principal from a file."""
    with open(object_storage_service.FindCredentialFile(azure_credentials.AZURE_CREDENTIAL_PROFILE_FILE), encoding='utf-8-sig') as profile_fp, \
        open(object_storage_service.FindCredentialFile(azure_credentials.AZURE_CREDENTIAL_TOKENS_FILE)) as tokens_fp:
      subscriptions = json.load(profile_fp)['subscriptions']
      subscription = [sub for sub in subscriptions if sub['isDefault']][0]
      subscription_type = subscription['user']['type']
      if subscription_type != 'servicePrincipal':
        # We are using user auth, and will probably have permission to create a
        # service principal.
        logging.info("Azure credentials are of type '%s'. "
                     'Will try to create a new service principal.',
                     subscription_type)
        return cls()
      # name and id are backwards
      name = subscription['id']
      app_id = subscription['user']['name']
      for token in json.load(tokens_fp):
        if token['servicePrincipalId'] == app_id:
          logging.info("Azure credentials are of type 'servicePrincipal'. "
                       'Will reuse them for benchmarking.')
          return cls(
              name, app_id, password=token['accessToken'], user_managed=True)
      logging.warning('No access tokens found matching Azure defaultProfile '
                      'Will try to create a new service principal.')
      return cls()

  def __init__(self, name=None, app_id=None, password=None, user_managed=False):
    super(ServicePrincipal, self).__init__(user_managed)
    # Service principles can be referred to by user provided name as long as
    # they are prefixed by http:// or by a server generated appId.
    # Prefer user provided ID for idempotence when talking to Active Directory.
    # When talking to AKS or ACR, app_id is required.
    self.name = 'http://' + (name or 'pkb-' + FLAGS.run_uri)
    self.app_id = app_id
    self.password = password

  def _Create(self):
    """Creates the service principal."""
    cmd = [
        azure.AZURE_PATH, 'ad', 'sp', 'create-for-rbac', '--name', self.name,
        '--skip-assignment'
    ]
    stdout, _, _ = vm_util.IssueCommand(cmd)
    response = json.loads(stdout)
    if response:
      self.app_id = response['appId']
      self.password = response['password']
      if not self.app_id or not self.password:
        raise errors.Resource.CreationError(
            'Invalid creation response when creating service principal. '
            'Expected appId and password. Received:\n' + stdout)
      return True
    return False

  def _Exists(self):
    """Returns True if the service principal exists."""
    # Use show rather than list, because list requires admin privileges.
    cmd = [azure.AZURE_PATH, 'ad', 'sp', 'show', '--id', self.name]
    try:
      vm_util.IssueCommand(cmd, raise_on_failure=True)
      return True
    except errors.VmUtil.IssueCommandError:
      return False

  def _Delete(self):
    """Deletes the service principal."""
    cmd = [azure.AZURE_PATH, 'ad', 'sp', 'delete', '--id', self.name]
    vm_util.IssueCommand(cmd)
