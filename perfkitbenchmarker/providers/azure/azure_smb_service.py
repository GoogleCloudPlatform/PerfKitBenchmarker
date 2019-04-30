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
"""Azure SMB implementation.

See Azure Files
https://docs.microsoft.com/en-us/azure/storage/files/storage-files-introduction

This launches an Azure Files instance and creates a mount point.  Individual
AzureDisks will then mount the share.

The AzureSmbService object is a resource.BaseResource that has two resources
underneath it:
1. A resource to connect to the filer.
2. A resource to connect to the mount point on the filer.

Lifecycle:
1. Azure Files service created and blocks until it is available, as it is needed
   to make the mount point.
2. Issues a non-blocking call to create the mount point.  Does not block as the
   SmbDisk will block on it being available.
3. The SmbDisk then mounts the mount point and uses the disk like normal.
4. On teardown the mount point is first deleted.  Blocks on that returning.
5. The Azure Files service is then deleted.  Does not block as can take some
   time.

"""

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import network
from perfkitbenchmarker import providers
from perfkitbenchmarker import smb_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS


class AzureSmbService(smb_service.BaseSmbService):
  """An Azure SMB resource.

  Creates the Azure Files file system and mount point for use with SMB clients.

  See
  https://docs.microsoft.com/en-us/azure/storage/files/storage-files-introduction
  """

  CLOUD = providers.AZURE
  SMB_TIERS = ('Standard')
  # TODO(spencerkim): Add smb tier and version to metadata
  DEFAULT_SMB_VERSION = '3.0'
  DEFAULT_TIER = 'Standard'

  def __init__(self, disk_spec, zone):
    super(AzureSmbService, self).__init__(disk_spec, zone)
    self.name = 'azure-smb-fs-%s' % FLAGS.run_uri
    self.resource_group = azure_network.GetResourceGroup(self.zone)

  @property
  def network(self):
    network_spec = network.BaseNetworkSpec(self.zone)
    return azure_network.AzureNetwork.GetNetworkFromNetworkSpec(network_spec)

  def GetRemoteAddress(self):
    logging.debug('Calling GetRemoteAddress on SMB server %s', self.name)
    if self.name is None:
      raise errors.Resource.RetryableGetError('Filer not created')
    return '//{storage}.file.core.windows.net/{name}'.format(
        storage=self.storage_account_name, name=self.name)

  def GetStorageAccountAndKey(self):
    logging.debug('Calling GetStorageAccountAndKey on SMB server %s', self.name)
    if self.name is None:
      raise errors.Resource.RetryableGetError('Filer not created')
    return {'user': self.storage_account_name, 'pw': self.storage_account_key}

  def _Create(self):
    logging.info('Creating SMB server %s', self.name)
    storage_account_number = azure_network.AzureStorageAccount.total_storage_accounts - 1
    self.storage_account_name = 'pkb%s' % FLAGS.run_uri + 'storage' + str(
        storage_account_number)
    self.connection_args = util.GetAzureStorageConnectionArgs(
        self.storage_account_name, self.resource_group.args)
    self.storage_account_key = util.GetAzureStorageAccountKey(
        self.storage_account_name, self.resource_group.args)

    self._AzureSmbCommand('create')

  def _Delete(self):
    logging.info('Deleting SMB server %s', self.name)
    self._AzureSmbCommand('delete')

  def _Exists(self):
    logging.debug('Calling Exists on SMB server %s', self.name)
    return self._AzureSmbCommand('exists')['exists']

  def _IsReady(self):
    logging.debug('Calling IsReady on SMB server %s', self.name)
    return self._Exists()

  def _Describe(self):
    logging.debug('Calling Describe on SMB server %s', self.name)
    output = self._AzureSmbCommand('show')
    return output

  def _AzureSmbCommand(self, verb):
    cmd = [azure.AZURE_PATH, 'storage', 'share', verb, '--output', 'json']
    cmd += ['--name', self.name]
    if verb == 'create':
      cmd += ['--quota', str(FLAGS.data_disk_size)]
    cmd += self.connection_args
    stdout, stderr, retcode = vm_util.IssueCommand(cmd)
    if retcode:
      raise errors.Error('Error running command %s : %s' % (verb, stderr))
    return json.loads(stdout)
