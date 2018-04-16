# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azurestack
from perfkitbenchmarker.providers.azurestack import azurestack_network

FLAGS = flags.FLAGS

DEFAULT_AZURESTACK_REGION = 'eastus2'


class AzureStackBlobStorageService(object_storage_service.ObjectStorageService):
  """Interface to AzureStack Blob Storage.

  Relevant documentation:
  http://azurestack.microsoft.com/en-us/documentation/articles/xplat-cli/
  """

  STORAGE_NAME = providers.AZURESTACK

  def PrepareService(self, location,
                     existing_storage_account_and_resource_group=None):
    # abs is "AzureStack Blob Storage"
    prefix = 'pkb%sabs' % FLAGS.run_uri

    # Maybe extract existing storage account and resource group names
    existing_storage_account, existing_resource_group = None, None
    if existing_storage_account_and_resource_group:
      existing_storage_account, existing_resource_group = \
          existing_storage_account_and_resource_group
      assert existing_storage_account is not None
      assert existing_resource_group is not None
    storage_account_name = existing_storage_account or prefix + 'storage'
    resource_group_name = existing_resource_group or prefix + '-resource-group'

    # We use a separate resource group so that our buckets can optionally stick
    # around after PKB runs. This is useful for things like cold reads tests
    self.resource_group = \
        azurestack_network.AzureStackResourceGroup(resource_group_name,
                                         existing_resource_group is not None)
    self.resource_group.Create()

    # We use a different AzureStack storage account than the VM account
    # because a) we need to be able to set the storage class
    # separately, including using a blob-specific storage account and
    # b) this account might be in a different location than any
    # VM-related account.
    self.storage_account = azurestack_network.AzureStackStorageAccount(
        FLAGS.azurestack_storage_type,
        location or DEFAULT_AZURESTACK_REGION,
        storage_account_name,
        kind=FLAGS.azurestack_blob_account_kind,
        resource_group=self.resource_group,
        use_existing=existing_storage_account is not None)
    self.storage_account.Create()

  def CleanupService(self):
    self.storage_account.Delete()
    self.resource_group.Delete()

  def MakeBucket(self, bucket):
    vm_util.IssueRetryableCommand([
        azurestack.AZURESTACK_PATH, 'storage', 'container', 'create',
        '--name', bucket] + self.storage_account.connection_args)

  def DeleteBucket(self, bucket):
    vm_util.IssueCommand([
        azurestack.AZURESTACK_PATH, 'storage', 'container', 'delete',
        '--name', bucket] + self.storage_account.connection_args)

  def EmptyBucket(self, bucket):
    # Emptying buckets on AzureStack is hard. We pass for now - this will
    # increase our use of storage space, but should not affect the
    # benchmark results.
    pass

  def PrepareVM(self, vm):
    vm.Install('azurestack_cli')
    vm.Install('azurestack_sdk')
    vm.Install('azurestack_credentials')

  def CLIUploadDirectory(self, vm, directory, file_names, bucket):
    return vm.RemoteCommand(
        ('time for file in {files}; '
         'do azurestack storage blob upload -q {directory}/$file {bucket} '
         '--connection-string {connection_string}; '
         'done').format(
             files=' '.join(file_names),
             directory=directory,
             bucket=bucket,
             connection_string=self.storage_account.connection_string))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        ('time for object in {objects}; '
         'do azurestack storage blob download {bucket} $object {dest} '
         '--connection-string {connection_string}; '
         'done').format(
             objects=' '.join(objects),
             bucket=bucket,
             dest=dest,
             connection_string=self.storage_account.connection_string))

  def Metadata(self, vm):
    return {'azurestack_lib_version':
            linux_packages.GetPipPackageVersion(vm, 'azurestack')}

  def APIScriptArgs(self):
    return ['--azurestack_account=%s' % self.storage_account.name,
            '--azurestack_key=%s' % self.storage_account.key]

  @classmethod
  def APIScriptFiles(cls):
    return ['azurestack_service.py']
