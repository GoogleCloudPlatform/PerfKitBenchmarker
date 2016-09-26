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
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

flags.DEFINE_string('azure_lib_version', None,
                    'Use a particular version of azure client lib, e.g.: 1.0.2')

FLAGS = flags.FLAGS

DEFAULT_AZURE_REGION = 'eastus2'
AZURE_CREDENTIAL_LOCATION = '.azure'


class AzureBlobStorageService(object_storage_service.ObjectStorageService):
  """Interface to Azure Blob Storage.

  Relevant documentation:
  http://azure.microsoft.com/en-us/documentation/articles/xplat-cli/
  """

  STORAGE_NAME = providers.AZURE

  def PrepareService(self, location):
    # abs is "Azure Blob Storage"
    prefix = 'pkb%sabs' % FLAGS.run_uri
    self.resource_group = azure_network.GetResourceGroup()

    # We use a different Azure storage account than the VM account
    # because a) we need to be able to set the storage class
    # separately, including using a blob-specific storage account and
    # b) this account might be in a different location than any
    # VM-related account.
    self.storage_account = azure_network.AzureStorageAccount(
        FLAGS.azure_storage_type,
        location or DEFAULT_AZURE_REGION,
        prefix + 'storage',
        kind=FLAGS.azure_blob_account_kind)
    self.storage_account.Create()

  def CleanupService(self):
    self.storage_account.Delete()

  def MakeBucket(self, bucket):
    vm_util.IssueRetryableCommand([
        azure.AZURE_PATH, 'storage', 'container', 'create',
        bucket] + self.storage_account.connection_args)

  def DeleteBucket(self, bucket):
    vm_util.IssueCommand([
        azure.AZURE_PATH, 'storage', 'container', 'delete',
        '--quiet',
        bucket] + self.storage_account.connection_args)

  def EmptyBucket(self, bucket):
    # Emptying buckets on Azure is hard. We pass for now - this will
    # increase our use of storage space, but should not affect the
    # benchmark results.
    pass

  def PrepareVM(self, vm):
    vm.Install('azure_cli')

    if FLAGS.azure_lib_version:
      version_string = '==' + FLAGS.azure_lib_version
    else:
      version_string = ''
    vm.RemoteCommand('sudo pip install azure%s' % version_string)

    vm.PushFile(
        object_storage_service.FindCredentialFile('~/' +
                                                  AZURE_CREDENTIAL_LOCATION),
        AZURE_CREDENTIAL_LOCATION)

  def CLIUploadDirectory(self, vm, directory, file_names, bucket):
    return vm.RemoteCommand(
        ('time for file in {files}; '
         'do azure storage blob upload -q {directory}/$file {bucket} '
         '--connection-string {connection_string}; '
         'done').format(
             files=' '.join(file_names),
             directory=directory,
             bucket=bucket,
             connection_string=self.storage_account.connection_string))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        ('time for object in {objects}; '
         'do azure storage blob download {bucket} $object {dest} '
         '--connection-string {connection_string}; '
         'done').format(
             objects=' '.join(objects),
             bucket=bucket,
             dest=dest,
             connection_string=self.storage_account.connection_string))

  def Metadata(self, vm):
    return {'azure_lib_version':
            linux_packages.GetPipPackageVersion(vm, 'azure')}

  def APIScriptArgs(self):
    return ['--azure_account=%s' % self.storage_account.name,
            '--azure_key=%s' % self.storage_account.key]

  @classmethod
  def APIScriptFiles(cls):
    return ['azure_service.py']
