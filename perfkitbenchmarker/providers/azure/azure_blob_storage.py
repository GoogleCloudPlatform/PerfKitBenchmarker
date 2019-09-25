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

"""Contains classes/functions related to Azure Blob Storage."""

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import azure_network

FLAGS = flags.FLAGS

DEFAULT_AZURE_REGION = 'eastus2'


class AzureBlobStorageService(object_storage_service.ObjectStorageService):
  """Interface to Azure Blob Storage.

  Relevant documentation:
  http://azure.microsoft.com/en-us/documentation/articles/xplat-cli/
  """

  STORAGE_NAME = providers.AZURE

  def PrepareService(self, location,
                     existing_storage_account_and_resource_group=None,
                     try_to_create_storage_account_and_resource_group=False):
    """See base class (without additional args).

    TODO(deitz): We should use the same interface across the clouds without
    additional arguments.

    Args:
      location: where to place our data.
      existing_storage_account_and_resource_group: An existing storage account
          and resource group for reading objects that may have already been
          created.
      try_to_create_storage_account_and_resource_group: Whether to try to create
          the storage account and resource group in case it does not exist yet.
          This supports invoking the object_storage_service_benchmark multiple
          times on the same bucket name and creating the resource group the
          first time. While this defaults to False, if there is no existing
          storage account and resource group passed to this function via
          existing_storage_account_and_resource_group, then one will be created.
    """
    # abs is "Azure Blob Storage"
    prefix = 'pkb%sabs' % FLAGS.run_uri

    # Maybe extract existing storage account and resource group names
    existing_storage_account, existing_resource_group = None, None
    if existing_storage_account_and_resource_group:
      existing_storage_account, existing_resource_group = \
          existing_storage_account_and_resource_group
      assert existing_storage_account is not None
      assert existing_resource_group is not None
    else:
      # We don't have an existing storage account or resource group so we better
      # create one.
      try_to_create_storage_account_and_resource_group = True
    storage_account_name = existing_storage_account or prefix + 'storage'
    resource_group_name = existing_resource_group or prefix + '-resource-group'

    # If we have an existing storage account and resource, we typically would
    # not try to create it. If try_to_create_storage_account_and_resource_group
    # is True, however, then we do try to create it. In this case, we shouldn't
    # raise on a failure since it may already exist.
    raise_on_create_failure = not (
        existing_storage_account_and_resource_group and
        try_to_create_storage_account_and_resource_group)

    # We use a separate resource group so that our buckets can optionally stick
    # around after PKB runs. This is useful for things like cold reads tests
    self.resource_group = \
        azure_network.AzureResourceGroup(
            resource_group_name,
            use_existing=not try_to_create_storage_account_and_resource_group,
            timeout_minutes=max(FLAGS.timeout_minutes,
                                FLAGS.persistent_timeout_minutes),
            raise_on_create_failure=raise_on_create_failure)
    self.resource_group.Create()

    # We use a different Azure storage account than the VM account
    # because a) we need to be able to set the storage class
    # separately, including using a blob-specific storage account and
    # b) this account might be in a different location than any
    # VM-related account.
    self.storage_account = azure_network.AzureStorageAccount(
        FLAGS.azure_storage_type,
        location or DEFAULT_AZURE_REGION,
        storage_account_name,
        kind=FLAGS.azure_blob_account_kind,
        resource_group=self.resource_group,
        use_existing=not try_to_create_storage_account_and_resource_group,
        raise_on_create_failure=raise_on_create_failure)
    self.storage_account.Create()

  def CleanupService(self):
    self.storage_account.Delete()
    self.resource_group.Delete()

  def MakeBucket(self, bucket, raise_on_failure=True):
    _, stderr, ret_code = vm_util.IssueCommand(
        [azure.AZURE_PATH, 'storage', 'container', 'create', '--name', bucket] +
        self.storage_account.connection_args,
        raise_on_failure=False)
    if ret_code and raise_on_failure:
      raise errors.Benchmarks.BucketCreationError(stderr)

  def DeleteBucket(self, bucket):
    vm_util.IssueCommand(
        [azure.AZURE_PATH, 'storage', 'container', 'delete', '--name', bucket] +
        self.storage_account.connection_args,
        raise_on_failure=False)

  def Copy(self, src_url, dst_url):
    """See base class."""
    raise NotImplementedError()

  def List(self, buckets):
    """See base class."""
    raise NotImplementedError()

  def EmptyBucket(self, bucket):
    # Emptying buckets on Azure is hard. We pass for now - this will
    # increase our use of storage space, but should not affect the
    # benchmark results.
    pass

  def PrepareVM(self, vm):
    vm.Install('azure_cli')
    vm.Install('azure_sdk')
    vm.Install('azure_credentials')

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
