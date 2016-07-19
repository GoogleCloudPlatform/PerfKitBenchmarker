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
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import vm_util

flags.DEFINE_string('azure_lib_version', None,
                    'Use a particular version of azure client lib, e.g.: 1.0.2')

FLAGS = flags.FLAGS

DEFAULT_AZURE_REGION = 'East US'
AZURE_CREDENTIAL_LOCATION = '.azure'


class AzureBlobStorageService(object_storage_service.ObjectStorageService):
  """Interface to Azure Blob Storage.

  Relevant documentation:
  http://azure.microsoft.com/en-us/documentation/articles/xplat-cli/
  """

  STORAGE_NAME = providers.AZURE

  def PrepareService(self, location):
    self.storage_account = 'pkb%s' % FLAGS.run_uri
    vm_util.IssueCommand(
        ['azure', 'storage', 'account', 'create',
         '--type', 'ZRS',
         '-l', location or DEFAULT_AZURE_REGION,
         self.storage_account])

    output, _, _ = vm_util.IssueCommand(
        ['azure', 'storage', 'account',
         'keys', 'list', self.storage_account])

    self.azure_key = regex_util.ExtractExactlyOneMatch(
        r'Primary:* (.+)', output)

  def CleanupService(self):
    vm_util.IssueCommand(
        ['azure', 'storage', 'account', 'delete',
         '-q', self.storage_account])

  def MakeBucket(self, bucket):
    vm_util.IssueCommand(
        ['azure', 'storage', 'container',
         'create', bucket,
         '-a', self.storage_account,
         '-k', self.azure_key])

  def DeleteBucket(self, bucket):
    vm_util.IssueCommand(
        ['azure', 'storage', 'container',
         'delete', '-q', bucket,
         '-a', self.storage_account,
         '-k', self.azure_key])

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
         '-a {storage_account} -k {key}; '
         'done').format(files=' '.join(file_names),
                        directory=directory,
                        bucket=bucket,
                        storage_account=self.storage_account,
                        key=self.azure_key))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        ('time for object in {objects}; '
         'do azure storage blob download {bucket} $object {dest} '
         '-a {storage_account} -k {key}; '
         'done').format(objects=' '.join(objects),
                        bucket=bucket,
                        dest=dest,
                        storage_account=self.storage_account,
                        key=self.azure_key))

  def Metadata(self, vm):
    return {'azure_lib_version':
            linux_packages.GetPipPackageVersion(vm, 'azure')}

  def APIScriptArgs(self):
    return ['--azure_account=%s' % self.storage_account,
            '--azure_key=%s' % self.azure_key]

  @classmethod
  def APIScriptFiles(cls):
    return ['azure_service.py']
