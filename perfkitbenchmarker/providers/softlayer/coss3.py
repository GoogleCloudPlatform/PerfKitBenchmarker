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

FLAGS = flags.FLAGS

AWS_CREDENTIAL_LOCATION = '.aws'

COS_S3_DEFAULT_ENDPOINT = 's3-api.us-geo.objectstorage.softlayer.net'


class S3Service(object_storage_service.ObjectStorageService):
  """Interface to IBM Cloud Object Storage S3 compatible storage."""

  STORAGE_NAME = providers.SOFTLAYER

  def PrepareService(self, location):
    self.endpoint = FLAGS.s3_custom_endpoint or COS_S3_DEFAULT_ENDPOINT

  def MakeBucket(self, bucket_name):
    vm_util.IssueCommand(
        ['aws',
	 '--endpoint-url',
	 'http://%s' % self.endpoint,
	 's3api', 'create-bucket',
	 '--bucket',
         '%s' % bucket_name])

  def DeleteBucket(self, bucket):
    vm_util.IssueCommand(
        ['aws',
	 '--endpoint-url',
	 'http://%s' % self.endpoint,
	 's3api',
	 'delete-bucket',
	 '--bucket',
         '%s' % bucket])

  def EmptyBucket(self, bucket):
    vm_util.IssueCommand(
        ['aws',
	 '--endpoint-url',
	 'http://%s' % self.endpoint,
	 's3',
	 'rm',
         's3://%s' % bucket,
         '--recursive'])

  def PrepareVM(self, vm):
    vm.Install('awscli')
    vm.Install('boto')

    vm.PushFile(
        object_storage_service.FindCredentialFile('~/' +
                                                  AWS_CREDENTIAL_LOCATION),
        AWS_CREDENTIAL_LOCATION)
    vm.PushFile(object_storage_service.FindBotoFile(),
                object_storage_service.DEFAULT_BOTO_LOCATION)

  def CleanupVM(self, vm):
    vm.Uninstall('awscli')

  def CLIUploadDirectory(self, vm, directory, file_names, bucket):
    return vm.RemoteCommand(
        'time aws --endpoint-url http://%s s3 sync %s s3://%s/' % (self.endpoint, directory, bucket))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        'time aws --endpoint-url http://%s s3 sync s3://%s/ %s' % (self.endpoint, bucket, dest))

  def Metadata(self, vm):
    return {object_storage_service.BOTO_LIB_VERSION:
            linux_packages.GetPipPackageVersion(vm, 'boto')}

  def APIScriptArgs(self):
      return ['--host=' + self.endpoint]

  @classmethod
  def APIScriptFiles(cls):
    return ['boto_service.py', 's3.py']
