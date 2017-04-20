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
DEFAULT_AWS_REGION = 'us-east-1'
# The endpoints in this table are subdomains of 'amazonaws.com'. So
# where the table says 's3-us-west-2', you should connect to
# 's3-us-west-2.amazonaws.com'. This table comes from
# http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
AWS_S3_REGION_TO_ENDPOINT_TABLE = {
    'us-east-1': 's3-external-1',
    'us-west-2': 's3-us-west-2',
    'us-west-1': 's3-us-west-1',
    'eu-west-1': 's3-eu-west-1',
    'eu-central-1': 's3-eu-central-1',
    'ap-southeast-1': 's3-ap-southeast-1',
    'ap-southeast-2': 's3-ap-southeast-2',
    'ap-northeast-1': 's3-ap-northeast-1',
    'ap-northeast-2': 's3-ap-northeast-2',
    'sa-east-1': 's3-sa-east-1'
}
AWS_S3_ENDPOINT_SUFFIX = '.amazonaws.com'


class S3Service(object_storage_service.ObjectStorageService):
  """Interface to Amazon S3."""

  STORAGE_NAME = providers.AWS

  def PrepareService(self, location):
    self.region = location or DEFAULT_AWS_REGION

  def MakeBucket(self, bucket_name):
    vm_util.IssueCommand(
        ['aws', 's3', 'mb',
         's3://%s' % bucket_name,
         '--region=%s' % self.region])

  @vm_util.Retry()
  def DeleteBucket(self, bucket):
    vm_util.IssueCommand(
        ['aws', 's3', 'rb',
         's3://%s' % bucket,
         '--region', self.region,
         '--force'])  # --force deletes even if bucket contains objects.

  def EmptyBucket(self, bucket):
    vm_util.IssueCommand(
        ['aws', 's3', 'rm',
         's3://%s' % bucket,
         '--region', self.region,
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
        'time aws s3 sync %s s3://%s/' % (directory, bucket))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        'time aws s3 sync s3://%s/ %s' % (bucket, dest))

  def Metadata(self, vm):
    return {object_storage_service.BOTO_LIB_VERSION:
            linux_packages.GetPipPackageVersion(vm, 'boto')}

  def APIScriptArgs(self):
    if FLAGS.s3_custom_endpoint:
      return ['--host=' + FLAGS.s3_custom_endpoint]
    else:
      hostname = AWS_S3_REGION_TO_ENDPOINT_TABLE[self.region]
      return ['--host=' + hostname + AWS_S3_ENDPOINT_SUFFIX]

  @classmethod
  def APIScriptFiles(cls):
    return ['boto_service.py', 's3.py']
