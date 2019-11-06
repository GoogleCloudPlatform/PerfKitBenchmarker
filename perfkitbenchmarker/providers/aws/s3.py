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

"""Contains classes/functions related to S3."""

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

AWS_CREDENTIAL_LOCATION = '.aws'
DEFAULT_AWS_REGION = 'us-east-1'

# S3 endpoints for a given region can be formed by prefixing the region with
# 's3.' and suffixing it with '.amazonaws.com'.
AWS_S3_ENDPOINT_PREFIX = 's3.'
AWS_S3_ENDPOINT_SUFFIX = '.amazonaws.com'


class S3Service(object_storage_service.ObjectStorageService):
  """Interface to Amazon S3."""

  STORAGE_NAME = providers.AWS

  def PrepareService(self, location):
    self.region = location or DEFAULT_AWS_REGION

  def MakeBucket(self, bucket_name, raise_on_failure=True):
    command = [
        'aws', 's3', 'mb',
        's3://%s' % bucket_name,
        '--region=%s' % self.region
    ]
    _, stderr, ret_code = vm_util.IssueCommand(command, raise_on_failure=False)
    if ret_code and raise_on_failure:
      raise errors.Benchmarks.BucketCreationError(stderr)

    # Tag the bucket with the persistent timeout flag so that buckets can
    # optionally stick around after PKB runs.
    default_tags = util.MakeFormattedDefaultTags(
        timeout_minutes=max(FLAGS.timeout_minutes,
                            FLAGS.persistent_timeout_minutes))
    tag_set = ','.join('{%s}' % tag for tag in default_tags)
    vm_util.IssueCommand(
        ['aws', 's3api', 'put-bucket-tagging',
         '--bucket', bucket_name,
         '--tagging', 'TagSet=[%s]' % tag_set,
         '--region=%s' % self.region])

  def Copy(self, src_url, dst_url):
    """See base class."""
    vm_util.IssueCommand(['aws', 's3', 'cp', src_url, dst_url])

  def List(self, buckets):
    """See base class."""
    stdout, _, _ = vm_util.IssueCommand(['aws', 's3', 'ls', buckets])
    return stdout

  @vm_util.Retry()
  def DeleteBucket(self, bucket):
    """See base class."""

    def _suppress_failure(stdout, stderr, retcode):
      """Suppresses failure when bucket does not exist."""
      del stdout  # unused
      if retcode and 'NoSuchBucket' in stderr:
        return True
      return False

    vm_util.IssueCommand(
        ['aws', 's3', 'rb',
         's3://%s' % bucket,
         '--region', self.region,
         '--force'],  # --force deletes even if bucket contains objects.
        suppress_failure=_suppress_failure)

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
      return ['--host=%s%s%s' % (AWS_S3_ENDPOINT_PREFIX, self.region,
                                 AWS_S3_ENDPOINT_SUFFIX)]

  @classmethod
  def APIScriptFiles(cls):
    return ['boto_service.py', 's3.py']
