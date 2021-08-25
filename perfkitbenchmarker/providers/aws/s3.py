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

import json
import os
import posixpath
from typing import List

from absl import flags
from absl import logging
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers import aws
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS

AWS_CREDENTIAL_LOCATION = '.aws'
DEFAULT_AWS_REGION = 'us-east-1'
_READ = 's3:GetObject'
_WRITE = 's3:PutObject'


class S3Service(object_storage_service.ObjectStorageService):
  """Interface to Amazon S3."""

  STORAGE_NAME = aws.CLOUD

  region: str

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
    vm_util.IssueRetryableCommand(
        ['aws', 's3api', 'put-bucket-tagging',
         '--bucket', bucket_name,
         '--tagging', 'TagSet=[%s]' % tag_set,
         '--region=%s' % self.region])

  def Copy(self, src_url, dst_url, recursive=False):
    """See base class."""
    cmd = ['aws', 's3', 'cp', '--region', self.region]
    if recursive:
      cmd.append('--recursive')
      # Fix cp to mimic gsutil behavior
      dst_url = os.path.join(dst_url, os.path.basename(src_url))
    cmd += [src_url, dst_url]
    vm_util.IssueCommand(cmd)

  def CopyToBucket(self, src_path, bucket, object_path):
    """See base class."""
    dst_url = self.MakeRemoteCliDownloadUrl(bucket, object_path)
    vm_util.IssueCommand(['aws', 's3', 'cp', src_path, dst_url,
                          '--region', self.region])

  def MakeRemoteCliDownloadUrl(self, bucket, object_path):
    """See base class."""
    path = posixpath.join(bucket, object_path)
    return 's3://' + path

  def GenerateCliDownloadFileCommand(self, src_url, local_path):
    """See base class."""
    return 'aws s3 cp "%s" "%s" --region=%s' % (
        src_url, local_path, self.region)

  def List(self, bucket):
    """See base class."""
    stdout, _, _ = vm_util.IssueCommand(
        ['aws', 's3', 'ls', bucket, '--region', self.region])
    return stdout

  def ListTopLevelSubfolders(self, bucket):
    """Lists the top level folders (not files) in a bucket.

    Each result that is a folder has "PRE" in front of the name (meaning
    prefix), eg. "PRE customer/", so that part is removed from each line. When
    there's more than one result, splitting on the newline returns a final blank
    row, so blank values are skipped.

    Args:
      bucket: Name of the bucket to list the top level subfolders of.

    Returns:
      A list of top level subfolder names. Can be empty if there are no folders.
    """
    return [
        obj.split('PRE ')[1].strip().replace('/', '')
        for obj in self.List(bucket).split('\n')
        if obj and obj.endswith('/')
    ]

  @vm_util.Retry()
  def DeleteBucket(self, bucket):
    """See base class."""

    def _SuppressFailure(stdout, stderr, retcode):
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
        suppress_failure=_SuppressFailure)

  def EmptyBucket(self, bucket):
    vm_util.IssueCommand(
        ['aws', 's3', 'rm',
         's3://%s' % bucket,
         '--region', self.region,
         '--recursive'])

  def MakeBucketPubliclyReadable(self, bucket, also_make_writable=False):
    """See base class."""
    actions = [_READ]
    logging.warning('Making bucket %s publicly readable!', bucket)
    if also_make_writable:
      actions.append(_WRITE)
      logging.warning('Making bucket %s publicly writable!', bucket)
    vm_util.IssueCommand([
        'aws', 's3api', 'put-bucket-policy', '--region', self.region,
        '--bucket', bucket, '--policy',
        _MakeS3BucketPolicy(bucket, actions)
    ])

  def GetDownloadUrl(self, bucket, object_name, use_https=True):
    """See base class."""
    assert self.region
    scheme = 'https' if use_https else 'http'
    return f'{scheme}://{bucket}.s3.{self.region}.amazonaws.com/{object_name}'

  UPLOAD_HTTP_METHOD = 'PUT'

  def PrepareVM(self, vm):
    vm.Install('awscli')
    vm.Install('boto3')

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
    return {
        object_storage_service.BOTO_LIB_VERSION:
            linux_packages.GetPipPackageVersion(vm, 'boto3')
    }

  def APIScriptArgs(self):
    return ['--region=' + self.region]

  @classmethod
  def APIScriptFiles(cls):
    return ['s3.py']


def _MakeS3BucketPolicy(bucket: str,
                        actions: List[str],
                        object_prefix='') -> str:
  # https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_examples_s3_rw-bucket.html
  return json.dumps({
      'Version':
          '2012-10-17',
      'Statement': [{
          'Principal': '*',
          'Sid': 'PkbAcl',
          'Effect': 'Allow',
          'Action': actions,
          'Resource': [f'arn:aws:s3:::{bucket}/{object_prefix}*']
      }]
  })
