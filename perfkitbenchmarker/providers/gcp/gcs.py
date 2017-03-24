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

import logging
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

flags.DEFINE_string('google_cloud_sdk_version', None,
                    'Use a particular version of the Google Cloud SDK, e.g.: '
                    '103.0.0')

FLAGS = flags.FLAGS

GCS_CREDENTIAL_LOCATION = '.config/gcloud/credentials'
DEFAULT_GCP_REGION = 'us-central1'
GCLOUD_CONFIG_PATH = '.config/gcloud'


class GoogleCloudStorageService(object_storage_service.ObjectStorageService):
  """Interface to Google Cloud Storage."""

  STORAGE_NAME = providers.GCP

  def PrepareService(self, location):
    self.location = location or DEFAULT_GCP_REGION

  def MakeBucket(self, bucket):
    command = ['gsutil', 'mb']
    if self.location:
      command.extend(['-l', self.location])
    if self.location and '-' in self.location:
      # regional buckets
      command.extend(['-c', 'regional'])
    elif FLAGS.object_storage_storage_class is not None:
      command.extend(['-c', FLAGS.object_storage_storage_class])
    command.extend(['gs://%s' % bucket])

    vm_util.IssueCommand(command)

  @vm_util.Retry()
  def DeleteBucket(self, bucket):
    # We want to retry rm and rb together because it's possible that
    # we issue rm followed by rb, but then rb fails because the
    # metadata store isn't consistent and the server that handles the
    # rb thinks there are still objects in the bucket. It's also
    # possible for rm to fail because the metadata store is
    # inconsistent and rm doesn't find all objects, so can't delete
    # them all.
    self.EmptyBucket(bucket)

    vm_util.IssueCommand(
        ['gsutil', 'rb',
         'gs://%s' % bucket])

  def EmptyBucket(self, bucket):
    vm_util.IssueCommand(
        ['gsutil', '-m', 'rm', '-r',
         'gs://%s/*' % bucket])

  def PrepareVM(self, vm):
    vm.Install('wget')
    # Unfortunately there isn't one URL scheme that works for both
    # versioned archives and "always get the latest version".
    if FLAGS.google_cloud_sdk_version is not None:
      sdk_file = ('google-cloud-sdk-%s-linux-x86_64.tar.gz' %
                  FLAGS.google_cloud_sdk_version)
      sdk_url = 'https://storage.googleapis.com/cloud-sdk-release/' + sdk_file
    else:
      sdk_file = 'google-cloud-sdk.tar.gz'
      sdk_url = 'https://dl.google.com/dl/cloudsdk/release/' + sdk_file
    vm.RemoteCommand('wget ' + sdk_url)
    vm.RemoteCommand('tar xvf ' + sdk_file)
    # Versioned and unversioned archives both unzip to a folder called
    # 'google-cloud-sdk'.
    vm.RemoteCommand('bash ./google-cloud-sdk/install.sh '
                     '--disable-installation-options '
                     '--usage-report=false '
                     '--rc-path=.bash_profile '
                     '--path-update=true '
                     '--bash-completion=true')

    vm.RemoteCommand('mkdir -p .config')
    vm.PushFile(object_storage_service.FindBotoFile(),
                object_storage_service.DEFAULT_BOTO_LOCATION)

    vm.gsutil_path, _ = vm.RemoteCommand('which gsutil', login_shell=True)
    vm.gsutil_path = vm.gsutil_path.split()[0]

    # Detect if we need to install crcmod for gcp.
    # See "gsutil help crc" for details.
    raw_result, _ = vm.RemoteCommand('%s version -l' % vm.gsutil_path)
    logging.info('gsutil version -l raw result is %s', raw_result)
    search_string = 'compiled crcmod: True'
    result_string = re.findall(search_string, raw_result)
    if len(result_string) == 0:
      logging.info('compiled crcmod is not available, installing now...')
      try:
        # Try uninstall first just in case there is a pure python version of
        # crcmod on the system already, this is required by gsutil doc:
        # https://cloud.google.com/storage/docs/
        # gsutil/addlhelp/CRC32CandInstallingcrcmod
        vm.Uninstall('crcmod')
      except errors.VirtualMachine.RemoteCommandError:
        logging.info('pip uninstall crcmod failed, could be normal if crcmod '
                     'is not available at all.')
        pass
      vm.Install('crcmod')
      vm.installed_crcmod = True
    else:
      logging.info('compiled crcmod is available, not installing again.')
      vm.installed_crcmod = False

    vm.Install('gcs_boto_plugin')

  def CleanupVM(self, vm):
    vm.RemoveFile('google-cloud-sdk')
    vm.RemoveFile(GCLOUD_CONFIG_PATH)
    vm.RemoveFile(object_storage_service.DEFAULT_BOTO_LOCATION)
    vm.Uninstall('gcs_boto_plugin')

  def CLIUploadDirectory(self, vm, directory, files, bucket):
    return vm.RemoteCommand(
        'time %s -m cp %s/* gs://%s/' % (
            vm.gsutil_path, directory, bucket))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        'time %s -m cp gs://%s/* %s' % (vm.gsutil_path, bucket, dest))

  def Metadata(self, vm):
    metadata = {'pkb_installed_crcmod': vm.installed_crcmod,
                object_storage_service.BOTO_LIB_VERSION:
                linux_packages.GetPipPackageVersion(vm, 'boto')}

    return metadata

  @classmethod
  def APIScriptFiles(cls):
    return ['boto_service.py', 'gcs.py']
