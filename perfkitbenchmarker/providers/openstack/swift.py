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

import os

from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import object_storage_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util

flags.DEFINE_boolean('openstack_swift_insecure', False,
                     'Allow swiftclient to access Swift service without \n'
                     'having to verify the SSL certificate')

FLAGS = flags.FLAGS

SWIFTCLIENT_LIB_VERSION = 'python-swiftclient_lib_version'


class SwiftStorageService(object_storage_service.ObjectStorageService):
  """Interface to OpenStack Swift."""

  STORAGE_NAME = providers.OPENSTACK

  def __init__(self):
    self.swift_command_prefix = ''

  def PrepareService(self, location):
    openstack_creds_set = ('OS_AUTH_URL' in os.environ,
                           'OS_TENANT_NAME' in os.environ,
                           'OS_USERNAME' in os.environ,
                           'OS_PASSWORD' in os.environ,)
    if not all(openstack_creds_set):
      raise errors.Benchmarks.MissingObjectCredentialException(
          'OpenStack credentials not found in environment variables')

    self.swift_command_parts = [
        '--os-auth-url', os.environ['OS_AUTH_URL'],
        '--os-tenant-name', os.environ['OS_TENANT_NAME'],
        '--os-username', os.environ['OS_USERNAME'],
        '--os-password', os.environ['OS_PASSWORD']]
    if FLAGS.openstack_swift_insecure:
      self.swift_command_parts.append('--insecure')

    self.swift_command_prefix = ' '.join(self.swift_command_parts)

  def MakeBucket(self, bucket):
    vm_util.IssueCommand(
        ['swift'] + self.swift_command_parts + ['post', bucket])

  def DeleteBucket(self, bucket):
    self.EmptyBucket(bucket)

    vm_util.IssueCommand(
        ['swift'] + self.swift_command_parts + ['delete', bucket])

  def EmptyBucket(self, bucket):
    vm_util.IssueCommand(
        ['swift'] + self.swift_command_parts + ['delete', bucket])

  def PrepareVM(self, vm):
    vm.Install('swift_client')

  def CleanupVM(self, vm):
    vm.Uninstall('swift_client')
    vm.RemoteCommand('/usr/bin/yes | sudo pip uninstall absl-py')

  def CLIUploadDirectory(self, vm, directory, file_names, bucket):
    return vm.RemoteCommand(
        'time swift %s upload %s %s'
        % (self.swift_command_prefix, bucket, directory))

  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    return vm.RemoteCommand(
        'time swift %s download %s -D %s'
        % (self.swift_command_prefix, bucket, dest))

  def Metadata(self, vm):
    return {SWIFTCLIENT_LIB_VERSION:
            linux_packages.GetPipPackageVersion(vm, 'python-swiftclient')}
