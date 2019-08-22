# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for the AWS NFS service."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import json
import unittest
import mock

from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_disk
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_nfs_service
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case
import six

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'

_SUBNET_ID = 'subnet1'
_SECURITY_GROUP_ID = 'group1'

_AWS_ZONE = 'us-east-1d'
_AWS_REGION = 'us-east-1'
_AWS_CMD_PREFIX = ['aws', '--output', 'json', '--region', 'us-east-1', 'efs']

_FILE_ID = 'FSID'

_MOUNT_ID = 'FSMT'

_BENCHMARK = 'fio'
_RUN_URI = 'fb810a9b'
_OWNER = 'joe'
_NFS_TOKEN = 'nfs-token-%s' % _RUN_URI

_TIER = 'generalPurpose'
_THROUGHPUT_MODE = 'bursting'
_PROVISIONED_THROUGHPUT = 100.0

AwsResponses = collections.namedtuple('Responses', 'create describe')

_FILER = AwsResponses({
    'SizeInBytes': {
        'Value': 0
    },
    'CreationToken': _NFS_TOKEN,
    'CreationTime': 1513322422.0,
    'PerformanceMode': 'generalPurpose',
    'FileSystemId': _FILE_ID,
    'NumberOfMountTargets': 0,
    'LifeCycleState': 'creating',
    'OwnerId': '835761027970'
}, {
    'FileSystems': [{
        'SizeInBytes': {
            'Value': 6144
        },
        'CreationToken': _NFS_TOKEN,
        'CreationTime': 1513322422.0,
        'PerformanceMode': 'generalPurpose',
        'FileSystemId': _FILE_ID,
        'NumberOfMountTargets': 0,
        'LifeCycleState': 'available',
        'OwnerId': '835761027970'
    }]
})

_MOUNT = AwsResponses({
    'MountTargetId': _MOUNT_ID,
    'NetworkInterfaceId': 'eni-9956273b',
    'FileSystemId': _FILE_ID,
    'LifeCycleState': 'creating',
    'SubnetId': _SUBNET_ID,
    'OwnerId': '835761027970',
    'IpAddress': '10.0.0.182'
}, {
    'MountTargets': [{
        'MountTargetId': _MOUNT_ID,
        'NetworkInterfaceId': 'eni-9956273b',
        'FileSystemId': _FILE_ID,
        'LifeCycleState': 'available',
        'SubnetId': _SUBNET_ID,
        'OwnerId': '835761027970',
        'IpAddress': '10.0.0.182'
    }]
})


class BaseTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(BaseTest, self).setUp()
    self.issue_cmd = mock.Mock()
    self.aws_network_spec = self._CreatePatched(aws_network, 'AwsNetwork')
    mock_network = mock.Mock()
    mock_network.subnet.id = 'subnet1'
    mock_network.vpc.default_security_group_id = 'group1'
    self.aws_network_spec.GetNetworkFromNetworkSpec.return_value = mock_network

  def SetFlags(self, **kwargs):
    FLAGS['aws_user_name'].parse('aws_user')
    FLAGS['nfs_timeout_hard'].parse(True)
    FLAGS['benchmarks'].parse([_BENCHMARK])
    FLAGS['nfs_rsize'].parse(1048576)
    FLAGS['nfs_wsize'].parse(1048576)
    FLAGS['nfs_tier'].parse('generalPurpose')
    FLAGS['nfs_timeout'].parse(60)
    FLAGS['default_timeout'].parse(10)
    FLAGS['owner'].parse(_OWNER)
    FLAGS['nfs_retries'].parse(2)
    FLAGS['run_uri'].parse(_RUN_URI)
    FLAGS['nfs_version'].parse('4.1')
    FLAGS['temp_dir'].parse('/non/existent/temp/dir')
    FLAGS['aws_efs_token'].parse('')
    FLAGS['aws_delete_file_system'].parse(True)
    FLAGS['efs_throughput_mode'].parse(_THROUGHPUT_MODE)
    FLAGS['efs_provisioned_throughput'].parse(_PROVISIONED_THROUGHPUT)
    for key, value in six.iteritems(kwargs):
      FLAGS[key].parse(value)

  def _CreatePatched(self, module, method_name):
    patcher = mock.patch.object(module, method_name)
    mock_method = patcher.start()
    self.addCleanup(patcher.stop)
    return mock_method

  def _CreateDiskSpec(self, fs_type):
    return aws_disk.AwsDiskSpec(
        _COMPONENT,
        num_striped_disks=1,
        disk_type=fs_type if fs_type == disk.NFS else disk.LOCAL,
        mount_point='/scratch')

  def _CreateMockNetwork(self):
    mock_network = mock.Mock()
    mock_network.subnet.id = _SUBNET_ID
    mock_network.vpc.default_security_group_id = _SECURITY_GROUP_ID
    return mock_network

  def _CreateNfsService(self, nfs_tier=''):
    self.SetFlags(nfs_tier=nfs_tier)
    disk_spec = self._CreateDiskSpec(disk.NFS)
    nfs = aws_nfs_service.AwsNfsService(disk_spec, _AWS_ZONE)
    nfs.aws_commands = self.issue_cmd
    nfs.networks = [self._CreateMockNetwork()]
    return nfs


class AwsNfsServiceTest(BaseTest):

  def _CreateFiler(self):
    nfs = self._CreateNfsService()
    self.issue_cmd.CreateFiler.return_value = _FILE_ID
    nfs._CreateFiler()
    return nfs

  def _CreateMount(self):
    nfs = self._CreateNfsService()
    nfs.filer_id = _FILE_ID
    self.issue_cmd.CreateMount.return_value = _MOUNT_ID
    nfs._CreateMount()
    return nfs

  # create NFS resource
  def testCreateNfsService(self):
    nfs = self._CreateNfsService()
    self.assertEqual(_AWS_REGION, nfs.region)
    self.issue_cmd.assert_not_called()

  def testInvalidNfsTier(self):
    with self.assertRaises(errors.Config.InvalidValue):
      self._CreateNfsService('INVALID_TIER')

  def testValidNfsTier(self):
    nfs = self._CreateNfsService('maxIO')
    self.assertEqual('maxIO', nfs.nfs_tier)

  def testNoNfsTier(self):
    nfs = self._CreateNfsService()
    self.assertEqual('generalPurpose', nfs.nfs_tier)

  # tests for file system resource
  def testCreateFiler(self):
    nfs = self._CreateFiler()
    self.assertEqual(_FILE_ID, nfs.filer_id)
    self.issue_cmd.CreateFiler.assert_called_with(
        _NFS_TOKEN, _TIER, _THROUGHPUT_MODE, _PROVISIONED_THROUGHPUT)

  def testDeleteFiler(self):
    nfs = self._CreateFiler()
    nfs._DeleteFiler()
    self.issue_cmd.DeleteFiler.assert_called_with(_FILE_ID)

  def testDeleteFilerWithoutDeletingMountFirst(self):
    nfs = self._CreateFiler()
    nfs._CreateMount()
    self.issue_cmd.reset_mock()
    with self.assertRaises(errors.Resource.RetryableDeletionError):
      nfs._DeleteFiler()
    self.issue_cmd.assert_not_called()

  # tests for mount resource
  def testCreateMount(self):
    nfs = self._CreateMount()
    self.assertEqual(_MOUNT_ID, nfs.mount_id)
    self.issue_cmd.CreateMount.assert_called_with(_FILE_ID, _SUBNET_ID,
                                                  _SECURITY_GROUP_ID)

  def testCreateMountNoFiler(self):
    nfs = self._CreateNfsService()
    self.issue_cmd.reset_mock()
    with self.assertRaises(errors.Resource.CreationError):
      nfs._CreateMount()
    self.issue_cmd.assert_not_called()

  def testDeleteMount(self):
    nfs = self._CreateMount()
    self.issue_cmd.reset_mock()
    nfs._DeleteMount()
    self.issue_cmd.DeleteMount.assert_called_with(_MOUNT_ID)

  def testFullLifeCycle(self):
    # summation of the testCreate and testDelete calls
    nfs = self._CreateNfsService()
    self.issue_cmd.CreateFiler.return_value = _FILE_ID
    self.issue_cmd.CreateMount.return_value = _MOUNT_ID
    nfs.Create()
    nfs.Delete()
    self.issue_cmd.CreateFiler.assert_called_with(
        _NFS_TOKEN, _TIER, _THROUGHPUT_MODE, _PROVISIONED_THROUGHPUT)
    self.issue_cmd.AddTagsToFiler.assert_called_with(_FILE_ID)
    self.issue_cmd.WaitUntilFilerAvailable.assert_called_with(_FILE_ID)
    self.issue_cmd.CreateMount.assert_called_with(_FILE_ID, _SUBNET_ID,
                                                  _SECURITY_GROUP_ID)
    self.issue_cmd.DeleteMount.assert_called_with(_MOUNT_ID)
    self.issue_cmd.DeleteFiler.assert_called_with(_FILE_ID)


class AwsVirtualMachineTest(BaseTest):

  def _CreateMockVm(self):
    self._CreatePatched(aws_network, 'AwsNetwork')
    self._CreatePatched(aws_network, 'AwsFirewall')
    vm_spec = aws_virtual_machine.AwsVmSpec(
        _COMPONENT, zone=_AWS_ZONE, machine_type='m2.2xlarge')
    aws_machine = aws_virtual_machine.RhelBasedAwsVirtualMachine(vm_spec)
    aws_machine.RemoteCommand = mock.Mock()
    aws_machine.RemoteHostCommand = mock.Mock()
    return aws_machine

  def _SetBmSpec(self, nfs):
    bm_spec = mock.Mock()
    bm_spec.nfs_service = nfs
    get_spec = self._CreatePatched(context, 'GetThreadBenchmarkSpec')
    get_spec.return_value = bm_spec

  def _CallCreateScratchDisk(self, fs_type):
    nfs = self._CreateNfsService()
    self.issue_cmd.CreateFiler.return_value = _FILE_ID
    self.issue_cmd.CreateMount.return_value = _MOUNT_ID
    nfs.Create()
    self._SetBmSpec(nfs)
    aws_machine = self._CreateMockVm()
    aws_machine.CreateScratchDisk(self._CreateDiskSpec(fs_type))
    return aws_machine

  def testCreateNfsDisk(self):
    mount_opt = ('hard,nfsvers=4.1,retrans=2,rsize=1048576,timeo=600,'
                 'wsize=1048576')
    host = 'FSID.efs.us-east-1.amazonaws.com'
    mount_cmd = ('sudo mkdir -p /scratch;'
                 'sudo mount -t nfs -o {mount_opt} {host}:/ /scratch && '
                 'sudo chown $USER:$USER /scratch;').format(
                     mount_opt=mount_opt, host=host)
    fstab_cmd = ('echo "{host}:/ /scratch nfs {mount_opt}"'
                 ' | sudo tee -a /etc/fstab').format(
                     mount_opt=mount_opt, host=host)
    install_nfs = 'sudo yum install -y nfs-utils'

    aws_machine = self._CallCreateScratchDisk(disk.NFS)
    aws_machine.RemoteCommand.assert_called_with(install_nfs)
    self.assertEqual(
        [mock.call(mount_cmd), mock.call(fstab_cmd)],
        aws_machine.RemoteHostCommand.call_args_list)

  def testCreateLocalDisk(self):
    # show that the non-NFS case formats the disk
    format_cmd = (
        '[[ -d /mnt ]] && sudo umount /mnt; '
        'sudo mke2fs -F -E lazy_itable_init=0,discard -O ^has_journal '
        '-t ext4 -b 4096 /dev/xvdb')
    mount_cmd = ('sudo mkdir -p /scratch;'
                 'sudo mount -o discard /dev/xvdb /scratch && '
                 'sudo chown $USER:$USER /scratch;')
    fstab_cmd = ('echo "/dev/xvdb /scratch ext4 defaults" | sudo tee -a '
                 '/etc/fstab')

    aws_machine = self._CallCreateScratchDisk('ext4')
    self.assertEqual(
        [mock.call(format_cmd),
         mock.call(mount_cmd),
         mock.call(fstab_cmd)], aws_machine.RemoteHostCommand.call_args_list)


class AwsEfsCommandsTest(BaseTest):

  def setUp(self):
    super(AwsEfsCommandsTest, self).setUp()
    self.SetFlags()
    self.issue_cmd = self._CreatePatched(vm_util, 'IssueCommand')
    self.aws = aws_nfs_service.AwsEfsCommands(_AWS_REGION)

  def _SetResponse(self, json_value=None):
    txt = json.dumps(json_value) if json_value else ''
    self.issue_cmd.return_value = (txt, '', 0)

  def assertCalled(self, *args):
    cmd = ['aws', '--output', 'json', '--region', _AWS_REGION,
           'efs'] + list(args)
    self.issue_cmd.assert_called_with(cmd, raise_on_failure=False)

  def testCreateFiler(self):
    self._SetResponse(_FILER.create)
    self.aws.CreateFiler(_NFS_TOKEN, _TIER, _THROUGHPUT_MODE,
                         _PROVISIONED_THROUGHPUT)
    self.assertCalled('create-file-system', '--creation-token', _NFS_TOKEN,
                      '--performance-mode', _TIER, '--throughput-mode',
                      _THROUGHPUT_MODE)

  def testAddTags(self):
    self._SetResponse()
    self.aws.AddTagsToFiler(_FILE_ID)
    tags = util.MakeFormattedDefaultTags()
    self.assertCalled('create-tags', '--file-system-id', _FILE_ID, '--tags',
                      *tags)

  def testFilerAvailable(self):
    self._SetResponse(_FILER.describe)
    self.aws.WaitUntilFilerAvailable(_FILE_ID)
    self.assertCalled('describe-file-systems', '--file-system-id', _FILE_ID)

  def testMountAvailable(self):
    self._SetResponse(_MOUNT.describe)
    self.aws.IsMountAvailable(_MOUNT_ID)
    self.assertCalled('describe-mount-targets', '--mount-target-id', _MOUNT_ID)

  def testCreateMount(self):
    self._SetResponse(_MOUNT.create)
    self.aws.CreateMount(_FILE_ID, _SUBNET_ID, _SECURITY_GROUP_ID)
    self.assertCalled('create-mount-target', '--file-system-id', _FILE_ID,
                      '--subnet-id', _SUBNET_ID, '--security-groups',
                      _SECURITY_GROUP_ID)

  def testDeleteFiler(self):
    self._SetResponse()
    self.aws.DeleteFiler(_FILE_ID)
    self.assertCalled('delete-file-system', '--file-system-id', _FILE_ID)

  def testDeleteMount(self):
    self._SetResponse()
    self.aws.DeleteMount(_MOUNT_ID)
    self.assertCalled('delete-mount-target', '--mount-target-id', _MOUNT_ID)


if __name__ == '__main__':
  unittest.main()
