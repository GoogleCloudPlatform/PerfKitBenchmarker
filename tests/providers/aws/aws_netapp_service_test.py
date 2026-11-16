# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for the AWS FSx for NetApp ONTAP service."""

import json
import time
import unittest
from unittest import mock

from absl import flags
from absl.testing import parameterized
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import aws_netapp_service
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_RUN_URI = 'fb810a9b'
_ZONE = 'us-east-1a'
_REGION = 'us-east-1'
_SUBNET_ID = 'subnet-1'
_SECURITY_GROUP_ID = 'sg-1'
_FILE_SYSTEM_ID = 'fs-1'
_SVM_ID = 'svm-1'
_VOLUME_ID = 'fsvol-1'
_NFS_IP = '10.0.0.37'
_NOT_FOUND = ('', 'NotFound', 255)


def _JsonResponse(value):
  return (json.dumps(value), '', 0)


def _DescribeResponse(response_key, lifecycle, **fields):
  return _JsonResponse({response_key: [{'Lifecycle': lifecycle, **fields}]})


class AwsNetAppServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS['run_uri'].parse(_RUN_URI)
    self.enter_context(mock.patch.object(time, 'sleep'))
    network = mock.Mock()
    network.subnet.id = _SUBNET_ID
    network.regional_network.vpc.default_security_group_id = _SECURITY_GROUP_ID
    aws_network_class = self.enter_context(
        mock.patch.object(aws_network, 'AwsNetwork')
    )
    aws_network_class.GetNetworkFromNetworkSpec.return_value = network

  def _NetAppService(self, disk_size=1024, spec_kwargs=None, **flag_values):
    for name, value in flag_values.items():
      FLAGS[name].parse(value)
    spec = aws_netapp_service.AwsNetAppDiskSpec(
        'test_component',
        FLAGS,
        disk_size=disk_size,
        disk_type=disk.NETAPP_VOLUMES,
        **(spec_kwargs or {}),
    )
    return aws_netapp_service.AwsNetAppService(spec, _ZONE)

  def _CreatedNetAppService(self):
    service = self._NetAppService()
    service.file_system_id = _FILE_SYSTEM_ID
    service.svm_id = _SVM_ID
    service.volume_id = _VOLUME_ID
    return service

  def _OntapConfiguration(self, issue_command, fsx_command):
    for call in issue_command.func_to_mock.call_args_list:
      cmd = call.args[0]
      if fsx_command in cmd:
        return json.loads(cmd[cmd.index('--ontap-configuration') + 1])
    self.fail(f'{fsx_command} was not run.')

  def testDefaultThroughputCapacity(self):
    service = self._NetAppService()
    self.assertEqual(service.disk_spec.provisioned_throughput, 1536)

  def testThroughputCapacityFromDiskSpec(self):
    service = self._NetAppService(
        spec_kwargs={'provisioned_throughput': 3072}
    )
    self.assertEqual(service.disk_spec.provisioned_throughput, 3072)

  def testFlagOverridesDiskSpec(self):
    service = self._NetAppService(
        spec_kwargs={'provisioned_throughput': 3072},
        provisioned_throughput=768,
    )
    self.assertEqual(service.disk_spec.provisioned_throughput, 768)

  @parameterized.named_parameters(
      ('DiskSpec', {'spec_kwargs': {'provisioned_throughput': 1000}}),
      ('Flag', {'provisioned_throughput': 1000}),
  )
  def testInvalidThroughputCapacity(self, kwargs):
    with self.assertRaises(errors.Config.InvalidValue):
      self._NetAppService(**kwargs)

  @parameterized.named_parameters(
      ('BelowMinimum', 500, 1024),
      ('AboveMinimum', 17920, 17920),
  )
  def testFileSystemCapacity(self, disk_size, expected_capacity_gib):
    service = self._NetAppService(disk_size=disk_size)
    self.assertEqual(service.file_system_capacity_gib, expected_capacity_gib)

  def testGetResourceMetadata(self):
    service = self._NetAppService(provisioned_throughput=3072)
    metadata = service.GetResourceMetadata()
    self.assertEqual(metadata['netapp_deployment_type'], 'SINGLE_AZ_2')
    self.assertEqual(metadata['netapp_throughput_capacity'], 3072)
    self.assertEqual(metadata['netapp_file_system_capacity_gib'], 1024)

  def testCreate(self):
    service = self._NetAppService(disk_size=2048)
    issue_command = self.MockIssueCommand({
        'create-file-system': [
            _JsonResponse({'FileSystem': {'FileSystemId': _FILE_SYSTEM_ID}})
        ],
        'describe-file-systems': [
            _DescribeResponse('FileSystems', 'CREATING'),
            _DescribeResponse('FileSystems', 'AVAILABLE'),
        ],
        'create-storage-virtual-machine': [
            _JsonResponse(
                {'StorageVirtualMachine': {'StorageVirtualMachineId': _SVM_ID}}
            )
        ],
        'describe-storage-virtual-machines': [
            _DescribeResponse('StorageVirtualMachines', 'CREATED')
        ],
        'create-volume': [_JsonResponse({'Volume': {'VolumeId': _VOLUME_ID}})],
        'describe-volumes': [_DescribeResponse('Volumes', 'CREATED')],
    })

    service.Create()

    self.assertEqual(service.file_system_id, _FILE_SYSTEM_ID)
    self.assertEqual(service.svm_id, _SVM_ID)
    self.assertEqual(service.volume_id, _VOLUME_ID)
    self.assertEqual(
        issue_command.progress_through_calls['describe-file-systems'], 2
    )
    file_system_cmd = issue_command.GetCommandWithSubstring(
        'create-file-system'
    )
    self.assertIn('--storage-capacity=2048', file_system_cmd)
    self.assertIn(f'--subnet-ids={_SUBNET_ID}', file_system_cmd)
    self.assertIn(f'--security-group-ids={_SECURITY_GROUP_ID}', file_system_cmd)
    self.assertEqual(
        self._OntapConfiguration(issue_command, 'create-file-system'),
        {
            'DeploymentType': 'SINGLE_AZ_2',
            'HAPairs': 1,
            'ThroughputCapacity': 1536,
            'AutomaticBackupRetentionDays': 0,
        },
    )
    svm_cmd = issue_command.GetCommandWithSubstring(
        'create-storage-virtual-machine'
    )
    self.assertIn(f'--file-system-id={_FILE_SYSTEM_ID}', svm_cmd)
    self.assertIn(f'--name=pkb_svm_{_RUN_URI}', svm_cmd)
    self.assertIn('--root-volume-security-style=UNIX', svm_cmd)
    self.assertIn(
        f'--name=pkb_vol_{_RUN_URI}',
        issue_command.GetCommandWithSubstring('create-volume'),
    )
    self.assertEqual(
        self._OntapConfiguration(issue_command, 'create-volume'),
        {
            'StorageVirtualMachineId': _SVM_ID,
            'JunctionPath': f'/pkb_vol_{_RUN_URI}',
            'SizeInBytes': 2048 * 1024**3,
            'VolumeStyle': 'FLEXVOL',
            'StorageEfficiencyEnabled': False,
        },
    )

  def testCreateFileSystemFailure(self):
    service = self._NetAppService()
    issue_command = self.MockIssueCommand(
        {'create-file-system': [('', 'BadRequest', 255)]}
    )
    with self.assertRaises(errors.Resource.CreationError):
      service.Create()
    self.assertNotIn(
        'create-storage-virtual-machine', issue_command.all_commands
    )

  def testCreateVolumeFailureIsRetryable(self):
    service = self._NetAppService()
    service.svm_id = _SVM_ID
    self.MockIssueCommand({'create-volume': [('', 'BadRequest', 255)]})
    with self.assertRaises(errors.Resource.RetryableCreationError):
      service._Create()

  def testCreateNetAppDisk(self):
    service = self._CreatedNetAppService()
    issue_command = self.MockIssueCommand({
        'describe-storage-virtual-machines': [
            _DescribeResponse(
                'StorageVirtualMachines',
                'CREATED',
                Endpoints={'Nfs': {'IpAddresses': [_NFS_IP]}},
            )
        ]
    })
    netapp_disk = service.CreateNetAppDisk()
    self.assertEqual(
        netapp_disk.GetDevicePath(), f'{_NFS_IP}:/pkb_vol_{_RUN_URI}'
    )
    self.assertIn(
        f'--storage-virtual-machine-ids={_SVM_ID}',
        issue_command.GetCommandWithSubstring(
            'describe-storage-virtual-machines'
        ),
    )

  def testGetRemoteAddressWithoutNfsIp(self):
    service = self._CreatedNetAppService()
    self.MockIssueCommand({
        'describe-storage-virtual-machines': [
            _DescribeResponse('StorageVirtualMachines', 'CREATED')
        ]
    })
    with self.assertRaises(errors.Resource.RetryableGetError):
      service._GetRemoteAddress()

  @parameterized.named_parameters(
      ('Created', 'CREATED', True),
      ('Creating', 'CREATING', False),
  )
  def testIsReady(self, lifecycle, expected_ready):
    service = self._CreatedNetAppService()
    self.MockIssueCommand(
        {'describe-volumes': [_DescribeResponse('Volumes', lifecycle)]}
    )
    self.assertEqual(service._IsReady(), expected_ready)

  @parameterized.named_parameters(
      (
          'Failed',
          {'Lifecycle': 'FAILED', 'FailureDetails': {'Message': 'Bad subnet'}},
          errors.Resource.CreationError,
          'Bad subnet',
      ),
      (
          'Misconfigured',
          {
              'Lifecycle': 'MISCONFIGURED',
              'LifecycleTransitionReason': {'Message': 'Bad config'},
          },
          errors.Resource.CreationError,
          'Bad config',
      ),
      (
          'InsufficientCapacity',
          {
              'Lifecycle': 'FAILED',
              'FailureDetails': {'Message': 'Insufficient capacity in AZ'},
          },
          errors.Benchmarks.InsufficientCapacityCloudFailure,
          'Insufficient capacity',
      ),
  )
  def testIsLifecycleReadyRaises(self, details, expected_error, message):
    with self.assertRaisesRegex(expected_error, message):
      aws_netapp_service.AwsNetAppService._IsLifecycleReady(
          'File system', details, ('AVAILABLE',)
      )

  def testExists(self):
    service = self._CreatedNetAppService()
    self.MockIssueCommand({
        'describe-volumes': [
            _DescribeResponse('Volumes', 'CREATED'),
            _NOT_FOUND,
        ]
    })
    self.assertTrue(service._Exists())
    self.assertFalse(service._Exists())

  def testDelete(self):
    service = self._CreatedNetAppService()
    service.created = True
    issue_command = self.MockIssueCommand({
        'delete-volume': [('', '', 0)],
        'describe-volumes': [_NOT_FOUND],
        'describe-storage-virtual-machines': [
            _DescribeResponse('StorageVirtualMachines', 'CREATED'),
            _NOT_FOUND,
        ],
        'delete-storage-virtual-machine': [('', '', 0)],
        'describe-file-systems': [
            _DescribeResponse('FileSystems', 'AVAILABLE'),
            _NOT_FOUND,
        ],
        'delete-file-system': [('', '', 0)],
    })

    service.Delete()

    commands = issue_command.all_commands
    self.assertLess(
        commands.index('delete-volume'),
        commands.index('delete-storage-virtual-machine'),
    )
    self.assertLess(
        commands.index('delete-storage-virtual-machine'),
        commands.index('delete-file-system'),
    )
    self.assertIn(
        '"SkipFinalBackup": true',
        issue_command.GetCommandWithSubstring('delete-volume'),
    )

  def testDeleteDependenciesWaitsWithoutResendingDelete(self):
    service = self._CreatedNetAppService()
    service.file_system_id = None
    issue_command = self.MockIssueCommand({
        'describe-storage-virtual-machines': [
            _DescribeResponse('StorageVirtualMachines', 'DELETING'),
            _NOT_FOUND,
        ]
    })
    service._DeleteDependencies()
    self.assertEqual(
        issue_command.progress_through_calls[
            'describe-storage-virtual-machines'
        ],
        2,
    )
    self.assertNotIn(
        'delete-storage-virtual-machine', issue_command.all_commands
    )

  def testFsxCommand(self):
    service = self._NetAppService()
    issue_command = self.MockIssueCommand(
        {'describe-volumes': [_JsonResponse({})]}
    )
    service._FsxCommand('describe-volumes', f'--volume-ids={_VOLUME_ID}')
    issue_command.func_to_mock.assert_called_once_with(
        util.AWS_PREFIX
        + [
            'fsx',
            'describe-volumes',
            f'--volume-ids={_VOLUME_ID}',
            f'--region={_REGION}',
        ],
        raise_on_failure=False,
        timeout=1800,
    )

  def testFsxCommandFailure(self):
    service = self._NetAppService()
    self.MockIssueCommand({'describe-volumes': [('', 'AccessDenied', 255)]})
    with self.assertRaisesRegex(
        aws_netapp_service.AwsNetAppError, 'AccessDenied'
    ):
      service._FsxCommand('describe-volumes')


if __name__ == '__main__':
  unittest.main()
