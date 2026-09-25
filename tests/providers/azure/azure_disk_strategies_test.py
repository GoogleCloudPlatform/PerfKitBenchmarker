# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for fallback and error handling in azure_disk_strategies."""

import unittest
from absl import flags
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.azure import azure_disk
from perfkitbenchmarker.providers.azure import azure_disk_strategies
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AzureSetUpRemoteDiskStrategyTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.Mock()
    self.vm.create_disk_strategy = mock.Mock()

    # Create fake disk specs
    self.disk_spec = mock.Mock()
    self.disk_spec.disk_type = azure_disk.PREMIUM_STORAGE_V2
    self.disk_spec.num_striped_disks = 1
    self.disk_spec.mount_point = '/scratch'
    self.disk_specs = [self.disk_spec]

    # Create fake AzureDisk
    self.mock_disk = mock.Mock(spec=azure_disk.AzureDisk)
    self.mock_disk.disk_type = azure_disk.PREMIUM_STORAGE_V2
    self.mock_disk.created = False
    self.mock_disk.metadata = {'type': azure_disk.PREMIUM_STORAGE_V2}

    self.vm.create_disk_strategy.remote_disk_groups = [[self.mock_disk]]
    self.vm.create_disk_strategy.DiskCreatedOnVMCreation.return_value = False

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskFallback(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    # First call to RunParallelThreads fails with SkuNotAvailable for
    # PremiumV2_LRS.
    # Second call succeeds.
    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException('SkuNotAvailable PremiumV2_LRS'),
        None,
    ]

    strategy.SetUpDisk()

    # Verify fallback happened
    self.assertEqual(self.disk_spec.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(self.mock_disk.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(
        self.mock_disk.metadata['type'], azure_disk.PREMIUM_STORAGE
    )

    # Verify RunParallelThreads was called twice
    self.assertEqual(mock_run_parallel.call_count, 2)

    # Verify AttachDisks and PrepareScratchDisk were called (in the second run)
    mock_attach.assert_called_once()
    mock_prepare_strategy.return_value.PrepareScratchDisk.assert_called_once()

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskFallbackWithCleanup(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    disk1 = mock.Mock(spec=azure_disk.AzureDisk)
    disk1.disk_type = azure_disk.PREMIUM_STORAGE_V2
    disk1.created = True  # Succeeded
    disk1.metadata = {'type': azure_disk.PREMIUM_STORAGE_V2}

    disk2 = mock.Mock(spec=azure_disk.AzureDisk)
    disk2.disk_type = azure_disk.PREMIUM_STORAGE_V2
    disk2.created = False  # Failed
    disk2.metadata = {'type': azure_disk.PREMIUM_STORAGE_V2}

    self.vm.create_disk_strategy.remote_disk_groups = [[disk1], [disk2]]

    spec1 = mock.Mock()
    spec1.disk_type = azure_disk.PREMIUM_STORAGE_V2
    spec1.num_striped_disks = 1
    spec1.mount_point = '/scratch0'

    spec2 = mock.Mock()
    spec2.disk_type = azure_disk.PREMIUM_STORAGE_V2
    spec2.num_striped_disks = 1
    spec2.mount_point = '/scratch1'

    self.disk_specs = [spec1, spec2]

    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException('SkuNotAvailable PremiumV2_LRS'),
        None,
    ]

    strategy.SetUpDisk()

    # Verify disk1 was deleted during cleanup
    disk1.Delete.assert_called_once()
    # disk2 was not created, so it should not be deleted
    disk2.Delete.assert_not_called()

    # Verify fallback happened for both
    self.assertEqual(spec1.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(spec2.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(disk1.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(disk2.disk_type, azure_disk.PREMIUM_STORAGE)

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskFallbackFails(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    # First call fails with PremiumV2 SkuNotAvailable
    # Second call (fallback) fails with Premium_LRS SkuNotAvailable
    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException('SkuNotAvailable PremiumV2_LRS'),
        errors.VmUtil.ThreadException('SkuNotAvailable Premium_LRS'),
    ]

    with self.assertRaises(errors.Benchmarks.UnsupportedConfigError) as cm:
      strategy.SetUpDisk()

    self.assertIn(
        'Failed to create fallback disk Premium_LRS', str(cm.exception)
    )
    self.assertEqual(self.disk_spec.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(mock_run_parallel.call_count, 2)
    mock_attach.assert_not_called()

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskStandardFails(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    self.disk_spec.disk_type = azure_disk.STANDARD_SSD_LRS
    self.mock_disk.disk_type = azure_disk.STANDARD_SSD_LRS
    self.mock_disk.metadata = {'type': azure_disk.STANDARD_SSD_LRS}

    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException('SkuNotAvailable StandardSSD_LRS')
    ]

    with self.assertRaises(errors.Benchmarks.UnsupportedConfigError) as cm:
      strategy.SetUpDisk()

    self.assertIn('Disk SKU is not available', str(cm.exception))
    self.assertEqual(mock_run_parallel.call_count, 1)
    mock_attach.assert_not_called()

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskOtherError(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException('Some other error')
    ]

    with self.assertRaises(errors.VmUtil.ThreadException):
      strategy.SetUpDisk()

    self.assertEqual(mock_run_parallel.call_count, 1)
    mock_attach.assert_not_called()

  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.background_tasks.RunParallelThreads'
  )
  @mock.patch.object(
      azure_disk_strategies.AzureSetUpRemoteDiskStrategy, 'AttachDisks'
  )
  @mock.patch(
      'perfkitbenchmarker.providers.azure.azure_disk_strategies.AzurePrepareScratchDiskStrategy'
  )
  def testSetUpDiskFallbackZoneError(
      self, mock_prepare_strategy, mock_attach, mock_run_parallel
  ):
    strategy = azure_disk_strategies.AzureSetUpRemoteDiskStrategy(
        self.vm, self.disk_specs
    )

    # First call fails with Availability Zone error for PremiumV2_LRS
    # Second call succeeds
    mock_run_parallel.side_effect = [
        errors.VmUtil.ThreadException(
            'Managed disks with PremiumV2_LRS storage account type can be used'
            ' only with Virtual Machines in an Availability Zone.'
        ),
        None,
    ]

    strategy.SetUpDisk()

    # Verify fallback happened
    self.assertEqual(self.disk_spec.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(self.mock_disk.disk_type, azure_disk.PREMIUM_STORAGE)
    self.assertEqual(
        self.mock_disk.metadata['type'], azure_disk.PREMIUM_STORAGE
    )

    # Verify RunParallelThreads was called twice
    self.assertEqual(mock_run_parallel.call_count, 2)

    # Verify AttachDisks and PrepareScratchDisk were called
    mock_attach.assert_called_once()
    mock_prepare_strategy.return_value.PrepareScratchDisk.assert_called_once()


if __name__ == '__main__':
  unittest.main()
