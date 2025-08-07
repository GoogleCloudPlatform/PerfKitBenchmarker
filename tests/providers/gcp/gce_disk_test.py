# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for Gce disk."""

import builtins
import contextlib
import json
import unittest
from absl import flags
import mock
from perfkitbenchmarker import context
from perfkitbenchmarker import disk
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gce_disk
from perfkitbenchmarker.providers.gcp import gce_nfs_service
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_windows_virtual_machine
from perfkitbenchmarker.providers.gcp import util as gcp_utils
from tests import pkb_common_test_case


FLAGS = flags.FLAGS

_BENCHMARK_NAME = 'name'
_BENCHMARK_UID = 'benchmark_uid'
_COMPONENT = 'test_component'
_FLAGS = None

_FAKE_INSTANCE_METADATA = {
    'id': '123456',
    'networkInterfaces': [{
        'accessConfigs': [{'natIP': '1.2.3.4'}],
        'networkIP': '1.2.3.4',
    }],
}
_FAKE_DISK_METADATA = {
    'id': '123456',
    'kind': 'compute#disk',
    'name': 'fakedisk',
    'sizeGb': '10',
    'sourceImage': '',
    'type': 'pd-standard',
}


_COMPUTE_DESCRIBE_RUNNING = r"""{
  "id": "758013403901965936",
  "networkInterfaces": [
    {
      "accessConfigs": [
        {
          "natIP": "35.227.176.232"
        }
      ],
      "networkIP": "10.138.0.113"
    }
  ],
  "status": "RUNNING"
}
"""

_DISK_JSON = """{
  "architecture": "X86_64",
  "creationTimestamp": "2023-10-20T11:32:32.497-07:00",
  "guestOsFeatures": [
    {
      "type": "VIRTIO_SCSI_MULTIQUEUE"
    },
    {
      "type": "SEV_CAPABLE"
    },
    {
      "type": "SEV_SNP_CAPABLE"
    },
    {
      "type": "SEV_LIVE_MIGRATABLE"
    },
    {
      "type": "UEFI_COMPATIBLE"
    },
    {
      "type": "GVNIC"
    }
  ],
  "id": "123",
  "kind": "compute#disk",
  "labelFingerprint": "123=",
  "lastAttachTimestamp": "2023-10-20T11:32:32.497-07:00",
  "licenseCodes": [
    "123"
  ],
  "licenses": [
    "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/licenses/ubuntu-2004-lts"
  ],
  "name": "pkb-9ae781cd16-0",
  "physicalBlockSizeBytes": "4096",
  "selfLink": "https://a",
  "sizeGb": "10",
  "sourceImage": "https://a",
  "sourceImageId": "6522713990305763548",
  "status": "READY",
  "type": "https://www.googleapis.com/compute/v1/projects/a/zones/europe-west4-b/diskTypes/pd-standard",
  "users": [
    "https://www.googleapis.com/compute/v1/projects/a/zones/europe-west4-b/instances/pkb-9ae781cd16-0"
  ],
  "zone": "https://www.googleapis.com/compute/v1/projects/a/zones/europe-west4-b"
}"""


@contextlib.contextmanager
def PatchCriticalObjects(retvals=None):
  """A context manager that patches a few critical objects with mocks."""

  def ReturnVal(*unused_arg, **unused_kwargs):
    del unused_arg
    del unused_kwargs
    return ('', '', 0) if retvals is None else retvals.pop(0)

  with mock.patch(
      vm_util.__name__ + '.IssueCommand', side_effect=ReturnVal
  ) as issue_command, mock.patch(builtins.__name__ + '.open'), mock.patch(
      vm_util.__name__ + '.NamedTemporaryFile'
  ):
    yield issue_command


class GCEDiskTest(pkb_common_test_case.PkbCommonTestCase):

  def GetDiskSpec(self, mount_point):
    return disk.BaseDiskSpec(_COMPONENT, mount_point=mount_point)

  def _MockGcpUtils(self, function_name, return_value=None, side_effect=None):
    return self.enter_context(
        mock.patch.object(
            gcp_utils,
            function_name,
            autospec=True,
            return_value=return_value,
            side_effect=side_effect,
        )
    )

  def setUp(self):
    """ """
    super().setUp()
    p = mock.patch(
        gce_virtual_machine.__name__ + '.gce_network.GceNetwork.GetNetwork'
    )
    self.mock_get_network = p.start()
    self.addCleanup(p.stop)
    p = mock.patch(
        gce_virtual_machine.__name__ + '.gce_network.GceFirewall.GetFirewall'
    )
    FLAGS.run_uri = 'aaaaaa'
    self.mock_get_firewall = p.start()
    self.addCleanup(p.stop)

    get_tmp_dir_mock = mock.patch(
        vm_util.__name__ + '.GetTempDir', return_value='TempDir'
    )
    get_tmp_dir_mock.start()
    self.addCleanup(get_tmp_dir_mock.stop)
    vm_spec = gce_virtual_machine.GceVmSpec('test_component', project='test')
    self.linux_vm = gce_virtual_machine.Ubuntu2004BasedGceVirtualMachine(
        vm_spec
    )

    self.linux_vm.HasPackage = mock.Mock(return_value=False)
    self.linux_vm.name = 'test_vm'
    self.linux_vm.machine_type = 'n1-standard-4'
    self.linux_vm.GetConnectionIp = mock.MagicMock(return_value='1.1.1.1')
    disk_spec = self.GetDiskSpec(mount_point='/mountpoint')
    self.linux_vm.SetDiskSpec(disk_spec, 2)
    self.linux_vm.create_disk_strategy.GetSetupDiskStrategy().WaitForRemoteDisksToVisibleFromVm = mock.MagicMock(
        return_value=12
    )
    self.mock_cmd = mock.Mock()
    self.windows_vm = virtual_machine.GetVmClass(
        'GCP', os_types.WINDOWS2022_DESKTOP
    )(vm_spec)
    gce_windows_virtual_machine.WindowsGceVirtualMachine(vm_spec)
    self.windows_vm.HasPackage = mock.Mock(return_value=False)
    self.mock_cmd = mock.Mock()
    self._MockGcpUtils('GetRegionFromZone', return_value='region_1')

  def CompareCallArgs(self, expected_returns, actual_returns):
    for i, command in enumerate(actual_returns):
      for expected_command in expected_returns[i]:
        self.assertIn(expected_command, command[0][0])


class GCERAMDiskTest(GCEDiskTest):

  def testRamDiskOnLinux(self):
    spec = disk.GetDiskSpecClass('GCP', disk.RAM)(_COMPONENT)
    spec.disk_type = disk.RAM
    spec.disk_size = 10
    spec.mount_point = '/scratch'
    spec.num_striped_disks = 1
    self.linux_vm.GetConnectionIp = mock.MagicMock(return_value='1.1.1.1')
    fake_rets = [('stdout', 'stderr', 0)]
    with PatchCriticalObjects(fake_rets) as issue_command:
      self.linux_vm.SetDiskSpec(spec, 1)
      self.linux_vm.SetupAllScratchDisks()
      self.assertIn(
          'sudo mkdir -p /scratch;sudo mount -t tmpfs -o size=10g tmpfs'
          ' /scratch;sudo chown -R $USER:$USER /scratch;',
          issue_command.call_args[0][0],
      )
      self.assertIsNone(self.linux_vm.scratch_disks[0].GetDevicePath())


class GCEPDDiskTest(GCEDiskTest):

  def testPdStandardOnLinux(self):
    spec = disk.GetDiskSpecClass('GCP', gce_disk.PD_STANDARD)(_COMPONENT)
    spec.disk_type = gce_disk.PD_STANDARD
    spec.disk_size = 10
    spec.mount_point = '/scratch'
    spec.num_striped_disks = 1
    spec.create_with_vm = True
    fake_rets = [
        ('/dev/nvme0n1', 'stderr', 0),
        ('0', 'stderr', 0),
        ('stdout', 'stderr', 0),
        ('stdout', 'stderr', 0),
        ('stdout', 'stderr', 0),
    ]
    with PatchCriticalObjects(fake_rets) as issue_command:
      self.linux_vm.SetDiskSpec(spec, 1)
      self.linux_vm.create_disk_strategy.GetSetupDiskStrategy().AttachDisks = (
          mock.MagicMock()
      )
      self.linux_vm.SetupAllScratchDisks()
      expected_commands = [
          ['readlink -f /dev/disk/by-id/google-test_vm-data-0-0'],
          ['mount | grep "/dev/nvme0n1 on /scratch" | wc -l'],
          [
              '[[ -d /mnt ]] && sudo umount /mnt; sudo mke2fs -F -E'
              ' lazy_itable_init=0,discard -O ^has_journal -t ext4 -b 4096'
              ' /dev/disk/by-id/google-test_vm-data-0-0'
          ],
          [
              'sudo mkdir -p /scratch;sudo mount -o discard'
              ' /dev/disk/by-id/google-test_vm-data-0-0 /scratch && sudo'
              ' chown $USER:$USER /scratch;'
          ],
          [
              'echo "/dev/disk/by-id/google-test_vm-data-0-0 /scratch ext4'
              ' defaults" | sudo tee -a /etc/fstab'
          ],
      ]
      self.CompareCallArgs(expected_commands, issue_command.call_args_list)
      self.assertEqual(
          self.linux_vm.scratch_disks[0].GetDevicePath(),
          '/dev/disk/by-id/google-test_vm-data-0-0',
      )

  def testPdExtremeOnCreateWithVMLinux(self):
    spec = disk.GetDiskSpecClass('GCP', gce_disk.PD_EXTREME)(_COMPONENT)
    spec.disk_type = gce_disk.PD_EXTREME
    spec.disk_size = 10
    spec.mount_point = '/scratch'
    spec.num_striped_disks = 1
    spec.create_with_vm = False
    fake_rets = [
        ('stdout', 'stderr', 0),
        (_DISK_JSON, 'stderr', 0),  # Exists command
        (_DISK_JSON, 'stderr', 0),  # Describe command
        ('', '', 0),
        ('/dev/nvme0n1', 'stderr', 0),
        ('0', 'stderr', 0),
        ('', 'stderr', 0),
        ('', 'stderr', 0),
        ('', 'stderr', 0),
        ('', 'stderr', 0),
        ('', 'stderr', 0),
    ]
    with PatchCriticalObjects(fake_rets) as issue_command:
      self.linux_vm.SetDiskSpec(spec, 1)
      self.linux_vm.create_disk_strategy.GetSetupDiskStrategy().WaitForRemoteDisksToVisibleFromVm = mock.MagicMock(
          return_value=12
      )
      self.linux_vm.SetupAllScratchDisks()
      expected_commands = [
          [
              'gcloud',
              'compute',
              'disks',
              'create',
              'test_vm-data-0-0',
              '--format',
              'json',
              '--labels',
              '',
              '--project',
              'test',
              '--quiet',
              '--size',
              '10',
              '--type',
              'pd-extreme',
          ],
          [
              'gcloud',
              'compute',
              'disks',
              'describe',
              'test_vm-data-0-0',
              '--format',
              'json',
              '--project',
              'test',
              '--quiet',
          ],
          [
              'gcloud',
              'compute',
              'disks',
              'describe',
              'test_vm-data-0-0',
              '--format',
              'json',
              '--project',
              'test',
              '--quiet',
          ],
          [
              'gcloud',
              'compute',
              'instances',
              'attach-disk',
              'test_vm',
              '--device-name',
              'test_vm-data-0-0',
              '--disk',
              'test_vm-data-0-0',
              '--format',
              'json',
              '--project',
              'test',
              '--quiet',
          ],
          ['readlink -f /dev/disk/by-id/google-test_vm-data-0-0'],
          ['mount | grep "/dev/nvme0n1 on /scratch" | wc -l'],
          [
              '[[ -d /mnt ]] && sudo umount /mnt; sudo mke2fs -F -E'
              ' lazy_itable_init=0,discard -O ^has_journal -t ext4 -b 4096'
              ' /dev/disk/by-id/google-test_vm-data-0-0'
          ],
          [
              'sudo mkdir -p /scratch;sudo mount -o discard'
              ' /dev/disk/by-id/google-test_vm-data-0-0 /scratch && sudo'
              ' chown $USER:$USER /scratch;'
          ],
          [
              'echo "/dev/disk/by-id/google-test_vm-data-0-0 /scratch ext4'
              ' defaults" | sudo tee -a /etc/fstab'
          ],
      ]
      self.CompareCallArgs(expected_commands, issue_command.call_args_list)

      self.assertEqual(
          self.linux_vm.scratch_disks[0].GetDevicePath(),
          '/dev/disk/by-id/google-test_vm-data-0-0',
      )


class GCENFSDiskTest(GCEDiskTest):

  def _CreatePatched(self, module, method_name):
    patcher = mock.patch.object(module, method_name)
    mock_method = patcher.start()
    self.addCleanup(patcher.stop)
    return mock_method

  def _DescribeResult(self, tier='STANDARD'):
    return {
        'createTime': '2018-05-04T21:38:49.862374Z',
        'name': 'projects/foo/locations/asia-east1-a/instances/nfs-xxxxxxxx',
        'networks': [{
            'ipAddresses': ['10.198.13.2'],
            'network': 'default2',
            'reservedIpRange': '10.198.13.0/29',
        }],
        'state': 'READY',
        'tier': tier,
        'volumes': [{'capacityGb': '1024', 'name': 'vol0'}],
    }

  def testNFSDiskOnLinux(self):
    spec = disk.GetDiskSpecClass('GCP', disk.NFS)(_COMPONENT)
    spec.disk_type = disk.NFS
    spec.disk_size = 10
    spec.mount_point = '/scratch'
    spec.num_striped_disks = 1
    spec.nfs_version = '1'
    spec.nfs_timeout_hard = 1
    spec.nfs_rsize = 10
    spec.nfs_wsize = 11
    spec.nfs_timeout = 10
    spec.nfs_retries = 1
    spec.nfs_nconnect = 3
    spec.nfs_managed = False
    spec.nfs_directory = '/scratch'
    nfs_service = gce_nfs_service.GceNfsService(spec, 'test-zone')
    bm_spec = mock.Mock()
    bm_spec.nfs_service = nfs_service
    get_spec = self._CreatePatched(context, 'GetThreadBenchmarkSpec')
    get_spec.return_value = bm_spec
    self.linux_vm._GetNfsService = mock.MagicMock(return_value=nfs_service)
    self.linux_vm.GetConnectionIp = mock.MagicMock(return_value='1.1.1.1')
    self.linux_vm.SetDiskSpec(spec, 1)
    fake_rets = [
        ('stdout', 'stderr', 0),
        ('stdout', 'stderr', 0),
        (json.dumps(self._DescribeResult()), '', 0),
        ('stdout', 'stderr', 0),
        ('stdout', 'stderr', 0),
        ('stdout', 'stderr', 0),
    ]

    with PatchCriticalObjects(fake_rets) as issue_command:
      self.linux_vm.create_disk_strategy.GetSetupDiskStrategy().WaitForRemoteDisksToVisibleFromVm = mock.MagicMock(
          return_value=12
      )
      self.linux_vm.SetupAllScratchDisks()
      expected_commands = [
          ['sudo apt-get update'],
          [
              "sudo DEBIAN_FRONTEND='noninteractive' /usr/bin/apt-get -y"
              ' install nfs-common'
          ],
          [
              'gcloud',
              '--quiet',
              '--format',
              'json',
              'filestore',
              'instances',
              'describe',
              'nfs-aaaaaa',
              '--location',
              'test-zone',
          ],
          [
              'sudo mkdir -p /scratch;sudo mount -t nfs -o'
              ' hard,nconnect=3,nfsvers=1,retrans=1,rsize=10,timeo=100,wsize=11'
              ' 10.198.13.2:/vol0 /scratch && sudo chown $USER:$USER /scratch;'
          ],
          [
              'echo "10.198.13.2:/vol0 /scratch nfs'
              ' hard,nconnect=3,nfsvers=1,retrans=1,rsize=10,timeo=100,wsize=11"'
              ' | sudo tee -a /etc/fstab'
          ],
      ]
      self.CompareCallArgs(expected_commands, issue_command.call_args_list)
      self.assertEqual(
          self.linux_vm.scratch_disks[0].GetDevicePath(), '10.198.13.2:/vol0'
      )


if __name__ == '__main__':
  unittest.main()
