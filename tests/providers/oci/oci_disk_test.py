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
"""Tests for perfkitbenchmarker.providers.oci.oci_disk."""

# pylint: disable=protected-access

import json
import unittest

from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.oci import oci_disk
from tests import pkb_common_test_case

_COMPONENT = 'test_component'
_REGION = 'us-ashburn-1'
_AD = 'Uocm:US-ASHBURN-AD-1'
_DEVICES = json.dumps({
    'data': [
        {'name': '/dev/oracleoci/oraclevdb', 'is-available': False},
        {'name': '/dev/oracleoci/oraclevdc', 'is-available': True},
        {'name': '/dev/oracleoci/oraclevdd', 'is-available': True},
    ]
})
_PARAVIRTUALIZED_ATTACHMENT = json.dumps(
    {'data': {'id': 'ocid1.volumeattachment.test'}}
)
_ISCSI_ATTACHMENT = json.dumps({
    'data': {
        'id': 'ocid1.volumeattachment.test',
        'iqn': 'iqn.2015-12.com.oracleiaas:test',
        'ipv4': '169.254.2.2',
        'port': 3260,
    }
})


def _GetArg(cmd, name):
  return cmd[cmd.index(name) + 1]


class OciBlockVolumeTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for OCI block volume lifecycle and attachment."""

  def _create_disk(self, disk_type=oci_disk.BALANCED, disk_size=100):
    spec = disk.BaseDiskSpec(
        _COMPONENT, disk_type=disk_type, disk_size=disk_size
    )
    return oci_disk.OciBlockVolume(spec, 'pkb-vm-data-0', _REGION, _AD)

  def _create_vm(self):
    vm = mock.Mock()
    vm.id = 'ocid1.instance.test'
    vm.name = 'pkb-vm'
    return vm

  @parameterized.named_parameters(
      ('lower_cost', oci_disk.LOWER_COST, 0),
      ('balanced', oci_disk.BALANCED, 10),
      ('higher_performance', oci_disk.HIGHER_PERFORMANCE, 20),
      ('ultra_high_performance', oci_disk.ULTRA_HIGH_PERFORMANCE, 30),
  )
  def test_vpus_per_gb_from_disk_type(self, disk_type, expected_vpus):
    """Tests vpus per gb from disk type."""
    data_disk = self._create_disk(disk_type)
    self.assertEqual(expected_vpus, data_disk.vpus_per_gb)
    self.assertEqual(expected_vpus, data_disk.metadata['vpus_per_gb'])
    self.assertEqual('paravirtualized', data_disk.metadata['attachment_type'])

  @flagsaver.flagsaver(oci_vpus_per_gb=120)
  def test_vpus_per_gb_flag_overrides_disk_type(self):
    """Tests vpus per gb flag overrides disk type."""
    self.assertEqual(120, self._create_disk(oci_disk.LOWER_COST).vpus_per_gb)

  def test_unsupported_disk_type(self):
    """Tests unsupported disk type."""
    with self.assertRaises(errors.Config.InvalidValue):
      self._create_disk('pd-ssd')

  def test_create(self):
    """Tests create."""
    mock_cmd = self.MockIssueCommand({
        'volume create': [('{"data": {"id": "ocid1.volume.test"}}', '', 0)],
        'volume get': [('{"data": {"lifecycle-state": "AVAILABLE"}}', '', 0)],
    })
    data_disk = self._create_disk(oci_disk.HIGHER_PERFORMANCE, 500)

    data_disk.Create()

    self.assertEqual('ocid1.volume.test', data_disk.id)
    self.assertIsNotNone(data_disk.GetCreateTime())
    cmd = mock_cmd.func_to_mock.call_args_list[0][0][0]
    self.assertEqual(['oci', 'bv', 'volume', 'create'], cmd[:4])
    self.assertEqual(_AD, _GetArg(cmd, '--availability-domain'))
    self.assertEqual('500', _GetArg(cmd, '--size-in-gbs'))
    self.assertEqual('20', _GetArg(cmd, '--vpus-per-gb'))
    self.assertEqual('pkb-vm-data-0', _GetArg(cmd, '--display-name'))
    self.assertEqual('AVAILABLE', _GetArg(cmd, '--wait-for-state'))
    self.assertEqual(_REGION, _GetArg(cmd, '--region'))

  def test_delete(self):
    """Tests delete."""
    mock_cmd = self.MockIssueCommand({
        'volume get': [('{"data": {"lifecycle-state": "TERMINATED"}}', '', 0)],
    })
    data_disk = self._create_disk()
    data_disk.id = 'ocid1.volume.test'
    data_disk.created = True

    data_disk.Delete()

    cmd = mock_cmd.GetCommandWithSubstring('volume delete')
    self.assertIn('--volume-id ocid1.volume.test', cmd)
    self.assertIn('--force', cmd)

  def test_exists(self):
    """Tests exists."""
    self.MockIssueCommand({
        'volume get': [('{"data": {"lifecycle-state": "AVAILABLE"}}', '', 0)],
    })
    data_disk = self._create_disk()
    self.assertFalse(data_disk._Exists())
    data_disk.id = 'ocid1.volume.test'
    self.assertTrue(data_disk._Exists())

  def test_attach_paravirtualized(self):
    """Tests attach paravirtualized."""
    mock_cmd = self.MockIssueCommand({
        'device list-instance': [(_DEVICES, '', 0)],
        'volume-attachment attach': [(_PARAVIRTUALIZED_ATTACHMENT, '', 0)],
    })
    data_disk = self._create_disk()
    data_disk.id = 'ocid1.volume.test'
    vm = self._create_vm()

    data_disk.Attach(vm)

    self.assertEqual('/dev/oracleoci/oraclevdc', data_disk.GetDevicePath())
    self.assertEqual('ocid1.volumeattachment.test', data_disk.attachment_id)
    self.assertIs(vm, data_disk.vm)
    self.assertIsNotNone(data_disk.GetAttachTime())
    cmd = mock_cmd.func_to_mock.call_args_list[1][0][0]
    self.assertEqual('ocid1.instance.test', _GetArg(cmd, '--instance-id'))
    self.assertEqual('ocid1.volume.test', _GetArg(cmd, '--volume-id'))
    self.assertEqual('paravirtualized', _GetArg(cmd, '--type'))
    self.assertEqual('/dev/oracleoci/oraclevdc', _GetArg(cmd, '--device'))
    vm.RemoteCommand.assert_not_called()

  def test_attach_with_no_free_devices(self):
    """Tests attach with no free devices."""
    self.MockIssueCommand({
        'device list-instance': [(
            json.dumps({
                'data': [
                    {'name': '/dev/oracleoci/oraclevdb', 'is-available': False}
                ]
            }),
            '',
            0,
        )],
    })
    data_disk = self._create_disk()
    data_disk.id = 'ocid1.volume.test'
    with self.assertRaises(errors.Resource.CreationError):
      data_disk.Attach(self._create_vm())

  @flagsaver.flagsaver(oci_volume_attachment_type='iscsi')
  def test_attach_and_detach_iscsi(self):
    """Tests attach and detach iscsi."""
    mock_cmd = self.MockIssueCommand({
        'device list-instance': [(_DEVICES, '', 0)],
        'volume-attachment attach': [(_ISCSI_ATTACHMENT, '', 0)],
    })
    data_disk = self._create_disk()
    data_disk.id = 'ocid1.volume.test'
    vm = self._create_vm()

    data_disk.Attach(vm)

    self.assertEqual('iqn.2015-12.com.oracleiaas:test', data_disk.iqn)
    login_commands = [c[0][0] for c in vm.RemoteCommand.call_args_list]
    target = '-T iqn.2015-12.com.oracleiaas:test -p 169.254.2.2:3260'
    self.assertEqual(
        [
            f'sudo iscsiadm -m node -o new {target}',
            (
                'sudo iscsiadm -m node -o update -T '
                'iqn.2015-12.com.oracleiaas:test -n node.startup -v automatic'
            ),
            f'sudo iscsiadm -m node {target} -l',
        ],
        login_commands,
    )

    vm.RemoteCommand.reset_mock()
    data_disk.Detach()

    self.assertEqual(
        [
            f'sudo iscsiadm -m node {target} -u',
            f'sudo iscsiadm -m node -o delete {target}',
        ],
        [c[0][0] for c in vm.RemoteCommand.call_args_list],
    )
    detach_cmd = mock_cmd.GetCommandWithSubstring('volume-attachment detach')
    self.assertIn(
        '--volume-attachment-id ocid1.volumeattachment.test', detach_cmd
    )
    self.assertIsNone(data_disk.attachment_id)
    self.assertIsNone(data_disk.vm)

  def test_detach_paravirtualized(self):
    """Tests detach paravirtualized."""
    mock_cmd = self.MockIssueCommand({})
    data_disk = self._create_disk()
    vm = self._create_vm()
    data_disk.vm = vm
    data_disk.attachment_id = 'ocid1.volumeattachment.test'

    data_disk.Detach()

    vm.RemoteCommand.assert_not_called()
    detach_cmd = mock_cmd.GetCommandWithSubstring('volume-attachment detach')
    self.assertIn('--wait-for-state DETACHED', detach_cmd)


if __name__ == '__main__':
  unittest.main()
