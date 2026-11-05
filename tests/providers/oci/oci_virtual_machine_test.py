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
"""Tests for perfkitbenchmarker.providers.oci.oci_virtual_machine."""

# pylint: disable=protected-access

import base64
import json
import time
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import disk
from perfkitbenchmarker import disk_strategies
from perfkitbenchmarker import errors
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.providers.oci import oci_disk
from perfkitbenchmarker.providers.oci import oci_virtual_machine
from perfkitbenchmarker.providers.oci import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'
_SUBNET_ID = 'ocid1.subnet.test'
_INSTANCE_ID = 'ocid1.instance.test'
_AD = 'Uocm:US-ASHBURN-AD-1'
_PUBLIC_KEY = 'ssh-rsa AAAAtestkey perfkit@test'


class TestOciVirtualMachine(
    pkb_common_test_case.TestOsMixin, oci_virtual_machine.OciVirtualMachine
):
  IMAGE_OPERATING_SYSTEM = 'Oracle Linux'
  IMAGE_OPERATING_SYSTEM_VERSION = '9'


def _GetArg(cmd, name):
  return cmd[cmd.index(name) + 1]


def _State(state):
  return (json.dumps({'data': {'lifecycle-state': state}}), '', 0)


class OciVmSpecTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for decoding OciVmSpec from config and flags."""

  def test_defaults(self):
    """Tests defaults."""
    spec = oci_virtual_machine.OciVmSpec(
        _COMPONENT, machine_type='VM.Standard.E5.Flex', zone='us-ashburn-1'
    )
    self.assertIsNone(spec.ocpus)
    self.assertIsNone(spec.memory_in_gbs)
    self.assertIsNone(spec.availability_domain)
    self.assertIsNone(spec.fault_domain)
    self.assertFalse(spec.preemptible)

  def test_config_values(self):
    """Tests config values."""
    spec = oci_virtual_machine.OciVmSpec(
        _COMPONENT,
        machine_type='VM.Standard.E5.Flex',
        zone='us-ashburn-1',
        ocpus=4,
        memory_in_gbs=64,
        availability_domain=_AD,
        fault_domain='FAULT-DOMAIN-2',
        preemptible=True,
    )
    self.assertEqual(4, spec.ocpus)
    self.assertEqual(64, spec.memory_in_gbs)
    self.assertEqual(_AD, spec.availability_domain)
    self.assertEqual('FAULT-DOMAIN-2', spec.fault_domain)
    self.assertTrue(spec.preemptible)

  def test_invalid_ocpus(self):
    """Tests invalid ocpus."""
    with self.assertRaises(errors.Config.InvalidValue):
      oci_virtual_machine.OciVmSpec(
          _COMPONENT,
          machine_type='VM.Standard.E5.Flex',
          zone='us-ashburn-1',
          ocpus=0,
      )

  @flagsaver.flagsaver
  def test_flags_override_config(self):
    """Tests flags override config."""
    FLAGS['oci_ocpus'].parse(8)
    FLAGS['oci_memory_in_gbs'].parse(128)
    FLAGS['oci_availability_domain'].parse('Uocm:US-ASHBURN-AD-3')
    FLAGS['oci_fault_domain'].parse('FAULT-DOMAIN-3')
    FLAGS['oci_preemptible'].parse(True)
    spec = oci_virtual_machine.OciVmSpec(
        _COMPONENT,
        flag_values=FLAGS,
        machine_type='VM.Standard.E5.Flex',
        zone='us-ashburn-1',
        ocpus=2,
        preemptible=False,
    )
    self.assertEqual(8, spec.ocpus)
    self.assertEqual(128, spec.memory_in_gbs)
    self.assertEqual('Uocm:US-ASHBURN-AD-3', spec.availability_domain)
    self.assertEqual('FAULT-DOMAIN-3', spec.fault_domain)
    self.assertTrue(spec.preemptible)


class OciVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the OCI virtual machine lifecycle."""

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'abc123'
    # Sets the thread's benchmark spec, which holds networks and firewalls.
    pkb_common_test_case.CreateBenchmarkSpecFromYaml()
    self.public_key_file = self.create_tempfile(content=_PUBLIC_KEY + '\n')
    self.enter_context(mock.patch.object(time, 'sleep'))

  def _create_vm(
      self, machine_type='VM.Standard.E5.Flex', zone='us-ashburn-1', **kwargs
  ):
    spec = oci_virtual_machine.OciVmSpec(
        _COMPONENT, machine_type=machine_type, zone=zone, **kwargs
    )
    vm = TestOciVirtualMachine(spec)
    vm.ssh_public_key = self.public_key_file.full_path
    vm.network.subnet.id = _SUBNET_ID
    vm.availability_domain = vm.availability_domain or _AD
    vm.image = vm.image or 'ocid1.image.test'
    return vm

  def _get_launch_command(self, mock_cmd):
    for call in mock_cmd.func_to_mock.call_args_list:
      cmd = call[0][0]
      if cmd[1:4] == ['compute', 'instance', 'launch']:
        return cmd
    self.fail('No launch command issued.')

  def test_init(self):
    """Tests init."""
    vm = self._create_vm(zone='us-ashburn-1-ad-2')
    self.assertEqual('us-ashburn-1', vm.region)
    self.assertTrue(vm.is_flex_shape)
    self.assertEqual(1, vm.ocpus)
    self.assertEqual(
        oci_virtual_machine.DEFAULT_BOOT_DISK_SIZE_GB, vm.boot_disk_size
    )
    self.assertFalse(vm.preemptible)
    self.assertEqual('us-ashburn-1', vm.network.region)
    self.assertIsNotNone(vm.firewall)

  def test_init_fixed_shape(self):
    """Tests init fixed shape."""
    vm = self._create_vm(machine_type='VM.Standard3.8')
    self.assertFalse(vm.is_flex_shape)
    self.assertIsNone(vm.ocpus)

  def test_vms_in_same_region_share_network(self):
    """Tests vms in same region share network."""
    vm1 = self._create_vm(zone='us-ashburn-1-ad-1')
    vm2 = self._create_vm(zone='us-ashburn-1-ad-2')
    self.assertIs(vm1.network, vm2.network)
    self.assertIs(vm1.firewall, vm2.firewall)

  @parameterized.named_parameters(
      ('ubuntu2204', os_types.UBUNTU2204, 'Canonical Ubuntu', '22.04'),
      ('ubuntu2404', os_types.UBUNTU2404, 'Canonical Ubuntu', '24.04'),
      ('ol8', os_types.OL8, 'Oracle Linux', '8'),
      ('ol9', os_types.OL9, 'Oracle Linux', '9'),
  )
  def test_os_types_are_registered(self, os_type, operating_system, version):
    """Tests os types are registered."""
    vm_class = virtual_machine.GetVmClass('OCI', os_type)
    self.assertTrue(issubclass(vm_class, oci_virtual_machine.OciVirtualMachine))
    self.assertEqual(operating_system, vm_class.IMAGE_OPERATING_SYSTEM)
    self.assertEqual(version, vm_class.IMAGE_OPERATING_SYSTEM_VERSION)

  def test_create_dependencies_resolves_availability_domain_and_image(self):
    """Tests create dependencies resolves availability domain and image."""
    get_ad = self.enter_context(
        mock.patch.object(util, 'GetAvailabilityDomain', return_value=_AD)
    )
    get_image = self.enter_context(
        mock.patch.object(util, 'GetImageId', return_value='ocid1.image.ol9')
    )
    vm = self._create_vm(zone='us-ashburn-1-ad-1')
    vm.availability_domain = None
    vm.image = None

    vm._CreateDependencies()

    self.assertEqual(_AD, vm.availability_domain)
    self.assertEqual('ocid1.image.ol9', vm.image)
    get_ad.assert_called_once_with('us-ashburn-1-ad-1')
    get_image.assert_called_once_with(
        'Oracle Linux', '9', 'VM.Standard.E5.Flex', 'us-ashburn-1'
    )

  def test_create_dependencies_looks_up_image_by_name(self):
    """Tests create dependencies looks up image by name."""
    get_image = self.enter_context(
        mock.patch.object(
            util, 'GetImageIdFromName', return_value='ocid1.image.custom'
        )
    )
    vm = self._create_vm()
    vm.image = 'my-custom-image'

    vm._CreateDependencies()

    self.assertEqual('ocid1.image.custom', vm.image)
    get_image.assert_called_once_with(
        'my-custom-image', 'VM.Standard.E5.Flex', 'us-ashburn-1'
    )

  def test_create_dependencies_keeps_image_ocid(self):
    """Tests create dependencies keeps image ocid."""
    get_image = self.enter_context(
        mock.patch.object(util, 'GetImageIdFromName')
    )
    vm = self._create_vm()
    vm.image = 'ocid1.image.oc1.iad.test'

    vm._CreateDependencies()

    self.assertEqual('ocid1.image.oc1.iad.test', vm.image)
    get_image.assert_not_called()

  def test_create(self):
    """Tests create."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [(
            json.dumps({'data': {'id': _INSTANCE_ID}}),
            '',
            0,
        )],
    })
    vm = self._create_vm(ocpus=2, memory_in_gbs=32)

    vm._Create()

    self.assertEqual(_INSTANCE_ID, vm.id)
    cmd = self._get_launch_command(mock_cmd)
    self.assertEqual(_AD, _GetArg(cmd, '--availability-domain'))
    self.assertRegex(vm.name, r'^pkb-abc123-\d+$')
    self.assertEqual(vm.name, _GetArg(cmd, '--display-name'))
    self.assertEqual(vm.name, _GetArg(cmd, '--hostname-label'))
    self.assertEqual('VM.Standard.E5.Flex', _GetArg(cmd, '--shape'))
    self.assertEqual('ocid1.image.test', _GetArg(cmd, '--image-id'))
    self.assertEqual(_SUBNET_ID, _GetArg(cmd, '--subnet-id'))
    self.assertEqual('true', _GetArg(cmd, '--assign-public-ip'))
    self.assertEqual('50', _GetArg(cmd, '--boot-volume-size-in-gbs'))
    self.assertEqual('us-ashburn-1', _GetArg(cmd, '--region'))
    self.assertEqual(
        {'ocpus': 2, 'memoryInGBs': 32},
        json.loads(_GetArg(cmd, '--shape-config')),
    )
    self.assertNotIn('--fault-domain', cmd)
    self.assertNotIn('--preemptible-instance-config', cmd)

    metadata = json.loads(_GetArg(cmd, '--metadata'))
    self.assertEqual(_PUBLIC_KEY, metadata['ssh_authorized_keys'])
    user_data = base64.b64decode(metadata['user_data']).decode()
    self.assertTrue(user_data.startswith('#cloud-config'))
    self.assertIn(f'- name: {vm.user_name}', user_data)
    self.assertIn(f'- {_PUBLIC_KEY}', user_data)

  def test_create_flex_shape_without_memory(self):
    """Tests create flex shape without memory."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [('{"data": {"id": "i"}}', '', 0)],
    })
    self._create_vm()._Create()
    cmd = self._get_launch_command(mock_cmd)
    self.assertEqual({'ocpus': 1}, json.loads(_GetArg(cmd, '--shape-config')))

  def test_create_fixed_shape_has_no_shape_config(self):
    """Tests create fixed shape has no shape config."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [('{"data": {"id": "i"}}', '', 0)],
    })
    self._create_vm(machine_type='VM.Standard3.8')._Create()
    self.assertNotIn('--shape-config', self._get_launch_command(mock_cmd))

  def test_create_preemptible_in_fault_domain(self):
    """Tests create preemptible in fault domain."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [('{"data": {"id": "i"}}', '', 0)],
    })
    self._create_vm(preemptible=True, fault_domain='FAULT-DOMAIN-2')._Create()
    cmd = self._get_launch_command(mock_cmd)
    self.assertEqual('FAULT-DOMAIN-2', _GetArg(cmd, '--fault-domain'))
    self.assertEqual(
        {
            'preemptionAction': {
                'type': 'TERMINATE',
                'preserveBootVolume': False,
            }
        },
        json.loads(_GetArg(cmd, '--preemptible-instance-config')),
    )

  @flagsaver.flagsaver(oci_compartment_id='ocid1.compartment.oc1..test')
  def test_create_in_compartment(self):
    """Tests create in compartment."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [('{"data": {"id": "i"}}', '', 0)],
    })
    self._create_vm()._Create()
    self.assertEqual(
        'ocid1.compartment.oc1..test',
        _GetArg(self._get_launch_command(mock_cmd), '--compartment-id'),
    )

  def test_create_without_external_ip(self):
    """Tests create without external ip."""
    mock_cmd = self.MockIssueCommand({
        'instance launch': [('{"data": {"id": "i"}}', '', 0)],
    })
    vm = self._create_vm()
    vm.assign_external_ip = False
    vm._Create()
    self.assertEqual(
        'false',
        _GetArg(self._get_launch_command(mock_cmd), '--assign-public-ip'),
    )

  @parameterized.named_parameters(
      (
          'capacity',
          'ServiceError: InternalError Out of host capacity.',
          errors.Benchmarks.InsufficientCapacityCloudFailure,
      ),
      (
          'quota',
          'ServiceError: LimitExceeded for standard-e5-core-count',
          errors.Benchmarks.QuotaFailure,
      ),
      (
          'other',
          'ServiceError: InvalidParameter',
          errors.Resource.CreationError,
      ),
  )
  def test_create_failure(self, stderr, expected_error):
    """Tests create failure."""
    self.MockIssueCommand({'instance launch': [('', stderr, 1)]})
    with self.assertRaises(expected_error):
      self._create_vm()._Create()

  def test_wait_until_running(self):
    """Tests wait until running."""
    mock_cmd = self.MockIssueCommand({
        'instance get': [_State('PROVISIONING'), _State('RUNNING')],
    })
    vm = self._create_vm()
    vm.id = _INSTANCE_ID

    vm._WaitUntilRunning()

    self.assertEqual(2, mock_cmd.progress_through_calls['instance get'])

  def test_wait_until_running_terminated(self):
    """Tests wait until running terminated."""
    self.MockIssueCommand({'instance get': [_State('TERMINATED')]})
    vm = self._create_vm()
    vm.id = _INSTANCE_ID
    with self.assertRaises(errors.Resource.CreationError):
      vm._WaitUntilRunning()

  def test_post_create(self):
    """Tests post create."""
    self.MockIssueCommand({
        'list-vnics': [(
            json.dumps(
                {'data': [{'private-ip': '10.0.0.5', 'public-ip': '129.1.2.3'}]}
            ),
            '',
            0,
        )],
    })
    vm = self._create_vm()
    vm.id = _INSTANCE_ID

    vm._PostCreate()

    self.assertEqual('10.0.0.5', vm.internal_ip)
    self.assertEqual(['10.0.0.5'], vm.internal_ips)
    self.assertEqual('129.1.2.3', vm.ip_address)
    self.assertEqual(['129.1.2.3'], vm.ip_addresses)

  def test_post_create_without_public_ip(self):
    """Tests post create without public ip."""
    self.MockIssueCommand({
        'list-vnics': [('{"data": [{"private-ip": "10.0.0.5"}]}', '', 0)],
    })
    vm = self._create_vm()
    vm.id = _INSTANCE_ID
    vm._PostCreate()
    self.assertEqual('10.0.0.5', vm.internal_ip)
    self.assertIsNone(vm.ip_address)

  @parameterized.named_parameters(
      ('running', _State('RUNNING'), True),
      ('stopped', _State('STOPPED'), True),
      ('terminated', _State('TERMINATED'), False),
      ('not_found', ('', 'NotAuthorizedOrNotFound', 1), False),
  )
  def test_exists(self, response, expected):
    """Tests exists."""
    self.MockIssueCommand({'instance get': [response]})
    vm = self._create_vm()
    vm.id = _INSTANCE_ID
    self.assertEqual(expected, vm._Exists())

  def test_exists_without_id(self):
    """Tests exists without id."""
    mock_cmd = self.MockIssueCommand({})
    self.assertFalse(self._create_vm()._Exists())
    mock_cmd.func_to_mock.assert_not_called()

  def test_delete(self):
    """Tests delete."""
    mock_cmd = self.MockIssueCommand({})
    vm = self._create_vm()
    vm.id = _INSTANCE_ID

    vm._Delete()

    cmd = mock_cmd.func_to_mock.call_args[0][0]
    self.assertEqual(['oci', 'compute', 'instance', 'terminate'], cmd[:4])
    self.assertEqual(_INSTANCE_ID, _GetArg(cmd, '--instance-id'))
    self.assertEqual('false', _GetArg(cmd, '--preserve-boot-volume'))
    self.assertIn('--force', cmd)

  @parameterized.named_parameters(
      ('start', '_Start', 'START', 'RUNNING'),
      ('stop', '_Stop', 'STOP', 'STOPPED'),
  )
  def test_start_stop(self, method, action, state):
    """Tests start stop."""
    mock_cmd = self.MockIssueCommand({})
    vm = self._create_vm()
    vm.id = _INSTANCE_ID

    # TestOsMixin stubs out _Start and _Stop, so call the OCI implementation.
    getattr(oci_virtual_machine.OciVirtualMachine, method)(vm)

    cmd = mock_cmd.func_to_mock.call_args[0][0]
    self.assertEqual(action, _GetArg(cmd, '--action'))
    self.assertEqual(state, _GetArg(cmd, '--wait-for-state'))

  def test_create_scratch_disk(self):
    """Tests create scratch disk."""
    create_from_disks = self.enter_context(
        mock.patch.object(TestOciVirtualMachine, '_CreateScratchDiskFromDisks')
    )
    prepare = self.enter_context(
        mock.patch.object(
            disk_strategies.PrepareScratchDiskStrategy, 'PrepareScratchDisk'
        )
    )
    vm = self._create_vm()
    disk_spec = disk.BaseDiskSpec(
        _COMPONENT,
        disk_type=oci_disk.BALANCED,
        disk_size=100,
        num_striped_disks=3,
        mount_point='/scratch',
    )

    vm.CreateScratchDisk(0, disk_spec)

    disks = create_from_disks.call_args[0][1]
    self.assertLen(disks, 3)
    self.assertEqual(
        [f'{vm.name}-data-{i}' for i in range(3)], [d.name for d in disks]
    )
    self.assertEqual([1, 2, 3], [d.disk_number for d in disks])
    self.assertTrue(all(d.availability_domain == _AD for d in disks))
    self.assertTrue(all(d.region == 'us-ashburn-1' for d in disks))
    self.assertEqual(3, vm.remote_disk_counter)
    prepare.assert_called_once_with(
        vm, create_from_disks.return_value, disk_spec
    )

  def test_create_scratch_disk_local_is_unsupported(self):
    """Tests create scratch disk local is unsupported."""
    vm = self._create_vm()
    disk_spec = disk.BaseDiskSpec(_COMPONENT, disk_type=disk.LOCAL)
    with self.assertRaises(errors.Config.InvalidValue):
      vm.CreateScratchDisk(0, disk_spec)

  def test_create_scratch_disk_too_many_disks(self):
    """Tests create scratch disk too many disks."""
    vm = self._create_vm()
    vm.remote_disk_counter = oci_virtual_machine.MAX_REMOTE_DISKS
    disk_spec = disk.BaseDiskSpec(
        _COMPONENT, disk_type=oci_disk.BALANCED, disk_size=100
    )
    with self.assertRaises(errors.Config.InvalidValue):
      vm.CreateScratchDisk(0, disk_spec)

  def test_get_resource_metadata(self):
    """Tests get resource metadata."""
    vm = self._create_vm(
        ocpus=4,
        memory_in_gbs=64,
        fault_domain='FAULT-DOMAIN-1',
        preemptible=True,
    )
    vm.created = True

    metadata = vm.GetResourceMetadata()

    self.assertEqual('OCI', metadata['cloud'])
    self.assertEqual('VM.Standard.E5.Flex', metadata['machine_type'])
    self.assertEqual('us-ashburn-1', metadata['region'])
    self.assertEqual(_AD, metadata['availability_domain'])
    self.assertEqual('FAULT-DOMAIN-1', metadata['fault_domain'])
    self.assertEqual(4, metadata['ocpus'])
    self.assertEqual(64, metadata['memory_in_gbs'])
    self.assertTrue(metadata['preemptible'])

  def test_get_resource_metadata_before_creation(self):
    """Tests get resource metadata before creation."""
    self.assertEqual({}, self._create_vm().GetResourceMetadata())

  @parameterized.named_parameters(
      ('running', _State('RUNNING'), False),
      ('terminated', _State('TERMINATED'), True),
  )
  def test_preemptible_interruption(self, response, expected):
    """Tests preemptible interruption."""
    self.MockIssueCommand({'instance get': [response]})
    vm = self._create_vm(preemptible=True)
    vm.id = _INSTANCE_ID
    self.assertTrue(vm.IsInterruptible())

    vm.UpdateInterruptibleVmStatus(use_api=True)

    self.assertEqual(expected, vm.WasInterrupted())

  def test_non_preemptible_is_not_interruptible(self):
    """Tests non preemptible is not interruptible."""
    vm = self._create_vm()
    self.assertFalse(vm.IsInterruptible())
    self.assertFalse(vm.WasInterrupted())


if __name__ == '__main__':
  unittest.main()
