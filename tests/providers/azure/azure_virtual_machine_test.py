"""Tests for perfkitbenchmarker.tests.providers.azure.azure_virtual_machine."""

import json
import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.azure import util
from tests import pkb_common_test_case

_COMPONENT = 'test_component'


class TestAzureVirtualMachine(
    pkb_common_test_case.TestOsMixin, azure_virtual_machine.AzureVirtualMachine
):
  GEN1_IMAGE_URN = 'test_image_urn'
  GEN2_IMAGE_URN = 'test_image_urn-gen2'
  ARM_IMAGE_URN = 'test_image_urn-arm64'


class AzureVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    mock_get_network = self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__
            + '.azure_network.AzureNetwork.GetNetwork'
        )
    )
    mock_get_network.return_value.subnets = [mock.MagicMock()]
    mock_get_network.return_value.placement_group = None
    self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__
            + '.azure_network.AzureFirewall.GetFirewall'
        )
    )
    mock_get_rg = self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__ + '.azure_network.GetResourceGroup'
        )
    )
    mock_get_rg.return_value.args = ['--resource-group', 'test_rg']
    mock_get_rg.return_value.timeout_minutes = 60
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', 0))
    )
    self.enter_context(
        mock.patch.object(util, 'GetResourceTags', return_value={})
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'QuotaExceeded',
          'stderror': 'Error Code: QuotaExceeded',
          'expected_error': errors.Benchmarks.QuotaFailure,
      },
      {
          'testcase_name': 'CoreQuotaExceeded',
          'stderror': (
              'Operation could not be completed as it results in exceeding '
              'approved standardEv3Family Cores quota'
          ),
          'expected_error': errors.Benchmarks.QuotaFailure,
      },
      {
          'testcase_name': 'CoreQuotaExceededDifferentWording',
          'stderror': (
              'The operation could not be completed as it results in exceeding '
              'quota limit of standardEv3Family Cores'
          ),
          'expected_error': errors.Benchmarks.QuotaFailure,
      },
      {
          'testcase_name': 'FamilyQuotaExceededWording',
          'stderror': (
              'Operation could not be completed as it results in exceeding '
              'approved Standard NDASv4_A100 Family Cores quota'
          ),
          'expected_error': errors.Benchmarks.QuotaFailure,
      },
      {
          'testcase_name': 'UnavailableInRegion',
          'stderror': (
              'The requested VM size Standard_D2s_v5 is not available in the '
              'current region. The sizes available in the current region are'
          ),
          'expected_error': errors.Benchmarks.UnsupportedConfigError,
      },
      {
          'testcase_name': 'UnavailableInZone',
          'stderror': (
              "The requested VM size 'Standard_D2s_v5' is not available in the "
              'current availability zone. The sizes available in the current '
              'availability zone are'
          ),
          'expected_error': errors.Benchmarks.UnsupportedConfigError,
      },
      {
          'testcase_name': 'UnsupportedSku',
          'stderror': (
              'The requested resource is currently not available in location '
              "'eastus' zones '1' for subscription"
          ),
          'expected_error': errors.Benchmarks.UnsupportedConfigError,
      },
      {
          'testcase_name': 'ProvisioningTimedOut',
          'stderror': (
              '"code": "OSProvisioningTimedOut",\r\n        "message": '
              "\"OS Provisioning for VM 'pkb-f1e4bae672b8-0' did not finish "
              'in the allotted time. The VM may still finish provisioning '
              'successfully. Please check provisioning state later.'
          ),
          'expected_error': errors.Resource.ProvisionTimeoutError,
      },
      {
          'testcase_name': 'AllocationFailed',
          'stderror': """{"error":{"code":"InvalidTemplateDeployment","message":"The template deployment 'vm_deploy_gwUdt7Sseaortu3nIvgZPN8rzgVucSOL' is not valid according to the validation procedure. The tracking id is '8b0552ca-7af5-4155-b653-8eb5f8713629'. See inner errors for details.","details":[{"code":"SkuNotAvailable","message":"The requested VM size for resource 'Following SKUs have failed for Capacity Restrictions: Standard_D2s_v4' is currently not available in location 'westus2'. Please try another size or deploy to a different location or different zone. See https://aka.ms/azureskunotavailable for details."}]}}""",  # pylint: disable=line-too-long
          'expected_error': errors.Benchmarks.InsufficientCapacityCloudFailure,
      },
      {
          'testcase_name': 'ZonalAllocationFailed',
          'stderror': """{"status":"Failed","error":{"code":"DeploymentFailed","message":"At least one resource deployment operation failed. Please list deployment operations for details. Please see https://aka.ms/DeployOperations for usage details.","details":[{"code":"Conflict","message":"{\r\n  \"status\": \"Failed\",\r\n  \"error\": {\r\n    \"code\": \"ResourceDeploymentFailure\",\r\n    \"message\": \"The resource operation completed with terminal provisioning state 'Failed'.\",\r\n    \"details\": [\r\n      {\r\n        \"code\": \"ZonalAllocationFailed\",\r\n        \"message\": \"Allocation failed. We do not have sufficient capacity for the requested VM size in this zone. Read more about improving likelihood of allocation success at http://aka.ms/allocation-guidance\"\r\n      }\r\n    ]\r\n  }\r\n}"}]}}""",  # pylint: disable=line-too-long
          'expected_error': errors.Benchmarks.InsufficientCapacityCloudFailure,
      },
      {
          'testcase_name': 'OverconstrainedZonalAllocationRequest',
          'stderror': """{"status":"Failed","error":{"code":"DeploymentFailed","message":"At least one resource deployment operation failed. Please list deployment operations for details. Please see https://aka.ms/DeployOperations for usage details.","details":[{"code":"Conflict","message":"{\r\n  \"status\": \"Failed\",\r\n  \"error\": {\r\n    \"code\": \"ResourceDeploymentFailure\",\r\n    \"message\": \"The resource operation completed with terminal provisioning state 'Failed'.\",\r\n    \"details\": [\r\n      {\r\n        \"code\": \"OverconstrainedZonalAllocationRequest\",\r\n        \"message\": \"Allocation failed. VM(s) with the following constraints cannot be allocated, because the condition is too restrictive. Please remove some constraints and try again. Constraints applied are:\\n  - Availability Zone\\n  - VM Size\\n\"\r\n      }\r\n    ]\r\n  }\r\n}"}]}}""",  # pylint: disable=line-too-long
          'expected_error': errors.Benchmarks.UnsupportedConfigError,
      },
  )
  def testVmCreationError(self, stderror, expected_error):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='Standard_D2s_v5', zone='testing'
    )
    vm = TestAzureVirtualMachine(spec)
    vm.SetDiskSpec(None, 0)
    self.mock_cmd.side_effect = [('', stderror, 1)]
    with self.assertRaises(expected_error):
      vm._Create()

  def testInsufficientSpotCapacity(self):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT,
        machine_type='Standard_D2s_v5',
        zone='testing',
        low_priority=True,
    )
    vm = TestAzureVirtualMachine(spec)
    vm.SetDiskSpec(None, 0)
    self.mock_cmd.side_effect = [('', 'OverconstrainedAllocationRequest', 1)]
    with self.assertRaises(errors.Benchmarks.InsufficientCapacityCloudFailure):
      vm._Create()

  def testPostCreateOfficialCreateTime(self):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='Standard_D2s_v5', zone='testing'
    )
    vm = TestAzureVirtualMachine(spec)
    vm.SetDiskSpec(None, 0)

    fake_response = {
        'privateIps': '10.0.0.1',
        'publicIps': '1.2.3.4',
        'storageProfile': {'osDisk': {'name': 'fake_os_disk'}},
        'timeCreated': '2026-07-15T12:00:00.000Z',
    }

    with mock.patch.object(
        vm_util,
        'IssueRetryableCommand',
        return_value=(json.dumps(fake_response), ''),
    ), mock.patch.object(util, 'GetResourceTags', return_value={}):
      vm._PostCreate()
      self.assertEqual(vm.official_create_time, 1784116800)

  @parameterized.named_parameters(
      dict(
          testcase_name='Gen5',
          machine_type='Standard_D4ps_v5',
          expected_arm_arch='neoverse-n1',
      ),
      dict(
          testcase_name='Gen6',
          machine_type='Standard_E4ps_v6',
          expected_arm_arch='neoverse-n2',
      ),
      dict(
          testcase_name='Gen7Dps',
          machine_type='Standard_D4ps_v7',
          expected_arm_arch='neoverse-v3',
      ),
      dict(
          testcase_name='Gen7Eps',
          machine_type='Standard_E4ps_v7',
          expected_arm_arch='neoverse-v3',
      ),
      dict(
          testcase_name='Gen7Dpds',
          machine_type='Standard_D2pds_v7',
          expected_arm_arch='neoverse-v3',
      ),
  )
  def testArmArchitecture(self, machine_type, expected_arm_arch):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type=machine_type, zone='testing'
    )
    vm = TestAzureVirtualMachine(spec)
    self.assertTrue(vm.is_aarch64)
    self.assertEqual(vm.host_arch, expected_arm_arch)
    self.assertEqual(vm.image, TestAzureVirtualMachine.ARM_IMAGE_URN)

  def testNonArmArchitecture(self):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='Standard_D4s_v7', zone='testing'
    )
    vm = TestAzureVirtualMachine(spec)
    self.assertFalse(vm.is_aarch64)
    self.assertFalse(hasattr(vm, 'host_arch'))
    self.assertEqual(vm.image, TestAzureVirtualMachine.GEN2_IMAGE_URN)

  def testUnknownArmArchitectureRaises(self):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='Standard_D4ps_v8', zone='testing'
    )
    with self.assertRaisesRegex(
        ValueError, 'Unknown ARM machine type: Standard_D4ps_v8'
    ):
      TestAzureVirtualMachine(spec)

  @parameterized.named_parameters(
      dict(
          testcase_name='ArmGen7DpsDefault',
          machine_type='Standard_D4ps_v7',
          processor_mode=None,
          expected_processor_mode='opportunistic',
      ),
      dict(
          testcase_name='ArmGen7EpsDefault',
          machine_type='Standard_E4ps_v7',
          processor_mode=None,
          expected_processor_mode='opportunistic',
      ),
      dict(
          testcase_name='ArmGen7DpdsDefault',
          machine_type='Standard_D2pds_v7',
          processor_mode=None,
          expected_processor_mode='opportunistic',
      ),
      dict(
          testcase_name='ArmGen7Deterministic',
          machine_type='Standard_D4ps_v7',
          processor_mode='deterministic',
          expected_processor_mode='deterministic',
      ),
      dict(
          testcase_name='ArmGen5NotSupported',
          machine_type='Standard_D4ps_v5',
          processor_mode=None,
          expected_processor_mode=None,
      ),
      dict(
          testcase_name='NonArmGen7DasNotSupported',
          machine_type='Standard_D4as_v7',
          processor_mode=None,
          expected_processor_mode=None,
      ),
  )
  def testProcessorMode(
      self, machine_type, processor_mode, expected_processor_mode
  ):
    flag_overrides = (
        {'azure_vm_processor_mode': processor_mode}
        if processor_mode is not None
        else {}
    )
    with flagsaver.flagsaver(**flag_overrides):
      spec = azure_virtual_machine.AzureVmSpec(
          _COMPONENT, machine_type=machine_type, zone='testing'
      )
      vm = TestAzureVirtualMachine(spec)
      self.assertEqual(vm.processor_mode, expected_processor_mode)
      vm.SetDiskSpec(None, 0)
      vm._Create()
      vm.created = True
      create_cmd = self.mock_cmd.call_args[0][0]
      metadata = vm.GetResourceMetadata()

      if expected_processor_mode is not None:
        self.assertContainsExactSubsequence(
            create_cmd, ['--processor-mode', expected_processor_mode]
        )
        self.assertEqual(
            metadata.get('processor_mode'), expected_processor_mode
        )
      else:
        self.assertNotIn('--processor-mode', create_cmd)
        self.assertNotIn('processor_mode', metadata)


class AzurePublicIPAddressTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__ + '.azure_network.GetResourceGroup'
        )
    )
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )
    self.ip_address = azure_virtual_machine.AzurePublicIPAddress(
        'westus2', None, 'test_ip'
    )

  def testQuotaExceeded(self):
    quota_error = (
        'ERROR: Cannot create more than 20 public IP addresses for '
        'this subscription in this region.'
    )
    self.mock_cmd.side_effect = [('', quota_error, 1)]
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      self.ip_address._Create()


class AzureNICTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    mock_rg = self.enter_context(
        mock.patch.object(
            azure_virtual_machine.azure_network,
            'GetResourceGroup',
            autospec=True,
        )
    )
    mock_rg.return_value.args = ['--resource-group', 'test_rg']
    mock_network = mock.create_autospec(
        azure_virtual_machine.azure_network.AzureNetwork, instance=True
    )
    mock_network.region = 'westus2'
    mock_network.nsgs = []
    mock_subnet = mock.create_autospec(
        azure_virtual_machine.azure_network.AzureSubnet, instance=True
    )
    mock_subnet.id = 'test_subnet_id'
    self.nic = azure_virtual_machine.AzureNIC(
        network=mock_network,
        subnet=mock_subnet,
        name='test_nic',
        public_ip=None,
        accelerated_networking=True,
    )

  def testCreateIssuesRetryableCommand(self):
    mock_retryable_cmd = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueRetryableCommand',
            autospec=True,
            return_value=('', ''),
        )
    )
    self.nic._Create()
    mock_retryable_cmd.assert_called_once_with([
        azure_virtual_machine.azure.AZURE_PATH,
        'network',
        'nic',
        'create',
        '--location',
        'westus2',
        '--name',
        'test_nic',
        '--resource-group',
        'test_rg',
        '--subnet',
        'test_subnet_id',
        '--accelerated-networking',
        'true',
    ])

  def testCreateRetriesOnTransientCommandFailure(self):
    self.enter_context(
        mock.patch.object(vm_util.time, 'sleep', autospec=True)
    )
    mock_cmd = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            autospec=True,
            side_effect=[
                ('', 'deadlock detected by _ModuleLock', 1),
                ('', '', 0),
            ],
        )
    )
    self.nic._Create()
    self.assertEqual(mock_cmd.call_count, 2)


if __name__ == '__main__':
  unittest.main()
