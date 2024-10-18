"""Tests for perfkitbenchmarker.tests.providers.azure.azure_virtual_machine."""

import unittest
from absl.testing import parameterized
import mock
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


class AzureVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__
            + '.azure_network.AzureNetwork.GetNetwork'
        )
    )
    self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__
            + '.azure_network.AzureFirewall.GetFirewall'
        )
    )
    self.enter_context(
        mock.patch(
            azure_virtual_machine.__name__ + '.azure_network.GetResourceGroup'
        )
    )
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand')
    )
    self.enter_context(mock.patch.object(util, 'GetResourceTags'))

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
          'testcase_name': 'SkuNotAvailable',
          'stderror': """{"error":{"code":"InvalidTemplateDeployment","message":"The template deployment 'vm_deploy_gwUdt7Sseaortu3nIvgZPN8rzgVucSOL' is not valid according to the validation procedure. The tracking id is '8b0552ca-7af5-4155-b653-8eb5f8713629'. See inner errors for details.","details":[{"code":"SkuNotAvailable","message":"The requested VM size for resource 'Following SKUs have failed for Capacity Restrictions: Standard_D2s_v4' is currently not available in location 'westus2'. Please try another size or deploy to a different location or different zone. See https://aka.ms/azureskunotavailable for details."}]}}""",  # pylint: disable=line-too-long
          'expected_error': errors.Benchmarks.UnsupportedConfigError,
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


if __name__ == '__main__':
  unittest.main()
