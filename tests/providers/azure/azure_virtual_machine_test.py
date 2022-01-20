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


class TestAzureVirtualMachine(pkb_common_test_case.TestOsMixin,
                              azure_virtual_machine.AzureVirtualMachine):
  IMAGE_URN = 'test_image_urn'


class AzureVirtualMachineTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzureVirtualMachineTest, self).setUp()
    self.enter_context(
        mock.patch(azure_virtual_machine.__name__ +
                   '.azure_network.AzureNetwork.GetNetwork'))
    self.enter_context(
        mock.patch(azure_virtual_machine.__name__ +
                   '.azure_network.AzureFirewall.GetFirewall'))
    self.enter_context(
        mock.patch(azure_virtual_machine.__name__ +
                   '.azure_network.GetResourceGroup'))
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand'))
    self.enter_context(mock.patch.object(util, 'GetResourceTags'))

  @parameterized.named_parameters(
      {
          'testcase_name': 'QuotaExceeded',
          'stderror': 'Error Code: QuotaExceeded',
          'expected_error': errors.Benchmarks.QuotaFailure
      },
      {
          'testcase_name': 'CoreQuotaExceeded',
          'stderror':
              'Operation could not be completed as it results in exceeding '
              'approved standardEv3Family Cores quota',
          'expected_error': errors.Benchmarks.QuotaFailure
      },
      {
          'testcase_name': 'CoreQuotaExceededDifferentWording',
          'stderror':
              'The operation could not be completed as it results in exceeding '
              'quota limit of standardEv3Family Cores',
          'expected_error': errors.Benchmarks.QuotaFailure,
      },
      {
          'testcase_name': 'FamilyQuotaExceededWording',
          'stderror':
              'Operation could not be completed as it results in exceeding '
              'approved Standard NDASv4_A100 Family Cores quota',
          'expected_error': errors.Benchmarks.QuotaFailure
      },
      {
          'testcase_name': 'UnavailableInRegion',
          'stderror':
              'The requested VM size Standard_D2s_v5 is not available in the '
              'current region. The sizes available in the current region are',
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      },
      {
          'testcase_name': 'UnavailableInZone',
          'stderror':
              "The requested VM size 'Standard_D2s_v5' is not available in the "
              "current availability zone. The sizes available in the current "
              "availability zone are",
          'expected_error': errors.Benchmarks.UnsupportedConfigError
      },
  )
  def testVmCreationError(self, stderror, expected_error):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='test_machine_type', zone='testing')
    vm = TestAzureVirtualMachine(spec)

    self.mock_cmd.side_effect = [('', stderror, 1)]
    with self.assertRaises(expected_error):
      vm._Create()

  def testInsufficientSpotCapacity(self):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='test_machine_type', zone='testing',
        low_priority=True)
    vm = TestAzureVirtualMachine(spec)

    self.mock_cmd.side_effect = [('', 'OverconstrainedAllocationRequest', 1)]
    with self.assertRaises(errors.Benchmarks.InsufficientCapacityCloudFailure):
      vm._Create()


class AzurePublicIPAddressTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AzurePublicIPAddressTest, self).setUp()
    self.enter_context(
        mock.patch(azure_virtual_machine.__name__ +
                   '.azure_network.GetResourceGroup'))
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand'))
    self.ip_address = azure_virtual_machine.AzurePublicIPAddress(
        'westus2', None, 'test_ip')

  def testQuotaExceeded(self):
    quota_error = ('ERROR: Cannot create more than 20 public IP addresses for '
                   'this subscription in this region.')
    self.mock_cmd.side_effect = [('', quota_error, 1)]
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      self.ip_address._Create()

if __name__ == '__main__':
  unittest.main()
