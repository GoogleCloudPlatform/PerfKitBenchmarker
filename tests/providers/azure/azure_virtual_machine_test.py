# Lint as: python3
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
      ('QuotaExceeded', '', 'Error Code: QuotaExceeded', 1),
      ('CoreQuotaExceeded', '',
       'Operation could not be completed as it results in exceeding approved '
       'standardEv3Family Cores quota', 1),
      ('CoreQuotaExceededDifferentWording', '',
       'The operation could not be completed as it results in exceeding quota '
       'limit of standardEv3Family Cores', 1))
  def testQuotaExceeded(self, _, stderror, retcode):
    spec = azure_virtual_machine.AzureVmSpec(
        _COMPONENT, machine_type='test_machine_type', zone='testing')
    vm = TestAzureVirtualMachine(spec)

    self.mock_cmd.side_effect = [(_, stderror, retcode)]
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
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
