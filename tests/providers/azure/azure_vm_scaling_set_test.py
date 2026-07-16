import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers.azure import azure_virtual_machine
from perfkitbenchmarker.providers.azure import azure_vm_scaling_set
from perfkitbenchmarker.providers.azure import util as azure_utils
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class TestAzureVirtualMachine(
    pkb_common_test_case.TestOsMixin, azure_virtual_machine.AzureVirtualMachine
):
  GEN1_IMAGE_URN = 'test_image_urn'
  GEN2_IMAGE_URN = 'test_image_urn-gen2'


class AzureVmScalingSetTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_run'
    virtual_machine.BaseVirtualMachine._instance_counter = 0
    self.mock_cmd = self.MockIssueCommand(
        {
            '': [('', '', 0)],
        },
    )

  @mock.patch.object(vm_util, 'GetPublicKeyPath', return_value='test_pub_key')
  @mock.patch.object(
      azure_virtual_machine.azure_network.AzureFirewall, 'GetFirewall'
  )
  @mock.patch.object(azure_virtual_machine.azure_network, 'GetResourceGroup')
  @mock.patch.object(
      azure_virtual_machine.azure_network.AzureNetwork, 'GetNetwork'
  )
  def TestMig(
      self,
      mock_get_network,
      mock_get_resource_group,
      *_,
  ):
    mock_get_network.return_value.subnets = [mock.MagicMock()]
    mock_get_network.return_value.vnet.name = 'test_vnet'
    mock_get_network.return_value.subnets[0].name = 'test_subnet'
    mock_get_network.return_value.subnet.name = 'test_subnet'
    mock_get_resource_group.return_value.args = [
        '--resource-group',
        'test_project',
    ]
    mock_get_resource_group.return_value.timeout_minutes = 60
    vm_config = TestAzureVirtualMachine(
        azure_virtual_machine.AzureVmSpec(
            'test_component',
            machine_type='Standard_D2s_v5',
            zone='eastus2',
        )
    )
    return azure_vm_scaling_set.AzureVmScalingSet(
        vm_group_decoders.VmGroupSpec(
            'test_component',
            cloud='Azure',
            os_type='debian12',
            vm_spec={'Azure': {'machine_type': 'Standard_D2s_v5'}},
        ),
        [vm_config],
    )

  @mock.patch.object(
      azure_utils, 'GetResourceTags', return_value={'timeout_utc': 'none'}
  )
  def testCreate(self, _):
    mig = self.TestMig()
    mig._Create()
    self.assertIn(
        'az vmss create --resource-group test_project --name pkb-test_run-0'
        ' --image test_image_urn-gen2 --vm-sku Standard_D2s_v5'
        ' --instance-count 1 --location eastus2 --admin-username perfkit'
        ' --ssh-key-value test_pub_key --vnet-name test_vnet --subnet'
        ' test_subnet --upgrade-policy-mode Manual --lb  --tags'
        ' timeout_utc=none vm_nature=ephemeral --public-ip-per-vm',
        self.mock_cmd.all_commands,
    )

  def testGetCurrentVms(self):
    self.mock_cmd = self.MockIssueCommand(
        {
            'list-instances': [(
                '[{"instanceId": "0", "location": "eastus2", "zones": ["1"]}]',
                '',
                0,
            )],
        },
    )
    mig = self.TestMig()
    vms = mig._GetCurrentVms()
    self.assertEqual(
        [azure_vm_scaling_set.VmReference(name='0', zone='eastus2-1')], vms
    )
    self.assertIn(
        'az vmss list-instances --resource-group test_project --name'
        ' pkb-test_run-0',
        self.mock_cmd.all_commands,
    )

  def testCheckForReadinessErrorsNoFailed(self):
    self.mock_cmd = self.MockIssueCommand(
        {
            'list-instances': [(
                (
                    '[{"instanceId": "0", "location": "eastus2",'
                    ' "provisioningState": "Succeeded"}]'
                ),
                '',
                0,
            )],
        },
    )
    mig = self.TestMig()
    # Should not raise any exceptions.
    mig._CheckForReadinessErrors()

  def testCheckForReadinessErrorsWithFailed(self):
    get_instance_view_resp = (
        '{"statuses": [{"code": "ProvisioningState/failed", "message": "out of'
        ' capacity"}]}'
    )
    self.mock_cmd = self.MockIssueCommand(
        {
            'list-instances': [(
                (
                    '[{"instanceId": "0", "location": "eastus2", "properties":'
                    ' {"provisioningState": "Failed"}}]'
                ),
                '',
                0,
            )],
            'get-instance-view': [(
                get_instance_view_resp,
                '',
                0,
            )],
        },
    )
    mig = self.TestMig()
    with self.assertRaises(errors.Resource.CreationError) as cm:
      mig._CheckForReadinessErrors()

    self.assertIn(
        'Failed to provision VMSS pkb-test_run-0 because VMs failed: 0',
        str(cm.exception),
    )


if __name__ == '__main__':
  unittest.main()
