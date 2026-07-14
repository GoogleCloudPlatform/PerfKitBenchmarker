import builtins
import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers.gcp import gce_managed_instance_group
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import util as gcp_utils
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class GceManagedInstanceGroupTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = 'test_run'
    virtual_machine.BaseVirtualMachine._instance_counter = 0
    self.mock_cmd = self.MockIssueCommand(
        {
            '': [('', '', 0)],
        },
    )

  @mock.patch.object(gcp_utils, 'GetRegionFromZone', return_value='us-central1')
  @mock.patch.object(gce_virtual_machine.gce_network.GceFirewall, 'GetFirewall')
  @mock.patch.object(gce_virtual_machine.gce_network.GceNetwork, 'GetNetwork')
  def TestMig(self, mock_get_network, *_, zone='us-central1-c'):
    mock_get_network.return_value.placement_group.name = 'test_placement_group'
    vm_config = pkb_common_test_case.TestGceVirtualMachine(
        gce_virtual_machine.GceVmSpec(
            'test_component',
            machine_type='n1-standard-4',
            zone=zone,
        )
    )
    return gce_managed_instance_group.GceManagedInstanceGroup(
        vm_group_decoders.VmGroupSpec(
            'test_component',
            cloud='GCP',
            os_type='debian12',
            vm_spec={'GCP': {'machine_type': 'n1-standard-4'}},
        ),
        [vm_config],
    )

  def testCreate(self, *_):
    mig = self.TestMig(zone='us-central1-c')
    mig._Create()
    self.assertIn(
        'gcloud compute instance-groups managed create pkb-test_run-0'
        ' --template'
        ' projects/test_project/regions/us-central1/instanceTemplates/pkb-test_run-0'
        ' --size 1 --format json --project test_project --quiet --zone'
        ' us-central1-c',
        self.mock_cmd.all_commands,
    )

  def testCreateRegional(self, *_):
    mig = self.TestMig(zone='us-central1')
    mig._Create()
    self.assertIn(
        'gcloud compute instance-groups managed create pkb-test_run-0'
        ' --template'
        ' projects/test_project/regions/us-central1/instanceTemplates/pkb-test_run-0'
        ' --size 1 --format json --project test_project --quiet --region'
        ' us-central1',
        self.mock_cmd.all_commands,
    )

  @mock.patch.object(gcp_utils, 'GetRegionFromZone', return_value='us-central1')
  @mock.patch.object(gce_virtual_machine.gce_network.GceFirewall, 'GetFirewall')
  @mock.patch.object(gce_virtual_machine.gce_network.GceNetwork, 'GetNetwork')
  def testCreateMultiZone(self, mock_get_network, *_):
    mock_get_network.return_value.placement_group.name = 'test_placement_group'
    vm_configs = [
        pkb_common_test_case.TestGceVirtualMachine(
            gce_virtual_machine.GceVmSpec(
                'test_component',
                machine_type='n1-standard-4',
                zone=zone,
            )
        )
        for zone in ['us-central1-a', 'us-central1-b']
    ]
    mig = gce_managed_instance_group.GceManagedInstanceGroup(
        vm_group_decoders.VmGroupSpec(
            'test_component',
            cloud='GCP',
            os_type='debian12',
            vm_spec={'GCP': {'machine_type': 'n1-standard-4'}},
        ),
        vm_configs,  # pyrefly: ignore[bad-argument-type]
    )
    mig._Create()
    self.assertIn(
        'gcloud compute instance-groups managed create pkb-test_run-0'
        ' --template'
        ' projects/test_project/regions/us-central1/instanceTemplates/pkb-test_run-0'
        ' --size 1 --zones=us-central1-a,us-central1-b --format json'
        ' --project test_project --quiet --region us-central1',
        self.mock_cmd.all_commands,
    )

  # SSH keys
  @mock.patch.object(builtins, 'open')
  @mock.patch.object(vm_util, 'NamedTemporaryFile')
  def testCreateDependencies(self, mock_named_temporary_file, _):
    mig = self.TestMig()
    mock_named_temporary_file.return_value.__enter__.return_value.name = (
        'ssh_key_file'
    )
    mig._CreateDependencies()
    self.assertIn(
        'gcloud compute instance-templates create pkb-test_run-0'
        ' --instance-template-region us-central1 --format json --labels '
        ' --machine-type n1-standard-4 --maintenance-policy TERMINATE'
        ' --metadata enable-oslogin=FALSE,vm_nature=ephemeral'
        ' --metadata-from-file sshKeys=ssh_key_file --no-restart-on-failure'
        ' --project test_project --quiet --resource-policies'
        ' test_placement_group --tags perfkitbenchmarker',
        self.mock_cmd.all_commands,
    )

  def testCheckForReadinessErrorsNoErrors(self):
    mig = self.TestMig()
    self.mock_cmd = self.MockIssueCommand({
        'list-errors': [('[]', '', 0)],
    })
    mig._CheckForReadinessErrors()

  def testCheckForReadinessErrorsWithErrors(self):
    mig = self.TestMig()
    error_resp = (
        '[{"error": {"code": "ZONE_RESOURCE_POOL_EXHAUSTED", "message": "does'
        ' not have enough resources available to fulfill the request."}}]'
    )
    self.mock_cmd = self.MockIssueCommand({
        'list-errors': [(error_resp, '', 0)],
    })
    with self.assertRaises(errors.Benchmarks.InsufficientCapacityCloudFailure):
      mig._CheckForReadinessErrors()

  def testGetCurrentVms(self):
    list_instances_response = (
        '[{"name": "pkb-test_run-0-0", "instance":'
        ' "https://www.googleapis.com/compute/v1/projects/test_project/'
        'zones/us-central1-c/instances/pkb-test_run-0-0"}]'
    )
    self.MockIssueCommand(
        {
            'gcloud compute instance-groups managed list-instances': [
                (list_instances_response, '', 0)
            ]
        }
    )
    mig = self.TestMig()
    vms = mig._GetCurrentVms()
    self.assertEqual(len(vms), 1)
    self.assertEqual(vms[0].name, 'pkb-test_run-0-0')
    self.assertEqual(vms[0].zone, 'us-central1-c')


if __name__ == '__main__':
  unittest.main()
