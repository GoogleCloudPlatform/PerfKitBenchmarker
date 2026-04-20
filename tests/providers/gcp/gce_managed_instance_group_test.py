import builtins
import unittest
from unittest import mock

from absl import flags
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
    self.mock_cmd = self.MockIssueCommand(
        {
            '': [('', '', 0)],
        },
    )

  @mock.patch.object(gcp_utils, 'GetRegionFromZone', return_value='us-central1')
  @mock.patch.object(gce_virtual_machine.gce_network.GceFirewall, 'GetFirewall')
  @mock.patch.object(gce_virtual_machine.gce_network.GceNetwork, 'GetNetwork')
  def TestMig(self, mock_get_network, *_):
    mock_get_network.return_value.placement_group.name = 'test_placement_group'
    vm_config = pkb_common_test_case.TestGceVirtualMachine(
        gce_virtual_machine.GceVmSpec(
            'test_component',
            machine_type='n1-standard-4',
            zone='us-central1-c',
        )
    )
    return gce_managed_instance_group.GceManagedInstanceGroup(
        vm_group_decoders.VmGroupSpec(
            'test_component',
            cloud='GCP',
            os_type='debian12',
            vm_spec={'GCP': {'machine_type': 'n1-standard-4'}},
        ),
        vm_config,
    )

  def testCreate(self, *_):
    mig = self.TestMig()
    mig._Create()
    self.assertIn(
        'gcloud compute instance-groups managed create pkb-test_run-0'
        ' --template'
        ' projects/test_project/regions/us-central1/instanceTemplates/pkb-test_run-0'
        ' --size 1 --format json --project test_project --quiet --zone'
        ' us-central1-c',
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
        'gcloud compute instance-templates create pkb-test_run-1'
        ' --instance-template-region us-central1 --format json --labels '
        ' --machine-type n1-standard-4 --maintenance-policy TERMINATE'
        ' --metadata enable-oslogin=FALSE,vm_nature=ephemeral'
        ' --metadata-from-file sshKeys=ssh_key_file --no-restart-on-failure'
        ' --project test_project --quiet --resource-policies'
        ' test_placement_group --tags perfkitbenchmarker',
        self.mock_cmd.all_commands,
    )


if __name__ == '__main__':
  unittest.main()
