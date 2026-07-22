import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.configs import vm_group_decoders
from perfkitbenchmarker.providers.aws import aws_auto_scaling_group
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import aws_virtual_machine
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class TestAwsVirtualMachine(
    pkb_common_test_case.TestOsMixin, aws_virtual_machine.AwsVirtualMachine
):
  pass


class AwsAutoScalingGroupTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            aws_virtual_machine.AwsVirtualMachine,
            'GetDefaultImage',
            return_value='ami-12345',
        )
    )
    self.enter_context(
        mock.patch.object(
            aws_virtual_machine,
            'GetRootBlockDeviceSpecForImage',
            return_value={
                'DeviceName': '/dev/sda1',
                'Ebs': {'VolumeSize': 10, 'VolumeType': 'gp2'},
            },
        )
    )
    self.mock_network = mock.MagicMock()
    self.mock_network.subnet.id = 'default'
    self.enter_context(
        mock.patch.object(
            aws_network.AwsNetwork, 'GetNetwork', return_value=self.mock_network
        )
    )
    self.enter_context(
        mock.patch.object(
            aws_network.AwsFirewall,
            'GetFirewall',
            return_value=mock.MagicMock(),
        )
    )
    FLAGS.run_uri = 'test_run'
    FLAGS.cloud = 'AWS'
    virtual_machine.BaseVirtualMachine._instance_counter = 0
    self.mock_cmd = self.MockIssueCommand(
        {
            '': [('', '', 0)],
        },
    )

  def _CreateAsg(self):
    spec = vm_group_decoders.VmGroupSpec(
        'test_asg',
        cloud='AWS',
        os_type='ubuntu2404',
        vm_count=1,
        vm_spec={'AWS': {'machine_type': 't2.small'}},
    )
    vm_spec = aws_virtual_machine.AwsVmSpec(
        'test_vm_spec', zone='us-east-1a', machine_type='t2.small'
    )
    vm = TestAwsVirtualMachine(vm_spec)
    vm.image = 'ami-12345'
    return aws_auto_scaling_group.AwsAutoScalingGroup(spec, [vm])

  def testInit(self):
    asg = self._CreateAsg()
    self.assertEqual(asg.name, 'pkb-test_run-0')
    self.assertEqual(asg.region, 'us-east-1')

  @mock.patch.object(aws_auto_scaling_group.AwsLaunchTemplate, 'Create')
  def testCreate(self, mock_lt_create):
    asg = self._CreateAsg()
    asg._Create()
    self.mock_cmd.func_to_mock.assert_called_with(
        [
            'aws',
            '--output',
            'json',
            'autoscaling',
            '--region',
            'us-east-1',
            'create-auto-scaling-group',
            '--auto-scaling-group-name',
            'pkb-test_run-0',
            '--launch-template',
            'LaunchTemplateName=pkb-test_run-0',
            '--min-size',
            '0',
            '--max-size',
            '10',
            '--desired-capacity',
            '1',
            '--vpc-zone-identifier',
            'default',
        ],
    )

  def testIsReadySuccess(self):
    asg = self._CreateAsg()
    self.mock_cmd = self.MockIssueCommand({
        'describe-scaling-activities': [(
            '{"Activities": [{"StatusCode": "Successful"}]}',
            '',
            0,
        )],
        'describe-auto-scaling-groups': [(
            (
                '{"AutoScalingGroups": [{"Instances": [{"LifecycleState":'
                ' "InService"}]}]}'
            ),
            '',
            0,
        )],
    })
    self.assertTrue(asg._IsReady())

  def testIsReadyInsufficientCapacity(self):
    asg = self._CreateAsg()
    self.mock_cmd = self.MockIssueCommand({
        'describe-scaling-activities': [(
            (
                '{"Activities": [{"StatusCode": "Failed", "Description":'
                ' "Launching a new EC2 instance", "StatusMessage":'
                ' "InsufficientInstanceCapacity"}]}'
            ),
            '',
            0,
        )],
    })
    with self.assertRaises(errors.Benchmarks.InsufficientCapacityCloudFailure):
      asg._IsReady()

  def testIsReadyUnsupported(self):
    asg = self._CreateAsg()
    self.mock_cmd = self.MockIssueCommand({
        'describe-scaling-activities': [(
            (
                '{"Activities": [{"StatusCode": "Failed", "Description":'
                ' "Launching a new EC2 instance", "StatusMessage": "The machine'
                ' type is not supported"}]}'
            ),
            '',
            0,
        )],
    })
    with self.assertRaises(errors.Benchmarks.UnsupportedConfigError):
      asg._IsReady()

  def testAddVms(self):
    asg = self._CreateAsg()
    asg.vm_count = 2
    asg._AddVms(1)
    self.mock_cmd.func_to_mock.assert_called_with(
        [
            'aws',
            '--output',
            'json',
            'autoscaling',
            '--region',
            'us-east-1',
            'launch-instances',
            '--auto-scaling-group-name',
            'pkb-test_run-0',
            '--requested-capacity',
            '1',
        ],
    )

  def testAddVmsWithZone(self):
    asg = self._CreateAsg()
    asg.vm_count = 2
    asg._AddVms(1, zone='us-east-1a')
    self.mock_cmd.func_to_mock.assert_called_with(
        [
            'aws',
            '--output',
            'json',
            'autoscaling',
            '--region',
            'us-east-1',
            'launch-instances',
            '--auto-scaling-group-name',
            'pkb-test_run-0',
            '--requested-capacity',
            '1',
            '--availability-zones',
            'us-east-1a',
        ],
    )


if __name__ == '__main__':
  unittest.main()
