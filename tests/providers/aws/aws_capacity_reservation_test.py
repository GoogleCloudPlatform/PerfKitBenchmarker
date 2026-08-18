"""Tests for perfkitbenchmarker.tests.providers.aws.aws_capacity_reservation."""

import datetime
import unittest

from absl import flags
import freezegun
import mock
from perfkitbenchmarker import virtual_machine_spec
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_capacity_reservation
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
TIMEOUT_UTC = '2019-05-23 03:56:22.703681'
FAKE_DATETIME_NOW = datetime.datetime(2010, 1, 1)

CREATE_STDOUT_SUCCESSFUL = """
{
  "CapacityReservation": {
      "CapacityReservationId": "cr-0b720f7e7f73b54e8",
      "EndDateType": "unlimited",
      "AvailabilityZone": "us-west-2a",
      "InstanceMatchCriteria": "open",
      "EphemeralStorage": false,
      "CreateDate": "2019-01-14T23:34:14.000Z",
      "AvailableInstanceCount": 3,
      "InstancePlatform": "Linux/UNIX",
      "TotalInstanceCount": 3,
      "State": "active",
      "Tenancy": "default",
      "EbsOptimized": false,
      "InstanceType": "m5.xlarge"
  }
}
"""


def FakeAwsVirtualMachine():
  vm_spec = virtual_machine_spec.BaseVmSpec('fake_vm')
  vm_spec.zone = 'us-west-1'
  vm_spec.machine_type = 'fake_machine_type'
  return vm_spec


class AwsCapacityReservationTest(pkb_common_test_case.PkbCommonTestCase):

  def _create_patch(self, target, return_val=None):
    p = mock.patch(target, return_value=return_val)
    mock_to_return = p.start()
    self.addCleanup(p.stop)
    return mock_to_return

  def setUp(self):
    super().setUp()
    FLAGS.timeout_minutes = 30

    self._create_patch(
        util.__name__ + '.GetZonesInRegion',
        return_val=['us-west-1a', 'us-west-1b'],
    )

  @freezegun.freeze_time(FAKE_DATETIME_NOW)
  def test_create(self):
    vm_group = [FakeAwsVirtualMachine()]
    capacity_reservation = aws_capacity_reservation.AwsCapacityReservation(
        vm_group
    )

    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=(CREATE_STDOUT_SUCCESSFUL, '', 0),
    ) as issue_command:
      capacity_reservation._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      expected_end_date = FAKE_DATETIME_NOW + datetime.timedelta(
          minutes=FLAGS.timeout_minutes
      )
      expected_command = (
          'aws --output json ec2 create-capacity-reservation '
          '--instance-type=fake_machine_type '
          '--instance-platform=Linux/UNIX --availability-zone=us-west-1a '
          '--instance-count=1 --instance-match-criteria=targeted '
          '--region=us-west-1 --end-date-type=limited --end-date=%s'
          % expected_end_date.isoformat()
      )

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(expected_command, command_string)

  def test_delete(self):
    vm_group = [FakeAwsVirtualMachine()]
    capacity_reservation = aws_capacity_reservation.AwsCapacityReservation(
        vm_group
    )
    capacity_reservation.capacity_reservation_id = 'foo'

    with mock.patch(vm_util.__name__ + '.IssueCommand') as issue_command:
      capacity_reservation._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      expected_command = (
          'aws --output json ec2 cancel-capacity-reservation '
          '--capacity-reservation-id=foo --region=us-west-1'
      )
      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(expected_command, command_string)


if __name__ == '__main__':
  unittest.main()
