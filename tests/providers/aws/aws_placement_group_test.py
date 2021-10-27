"""Tests for perfkitbenchmarker.providers.aws.aws_placement_group."""


import json
import unittest
import uuid
from absl import flags
from absl.testing import parameterized
import mock

from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import spec
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

CLOUD = providers.AWS
ZONE = 'us-west-1a'
REGION = 'us-west-1'
STRATEGY = placement_group.PLACEMENT_GROUP_CLUSTER
RUN_URI = 'run12345'
UUID = 'random'
GROUP_NAME = 'perfkit-{}-{}'.format(RUN_URI, UUID)

EXISTS_NONE_RESPONSE = {'PlacementGroups': []}
EXISTS_ONE_RESPONSE = {
    'PlacementGroups': [{
        'GroupName': GROUP_NAME,
        'State': 'available',
        'Strategy': STRATEGY
    }]
}
EXISTS_TWO_RESPONSE = {'PlacementGroups': ['seat', 'filler']}
CREATE_RESPONSE = None
DELETE_RESPONSE = None

FLAGS = flags.FLAGS
FLAGS.run_uri = RUN_URI


def AwsCommand(topic, *aws_args, **env):
  # used when validating an AWS command run via vm_util.IssueCommand
  aws_bash_cmd = [
      'aws', '--output', 'json', 'ec2', topic, '--region={}'.format(REGION)
  ] + list(aws_args)
  return mock.call(aws_bash_cmd, **env)


EXISTS_CALL = AwsCommand(
    'describe-placement-groups',
    '--filter=Name=group-name,Values={}'.format(GROUP_NAME),
    env=None,
    raise_on_failure=False,
    suppress_failure=None)
CREATE_CALL = AwsCommand('create-placement-group',
                         '--group-name={}'.format(GROUP_NAME),
                         '--strategy={}'.format(STRATEGY),
                         '--tag-specifications=foobar')
DELETE_CALL = AwsCommand(
    'delete-placement-group',
    '--group-name={}'.format(GROUP_NAME),
    raise_on_failure=False)


def AwsResponse(data):
  return ('' if data is None else json.dumps(data), '', 0)


def CreateAwsPlacementGroupSpec(group_style=STRATEGY):
  spec_class = spec.GetSpecClass(
      placement_group.BasePlacementGroupSpec, CLOUD=CLOUD)
  FLAGS.placement_group_style = group_style
  name = '{0}.placement_group_spec.{1}'.format(spec_class.SPEC_TYPE, CLOUD),
  return spec_class(name, zone=ZONE, flag_values=FLAGS)


def CreateAwsPlacementGroup(group_style=STRATEGY):
  placement_group_class = placement_group.GetPlacementGroupClass(CLOUD)
  return placement_group_class(CreateAwsPlacementGroupSpec(group_style))


class AwsPlacementGroupTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsPlacementGroupTest, self).setUp()
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand'))
    uuid_call = self.enter_context(mock.patch.object(uuid, 'uuid4'))
    uuid_call.return_value = UUID

  def assertAwsCommands(self, *expected_calls):
    # easier to read error message when looping through individual items
    for expected, call in zip(expected_calls, self.mock_cmd.call_args_list):
      self.assertEqual(expected, call)
    self.assertEqual(list(expected_calls), self.mock_cmd.call_args_list)

  def testGetSpec(self):
    pg_spec = CreateAwsPlacementGroupSpec('spread')
    self.assertEqual('spread', pg_spec.placement_group_style)

  def testGetPlacementGroup(self):
    pg = CreateAwsPlacementGroup()
    self.assertEqual(REGION, pg.region)
    self.assertEqual(STRATEGY, pg.strategy)

  @parameterized.named_parameters(
      ('EXISTS_NONE_RESPONSE', EXISTS_NONE_RESPONSE, False),
      ('EXISTS_ONE_RESPONSE', EXISTS_ONE_RESPONSE, True),
      ('EXISTS_TWO_RESPONSE', EXISTS_TWO_RESPONSE, None, True))
  def testExists(self, response, exists_value, throws_exception=False):
    self.mock_cmd.side_effect = [AwsResponse(response)]
    pg = CreateAwsPlacementGroup()
    if throws_exception:
      with self.assertRaises(AssertionError):
        pg._Exists()
    else:
      self.assertEqual(exists_value, pg._Exists())
    self.assertAwsCommands(EXISTS_CALL)

  @mock.patch.object(util, 'FormatTagSpecifications')
  def testCreate(self, mock_cmd):
    mock_cmd.return_value = 'foobar'
    self.mock_cmd.side_effect = [
        AwsResponse(CREATE_RESPONSE),
        AwsResponse(EXISTS_ONE_RESPONSE),
    ]
    pg = CreateAwsPlacementGroup()
    pg.Create()
    self.assertAwsCommands(CREATE_CALL, EXISTS_CALL)

  def testDelete(self):
    self.mock_cmd.side_effect = [
        AwsResponse(DELETE_RESPONSE),
        AwsResponse(EXISTS_NONE_RESPONSE),
    ]
    pg = CreateAwsPlacementGroup()
    pg.created = True
    pg.Delete()
    self.assertAwsCommands(DELETE_CALL, EXISTS_CALL)


if __name__ == '__main__':
  unittest.main()
