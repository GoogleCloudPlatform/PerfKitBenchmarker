# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Class to represent an AWS Placement Group object.

Cloud specific implementations of Placement Group.
"""

import json
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.aws import util


_PLACEMENT_GROUP_OPTIONS = frozenset(['cluster', 'spread', 'none'])
_PLACEMENT_GROUP_DEFAULT = 'cluster'
PLACEMENT_GROUP_NONE = 'none'

flags.DEFINE_enum(
    'aws_placement_group_style', _PLACEMENT_GROUP_DEFAULT,
    _PLACEMENT_GROUP_OPTIONS,
    'The AWS placement group option to use. Default sets placement to cluster.')

FLAGS = flags.FLAGS


class AwsPlacementGroupSpec(placement_group.BasePlacementGroupSpec):
  """Object containing the information needed to create an AwsPlacementGroup.

  Attributes:
      zone: The AWS zone the Placement Group is in.
  """

  CLOUD = providers.AWS

  @classmethod
  def _ApplyFlags(cls, config_values, flag_values):
    """Modifies config options based on runtime flag values.

    Can be overridden by derived classes to add support for specific flags.

    Args:
      config_values: dict mapping config option names to provided values. May
          be modified by this function.
      flag_values: flags.FlagValues. Runtime flags that may override the
          provided config values.
    """
    super(AwsPlacementGroupSpec, cls)._ApplyFlags(config_values, flag_values)
    config_values['aws_placement_group_style'] = FLAGS.aws_placement_group_style

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AwsPlacementGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({
        'aws_placement_group_style': (option_decoders.EnumDecoder, {
            'valid_values': _PLACEMENT_GROUP_OPTIONS,
            'default': _PLACEMENT_GROUP_DEFAULT
        })
    })
    return result


class AwsPlacementGroup(placement_group.BasePlacementGroup):
  """Object representing an AWS Placement Group."""

  CLOUD = providers.AWS

  def __init__(self, aws_placement_group_spec):
    """Init method for AwsPlacementGroup.

    Args:
      aws_placement_group_spec: Object containing the
        information needed to create an AwsPlacementGroup.
    """
    super(AwsPlacementGroup, self).__init__(aws_placement_group_spec)
    self.name = (
        'perfkit-%s-%s' % (FLAGS.run_uri, str(uuid.uuid4())[-12:]))
    self.region = util.GetRegionFromZone(self.zone)
    self.strategy = aws_placement_group_spec.aws_placement_group_style

  def _Create(self):
    """Creates the Placement Group."""
    create_cmd = util.AWS_PREFIX + [
        'ec2',
        'create-placement-group',
        '--region=%s' % self.region,
        '--group-name=%s' % self.name,
        '--strategy=%s' % self.strategy]
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    """Deletes the Placement Group."""
    delete_cmd = util.AWS_PREFIX + [
        'ec2',
        'delete-placement-group',
        '--region=%s' % self.region,
        '--group-name=%s' % self.name]
    # Failed deletes are ignorable (probably already deleted).
    vm_util.IssueCommand(delete_cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the Placement Group exists."""
    describe_cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-placement-groups',
        '--region=%s' % self.region,
        '--filter=Name=group-name,Values=%s' % self.name]
    stdout, _ = util.IssueRetryableCommand(describe_cmd)
    response = json.loads(stdout)
    placement_groups = response['PlacementGroups']
    assert len(placement_groups) < 2, 'Too many placement groups.'
    return bool(placement_groups)
