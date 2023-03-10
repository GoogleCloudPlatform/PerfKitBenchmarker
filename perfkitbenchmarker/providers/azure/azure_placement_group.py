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
"""Class to represent an Azure Placement Group object.

Cloud specific implementations of Placement Group.
"""

import json

from absl import flags
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import util

FLAGS = flags.FLAGS

PROXIMITY_PLACEMENT_GROUP = 'proximity-placement-group'
AVAILABILITY_SET = 'availability-set'
_CLI_STRATEGY_ARGS_DICT = {
    PROXIMITY_PLACEMENT_GROUP: ['ppg'],
    AVAILABILITY_SET: ['vm', 'availability-set']
    }


class AzurePlacementGroupSpec(placement_group.BasePlacementGroupSpec):
  """Object containing the information needed to create an AzurePlacementGroup.

  Attributes:
      zone: The Azure zone the Placement Group is in.
  """

  CLOUD = provider_info.AZURE

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(AzurePlacementGroupSpec,
                   cls)._GetOptionDecoderConstructions()
    result.update({
        'resource_group': (option_decoders.StringDecoder, {
            'none_ok': False
        }),
        'placement_group_style': (option_decoders.EnumDecoder, {
            'valid_values':
                set([
                    PROXIMITY_PLACEMENT_GROUP,
                    AVAILABILITY_SET
                ] + list(placement_group.PLACEMENT_GROUP_OPTIONS)),
            'default':
                placement_group.PLACEMENT_GROUP_NONE,
        })
    })
    return result


class AzurePlacementGroup(placement_group.BasePlacementGroup):
  """Object representing an Azure Placement Group."""

  CLOUD = provider_info.AZURE

  def __init__(self, azure_placement_group_spec):
    """Init method for AzurePlacementGroup.

    Args:
      azure_placement_group_spec: Object containing the information needed to
        create an AzurePlacementGroup.
    """
    super(AzurePlacementGroup, self).__init__(azure_placement_group_spec)
    self.resource_group = azure_placement_group_spec.resource_group
    self.name = '%s-%s' % (self.resource_group, self.zone)
    self.region = util.GetRegionFromZone(self.zone)
    self.strategy = azure_placement_group_spec.placement_group_style
    if self.strategy == placement_group.PLACEMENT_GROUP_CLOSEST_SUPPORTED:
      self.strategy = PROXIMITY_PLACEMENT_GROUP

  def _Create(self):
    """Create the placement group."""
    create_cmd = [azure.AZURE_PATH] + _CLI_STRATEGY_ARGS_DICT[self.strategy] + [
        'create', '--resource-group', self.resource_group, '--name', self.name
    ]
    if self.region:
      create_cmd.extend(['--location', self.region])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    pass

  @vm_util.Retry()
  def _Exists(self):
    """Returns True if the placement group exists."""
    show_cmd = [azure.AZURE_PATH] + _CLI_STRATEGY_ARGS_DICT[self.strategy] + [
        'show', '--output', 'json', '--resource-group', self.resource_group,
        '--name', self.name
    ]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    return bool(json.loads(stdout))

  def AddVmArgs(self):
    """Returns Azure command to add VM to placement group."""
    return ['--' + _CLI_STRATEGY_ARGS_DICT[self.strategy][-1], self.name]
