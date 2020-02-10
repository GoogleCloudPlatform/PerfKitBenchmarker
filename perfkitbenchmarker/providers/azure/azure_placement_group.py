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

import abc
import json

from perfkitbenchmarker import flags
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers import azure
from perfkitbenchmarker.providers.azure import util


FLAGS = flags.FLAGS


class AzurePlacementGroupSpec(placement_group.BasePlacementGroupSpec):
  """Object containing the information needed to create an AzurePlacementGroup.

  Attributes:
      zone: The Azure zone the Placement Group is in.
  """

  CLOUD = providers.AZURE

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
        'resource_group': (option_decoders.StringDecoder, {'none_ok': False}),
        'placement_group_style': (option_decoders.EnumDecoder, {
            'valid_values': placement_group.PLACEMENT_GROUP_OPTIONS,
            'default': placement_group.PLACEMENT_GROUP_NONE,
        })
    })
    return result


class AzurePlacementGroup(placement_group.BasePlacementGroup):
  """Object representing an Azure Placement Group."""

  CLOUD = providers.AZURE

  def __init__(self, azure_placement_group_spec):
    """Init method for AzurePlacementGroup.

    Args:
      azure_placement_group_spec: Object containing the
        information needed to create an AzurePlacementGroup.
    """
    super(AzurePlacementGroup, self).__init__(azure_placement_group_spec)
    self.resource_group = azure_placement_group_spec.resource_group
    self.name = '%s-%s' % (self.resource_group, self.zone)
    self.location = util.GetLocationFromZone(self.zone)
    self.strategy = azure_placement_group_spec.placement_group_style

  @abc.abstractmethod
  def AddVmArgs(self):
    """List of arguments to add to vm creation."""
    raise NotImplementedError()


class AzureAvailSet(AzurePlacementGroup):
  """Object representing an Azure Availability Set."""

  def _Create(self):
    """Create the availability set."""
    create_cmd = [
        azure.AZURE_PATH, 'vm', 'availability-set', 'create',
        '--resource-group', self.resource_group, '--name', self.name
    ]
    if self.location:
      create_cmd.extend(['--location', self.location])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    pass

  @vm_util.Retry()
  def _Exists(self):
    """Returns True if the availability set exists."""
    show_cmd = [
        azure.AZURE_PATH, 'vm', 'availability-set', 'show', '--output', 'json',
        '--resource-group', self.resource_group, '--name', self.name
    ]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    return bool(json.loads(stdout))

  def AddVmArgs(self):
    """Returns Azure command to add VM to availability set."""
    return ['--availability-set', self.name]


class AzureProximityGroup(AzurePlacementGroup):
  """Object representing an Azure Proximity Placement Group."""

  def _Create(self):
    """Create the Proximity Placement Group."""
    create_cmd = [
        azure.AZURE_PATH, 'ppg', 'create',
        '--resource-group', self.resource_group, '--name', self.name
    ]
    if self.location:
      create_cmd.extend(['--location', self.location])
    vm_util.IssueCommand(create_cmd)

  def _Delete(self):
    pass

  @vm_util.Retry()
  def _Exists(self):
    """Returns True if the Proximity Placement Group exists."""
    show_cmd = [
        azure.AZURE_PATH, 'ppg', 'show', '--output', 'json',
        '--resource-group', self.resource_group, '--name', self.name
    ]
    stdout, _, _ = vm_util.IssueCommand(show_cmd, raise_on_failure=False)
    return bool(json.loads(stdout))

  def AddVmArgs(self):
    """Returns Azure command to add VM to placement group."""
    return ['--ppg', self.name]
