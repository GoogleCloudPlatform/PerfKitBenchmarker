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

"""Class to represent Placement Group Object.

Top-level Placement Group implementation.
Cloud specific implementations of Placement Group needed.
"""


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS


def GetPlacementGroupSpecClass(cloud):
  """Returns the PlacementGroupSpec class corresponding to 'cloud'."""
  return spec.GetSpecClass(BasePlacementGroupSpec, CLOUD=cloud)


def GetPlacementGroupClass(cloud):
  """Returns the PlacementGroup class corresponding to 'cloud'."""
  return resource.GetResourceClass(BasePlacementGroup,
                                   CLOUD=cloud)


class BasePlacementGroupSpec(spec.BaseSpec):
  """Storing various data about a placement group.

  Attributes:
    zone: The zone the in which the placement group will launch.
  """

  SPEC_TYPE = 'BasePlacementGroupSpec'
  CLOUD = None

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Can be overridden by derived classes to add options or impose additional
    requirements on existing options.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
          The pair specifies a decoder class and its __init__() keyword
          arguments to construct in order to decode the named option.
    """
    result = super(BasePlacementGroupSpec, cls)._GetOptionDecoderConstructions()
    result.update({'zone': (option_decoders.StringDecoder, {'none_ok': True})})
    return result


class BasePlacementGroup(resource.BaseResource):
  """Base class for Placement Groups.

  This class holds Placement Group methods and attributes relating to the
  Placement Groups as a cloud
  resource.

  Attributes:
    zone: The zone the Placement Group was launched in.
  """

  RESOURCE_TYPE = 'BasePlacementGroup'

  def __init__(self, placement_group_spec):
    """Initialize BasePlacementGroup class.

    Args:
      placement_group_spec: placement_group.BasePlacementGroupSpec object of the
        placement group.
    """
    super(BasePlacementGroup, self).__init__()
    self.zone = placement_group_spec.zone
