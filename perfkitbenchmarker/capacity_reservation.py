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

"""Module containing abstract class for a capacity reservation for VMs."""

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

FLAGS = flags.FLAGS

flags.DEFINE_bool('use_capacity_reservations', False,
                  'Whether to use capacity reservations for virtual '
                  'machines. Only supported on AWS.')


def GetResourceClass(cloud):
  """Get the CapacityReservation class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for.

  Returns:
    Cloud-specific implementation of BaseCapacityReservation.
  """
  return resource.GetResourceClass(BaseCapacityReservation, CLOUD=cloud)


class BaseCapacityReservation(resource.BaseResource):
  """An object representing a CapacityReservation."""

  RESOURCE_TYPE = 'BaseCapacityReservation'

  def __init__(self, vm_group):
    super(BaseCapacityReservation, self).__init__()
    self.vm_group = vm_group
