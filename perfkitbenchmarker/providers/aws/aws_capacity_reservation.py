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

"""CapacityReservation for AWS virtual machines.

AWS EC2 has the concept of capacity reservations which allow the
user to request a reservation for a given number of VMs of a
specified shape (machine type and os type) in a given zone, for
an optionally-supplied duration. This module implements this functionaly.

A useful feature of using AwsCapacityReservation is that it allows the
user to specify a region instead of a zone, and this module will automatically
pick a zone that has capacity, and the VM(s) will then be launched in that zone.

AwsCapacityReservation modifies all the VMs in a given vm_group in the
following way:
  1. The capacity_reservation_id attribute on the VM is set after the
     reservation is created. The VM needs to reference this id during
     creation.
  2. If the user supplied a region instead of zone, then this module
     will update the zone attribute on the VM, as well as the zone
     attribute on the VM's network instance.

A run of PKB may have several capacity reservations; there is a 1:1 mapping
from AWS vm_groups to AwsCapacityReservation instances. This is because all
VMs in a VM group share the same shape and zone.
"""

import datetime
import json
import logging
from perfkitbenchmarker import capacity_reservation
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class InvalidVmGroupSizeError(Exception):
  pass


class UnsupportedOsTypeError(Exception):
  pass


class CreationError(Exception):
  pass


class AwsCapacityReservation(capacity_reservation.BaseCapacityReservation):
  """An object representing an AWS EC2 CapacityReservation."""
  CLOUD = providers.AWS

  def __init__(self, vm_group):
    if not vm_group:
      raise InvalidVmGroupSizeError(
          'AwsCapacityReservation must be initialized with at least one '
          'VM in the vm_group.')

    super(AwsCapacityReservation, self).__init__(vm_group)
    self.zone_or_region = vm_group[0].zone
    self.region = util.GetRegionFromZone(self.zone_or_region)
    self.machine_type = vm_group[0].machine_type
    self.os_type = vm_group[0].OS_TYPE
    self.vm_count = len(vm_group)

  def _Create(self):
    """Creates the AWS CapacaityReservation.

    A reservation will be created given the VM shape in self.vm_groups.
    Count is determined by the number of VMs in said group. The reservation
    will have a lifetime determined by the general PKB concept of
    timeout_minutes. If the reservation exceeds this timeout, AWS will
    cancel it automatically. The VMs in the reservation will not be deleted.
    Note that an empty capacity reservation will encur costs for the
    VM shape / count, even if no VMs are using it.

    After the reservation is created, this method updates all the VMs
    in self.vm_groups by setting the capacity_reservation_id, as well
    as the zone attributes on the VM, and the VM's network instance.

    Raises:
      UnsupportedOsTypeError: If creating a capacity reservation for the
        given os type is not supported.
      CreationError: If a capacity reservation cannot be created in the
        region (typically indicates a stockout).
    """
    if self.os_type in os_types.LINUX_OS_TYPES:
      instance_platform = 'Linux/UNIX'
    elif self.os_type in os_types.WINDOWS_OS_TYPES:
      instance_platform = 'Windows'
    else:
      raise UnsupportedOsTypeError(
          'Unsupported os_type for AWS CapacityReservation: %s.'
          % self.os_type)

    # If the user did not specify an AZ, we need to try to create the
    # CapacityReservation in a specifc AZ until it succeeds.
    # Then update the zone attribute on all the VMs in the group,
    # as well as the zone attribute on the VMs' network instance.
    if util.IsRegion(self.zone_or_region):
      zones_to_try = util.GetZonesInRegion(self.region)
    else:
      zones_to_try = [self.zone_or_region]

    end_date = (
        datetime.datetime.utcnow() +
        datetime.timedelta(minutes=FLAGS.timeout_minutes))
    for zone in zones_to_try:
      cmd = util.AWS_PREFIX + [
          'ec2',
          'create-capacity-reservation',
          '--instance-type=%s' % self.machine_type,
          '--instance-platform=%s' % instance_platform,
          '--availability-zone=%s' % zone,
          '--instance-count=%s' % self.vm_count,
          '--instance-match-criteria=targeted',
          '--region=%s' % self.region,
          '--end-date-type=limited',
          '--end-date=%s' % end_date,
      ]
      stdout, stderr, retcode = vm_util.IssueCommand(cmd,
                                                     raise_on_failure=False)
      if retcode:
        logging.info('Unable to create CapacityReservation in %s. '
                     'This may be retried. Details: %s', zone, stderr)
        continue
      json_output = json.loads(stdout)
      self.capacity_reservation_id = (
          json_output['CapacityReservation']['CapacityReservationId'])
      self._UpdateVmsInGroup(self.capacity_reservation_id, zone)
      return
    raise CreationError('Unable to create CapacityReservation in any of the '
                        'following zones: %s.' % zones_to_try)

  def _Delete(self):
    """Deletes the capacity reservation."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'cancel-capacity-reservation',
        '--capacity-reservation-id=%s' % self.capacity_reservation_id,
        '--region=%s' % self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _Exists(self):
    """Returns true if the underlying reservation exists and is active."""
    cmd = util.AWS_PREFIX + [
        'ec2',
        'describe-capacity-reservations',
        '--capacity-reservation-id=%s' % self.capacity_reservation_id,
        '--region=%s' % self.region,
    ]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode != 0:
      return False

    json_output = json.loads(stdout)
    return json_output['CapacityReservations'][0]['State'] == 'active'

  def _UpdateVmsInGroup(self, capacity_reservation_id, zone):
    """Updates the VMs in a group with necessary reservation details.

    AWS virtual machines need to reference the capacity reservation id
    during creation, so it is set on all VMs in the group. Additionally,
    this class may determine which zone to run in, so that needs to be
    updated too (on the VM, and the VM's network instance).

    Args:
      capacity_reservation_id: ID of the reservation created by this instance.
      zone: Zone chosen by this class, or if it was supplied, the zone
      provided by the user. In the latter case, setting the zone is equivalent
      to a no-op.
    """
    for vm in self.vm_group:
      vm.capacity_reservation_id = capacity_reservation_id
      vm.zone = zone
      vm.network.zone = zone
