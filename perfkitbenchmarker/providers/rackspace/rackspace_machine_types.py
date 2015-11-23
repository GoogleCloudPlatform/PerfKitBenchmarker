# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module contains constants, baked-in assumptions and utility functions related
 to Rackspace flavors, aka. machine types."""

from perfkitbenchmarker import disk

STANDARD1_CLASS = 'standard1'
GENERAL1_CLASS = 'general1'
COMPUTE1_CLASS = 'compute1'
MEMORY1_CLASS = 'memory1'
IO1_CLASS = 'io1'
PERFORMANCE1_CLASS = 'performance1'
PERFORMANCE2_CLASS = 'performance2'
ONMETAL_CLASS = 'onmetal'

ROOT_DISK = 'root_disk'
LOCAL = 'local'
REMOTE = 'remote'

RACKSPACE_DISK_TYPES = {
    (disk.STANDARD, STANDARD1_CLASS): ROOT_DISK,
    (disk.STANDARD, GENERAL1_CLASS): REMOTE,
    (disk.STANDARD, COMPUTE1_CLASS): REMOTE,
    (disk.STANDARD, MEMORY1_CLASS): REMOTE,
    (disk.STANDARD, IO1_CLASS): REMOTE,
    (disk.STANDARD, PERFORMANCE1_CLASS): REMOTE,
    (disk.STANDARD, PERFORMANCE2_CLASS): REMOTE,
    (disk.STANDARD, ONMETAL_CLASS): ROOT_DISK,
    (disk.LOCAL, STANDARD1_CLASS): ROOT_DISK,
    (disk.LOCAL, GENERAL1_CLASS): ROOT_DISK,
    (disk.LOCAL, COMPUTE1_CLASS): ROOT_DISK,
    (disk.LOCAL, MEMORY1_CLASS): ROOT_DISK,
    (disk.LOCAL, IO1_CLASS): LOCAL,
    (disk.LOCAL, PERFORMANCE1_CLASS): ROOT_DISK,
    (disk.LOCAL, PERFORMANCE2_CLASS): LOCAL,
    (disk.LOCAL, ONMETAL_CLASS): ROOT_DISK,
    (disk.REMOTE_SSD, GENERAL1_CLASS): REMOTE,
    (disk.REMOTE_SSD, COMPUTE1_CLASS): REMOTE,
    (disk.REMOTE_SSD, MEMORY1_CLASS): REMOTE,
    (disk.REMOTE_SSD, IO1_CLASS): REMOTE,
    (disk.REMOTE_SSD, PERFORMANCE1_CLASS): REMOTE,
    (disk.REMOTE_SSD, PERFORMANCE2_CLASS): REMOTE,
}

REMOTE_BOOT_DISK_SIZE_GB = 50
ONMETAL_IO_LOCAL_DISKS_NUM = 2
ONMETAL_DISK_MODEL = 'NWD-BLP4-1600'


def GetRackspaceDiskSpecs(machine_type, flavor_specs):
  """Returns a dict with data about the disk assumptions based on the given
  machine type.

  Returns
    A dict with key a strings for each disk property
  """
  flavor_class = flavor_specs.get('class')
  disk_specs = {}

  if flavor_class in (IO1_CLASS, PERFORMANCE2_CLASS,):
    data_disks = int(flavor_specs.get('number_of_data_disks', 0))
    disk_specs['max_local_disks'] = data_disks
  elif machine_type == 'onmetal-io1':
    disk_specs['max_local_disks'] = ONMETAL_IO_LOCAL_DISKS_NUM
  else:
    disk_specs['max_local_disks'] = 1  # Boot disk

  disk_specs['remote_disk_support'] = flavor_class not in (ONMETAL_CLASS,
                                                           STANDARD1_CLASS,)
  return disk_specs


def GetRackspaceDiskType(machine_type, flavor_class, scratch_disk_type):
  """Returns the type of disk to use for the machine_type and scratch_disk_type
  combination.

  Returns
    A string from (ROOT_DISK, REMOTE, LOCAL)
  """
  key = (scratch_disk_type, flavor_class,)
  rackspace_disk_type = RACKSPACE_DISK_TYPES.get(key)
  if machine_type == 'onmetal-io1' and rackspace_disk_type == ROOT_DISK:
    return LOCAL

  return rackspace_disk_type
