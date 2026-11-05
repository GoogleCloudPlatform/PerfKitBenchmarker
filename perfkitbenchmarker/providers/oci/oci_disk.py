# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing classes related to OCI block volumes.

Block volume performance is set in volume performance units (VPUs) per GB:
https://docs.oracle.com/iaas/Content/Block/Concepts/blockvolumeperformance.htm
"""

import threading
import time

from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.oci import flags as oci_flags
from perfkitbenchmarker.providers.oci import util

LOWER_COST = 'lower_cost'
BALANCED = 'balanced'
HIGHER_PERFORMANCE = 'higher_performance'
ULTRA_HIGH_PERFORMANCE = 'ultra_high_performance'

DISK_TYPE_TO_VPUS_PER_GB = {
    LOWER_COST: 0,
    BALANCED: 10,
    HIGHER_PERFORMANCE: 20,
    ULTRA_HIGH_PERFORMANCE: 30,
}

ISCSI = 'iscsi'


class OciBlockVolume(disk.BaseDisk):
  """Object representing an OCI block volume."""

  # Serializes picking a free device path and attaching to it, so that
  # concurrently attached volumes do not claim the same device.
  _attach_lock = threading.Lock()

  def __init__(self, disk_spec, name, region, availability_domain):
    super().__init__(disk_spec)
    self.name = name
    self.region = region
    self.availability_domain = availability_domain
    self.id = None
    self.attachment_id = None
    self.attachment_type = oci_flags.OCI_VOLUME_ATTACHMENT_TYPE.value
    self.iqn = None
    self.ipv4 = None
    self.port = None
    if oci_flags.OCI_VPUS_PER_GB.value is not None:
      self.vpus_per_gb = oci_flags.OCI_VPUS_PER_GB.value
    elif self.disk_type in DISK_TYPE_TO_VPUS_PER_GB:
      self.vpus_per_gb = DISK_TYPE_TO_VPUS_PER_GB[self.disk_type]
    else:
      raise errors.Config.InvalidValue(
          f'Unsupported OCI disk type {self.disk_type}. Supported types: '
          f'{", ".join(DISK_TYPE_TO_VPUS_PER_GB)}.'
      )
    self.metadata.update({
        'vpus_per_gb': self.vpus_per_gb,
        'attachment_type': self.attachment_type,
    })

  def _Create(self):
    """Creates the block volume."""
    self.create_disk_start_time = time.time()
    response, _, _ = util.IssueOciCommand(
        [
            'bv',
            'volume',
            'create',
            '--availability-domain',
            self.availability_domain,
            '--size-in-gbs',
            self.disk_size,
            '--display-name',
            self.name,
            '--vpus-per-gb',
            self.vpus_per_gb,
            '--freeform-tags',
            util.MakeFormattedDefaultTags(),
            '--wait-for-state',
            'AVAILABLE',
        ]
        + util.GetCompartmentArgs(),
        self.region,
    )
    self.id = response['data']['id']
    self.create_disk_end_time = time.time()

  def _Exists(self) -> bool:
    return bool(self.id) and util.ResourceExists(
        ['bv', 'volume', 'get', '--volume-id', self.id], self.region
    )

  def _Delete(self):
    """Deletes the block volume."""
    util.IssueOciCommand(
        [
            'bv',
            'volume',
            'delete',
            '--volume-id',
            self.id,
            '--force',
            '--wait-for-state',
            'TERMINATED',
        ],
        self.region,
        raise_on_failure=False,
    )

  def _GetFreeDevicePath(self, vm) -> str:
    """Returns an available consistent device path on the VM."""
    response, _, _ = util.IssueOciCommand(
        ['compute', 'device', 'list-instance', '--instance-id', vm.id],
        self.region,
    )
    for device in response['data']:
      if device['is-available']:
        return device['name']
    raise errors.Resource.CreationError(
        f'No free device paths left on {vm.name}.'
    )

  def _Attach(self, vm):
    """Attaches the volume to a VM at a consistent device path."""
    self.attach_start_time = time.time()
    with self._attach_lock:
      device_path = self._GetFreeDevicePath(vm)
      response, _, _ = util.IssueOciCommand(
          [
              'compute',
              'volume-attachment',
              'attach',
              '--instance-id',
              vm.id,
              '--volume-id',
              self.id,
              '--type',
              self.attachment_type,
              '--device',
              device_path,
              '--wait-for-state',
              'ATTACHED',
          ],
          self.region,
      )
    data = response['data']
    self.attachment_id = data['id']
    self.device_path = device_path
    self.vm = vm
    if self.attachment_type == ISCSI:
      self.iqn = data['iqn']
      self.ipv4 = data['ipv4']
      self.port = data['port']
      self._LoginIscsi(vm)
    self.attach_end_time = time.time()

  def _Detach(self):
    """Detaches the volume from its VM."""
    if self.attachment_type == ISCSI:
      self._LogoutIscsi(self.vm)
    util.IssueOciCommand(
        [
            'compute',
            'volume-attachment',
            'detach',
            '--volume-attachment-id',
            self.attachment_id,
            '--force',
            '--wait-for-state',
            'DETACHED',
        ],
        self.region,
    )
    self.vm = None
    self.attachment_id = None

  def _LoginIscsi(self, vm):
    target = f'-T {self.iqn} -p {self.ipv4}:{self.port}'
    vm.RemoteCommand(f'sudo iscsiadm -m node -o new {target}')
    vm.RemoteCommand(
        f'sudo iscsiadm -m node -o update -T {self.iqn} '
        '-n node.startup -v automatic'
    )
    vm.RemoteCommand(f'sudo iscsiadm -m node {target} -l')

  def _LogoutIscsi(self, vm):
    target = f'-T {self.iqn} -p {self.ipv4}:{self.port}'
    vm.RemoteCommand(f'sudo iscsiadm -m node {target} -u')
    vm.RemoteCommand(f'sudo iscsiadm -m node -o delete {target}')
