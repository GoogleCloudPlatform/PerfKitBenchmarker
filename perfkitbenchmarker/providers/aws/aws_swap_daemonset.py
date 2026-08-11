
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
"""AWS-specific Swap DaemonSet resource."""

import logging
import textwrap

from perfkitbenchmarker import errors
from perfkitbenchmarker.resources.container_service import swap_daemonset


class AwsSwapDaemonSet(swap_daemonset.SwapDaemonSet):
  """AWS implementation of SwapDaemonSet."""

  CLOUD = 'AWS'

  def SetupSwap(
      self,
      swap_type: str,
      enable_dmcrypt: bool,
      swap_size_gb: int,
      io2_volume_id: str = '',
  ) -> None:
    """Configure swap on the cluster node (EKS)."""
    del enable_dmcrypt  # Unused for AWS
    self._SetupEksSwap(swap_type, swap_size_gb, io2_volume_id)

  def _SetupEksSwap(
      self, swap_type: str, swap_size_gb: int, io2_volume_id: str = ''
  ) -> None:
    """Configure swap on EKS — Instance Store OR io2 root disk."""
    if swap_type in ('auto', 'instance_store'):
      self._SetupEksInstanceStoreSwap(swap_size_gb)
    elif swap_type == 'io2':
      self._SetupEksIo2Swap(io2_volume_id)
    else:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] Unknown EKS swap type {swap_type!r}.'
      )

  def _SetupEksInstanceStoreSwap(self, swap_size_gb: int) -> None:
    """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
    logging.info('[swap_encryption] EKS: setting up Instance Store swap')
    nvme_out, _ = self.PodExec(
        "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1"
        " || lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | grep -v"
        " 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
        ignore_failure=True,
    )
    device = nvme_out.strip()
    if not device:
      for candidate in ['/dev/nvme1n1', '/dev/nvme2n1', '/dev/xvdb']:
        exists_out, _ = self.PodExec(
            f'test -b {candidate} && echo yes || echo no', ignore_failure=True
        )
        if exists_out.strip() == 'yes':
          device = candidate
          break
    if not device:
      logging.info(
          '[swap_encryption] No Instance Store NVMe — creating swapfile'
      )
      self.SetupPlainSwapFile(swap_size_gb)
      return
    logging.info('[swap_encryption] EKS: Instance Store device: %s', device)
    self.PodExec(f'mkswap {device} && swapon {device}')
    logging.info(
        '[swap_encryption] EKS: Instance Store swap active on %s', device
    )

  def _SetupEksIo2Swap(self, io2_volume_id: str = '') -> None:
    """Swap on AWS EBS io2 volume.

    Identifies the io2 device by NVMe serial number matching io2_volume_id,
    falling back to first non-root EBS device if serial match fails.

    Args:
      io2_volume_id: EBS volume id for the io2 swap path.
    """
    logging.info('[swap_encryption] EKS: setting up io2 EBS swap')
    root_out, _ = self.PodExec(
        'lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo'
        ' nvme0n1',
        ignore_failure=True,
    )
    root_base = root_out.strip() or 'nvme0n1'
    device = ''
    target = io2_volume_id.replace('-', '')
    if target:
      ser_out, _ = self.PodExec(
          'for d in /sys/block/nvme*n1; do [ -e "$d" ] || continue; s=$(cat'
          ' "$d/device/serial" 2>/dev/null | tr -d "-" | tr -d " "); [ "$s" ='
          f' "{target}" ] && {{ echo "/dev/$(basename "$d")"; break; }}; done',
          ignore_failure=True,
      )
      device = ser_out.strip()
      if device:
        logging.info(
            '[swap_encryption] EKS: io2 matched by serial %s -> %s',
            target,
            device,
        )
    if not device:
      disk_out, _ = self.PodExec(
          'for d in /sys/block/nvme*n1 /sys/block/xvd[b-z] /sys/block/sd[b-z];'
          ' do [ -e "$d" ] || continue; n=$(basename "$d");'
          f' [ "$n" = "{root_base}" ] && continue;'
          ' m=$(cat "$d/device/model" 2>/dev/null);'
          ' echo "$m" | grep -qi "Elastic Block Store" || continue;'
          ' mnt=$(lsblk -no MOUNTPOINT "/dev/$n" 2>/dev/null | tr -d " ");'
          ' [ -n "$mnt" ] && continue; echo "/dev/$n"; break; done',
          ignore_failure=True,
      )
      device = disk_out.strip()
    if not device:
      logging.info(
          '[swap_encryption] No io2 EBS disk — creating plain swapfile'
      )
      self.SetupPlainSwapFile(32)
      return
    logging.info('[swap_encryption] EKS: io2 EBS device: %s', device)
    out, _ = self.PodExec(
        textwrap.dedent(f"""
    swapoff {device} 2>/dev/null || true
    wipefs -a {device} 2>/dev/null || true
    mkswap -f {device} && swapon {device}
    swapon --show
  """),
        ignore_failure=True,
    )
    if device not in out:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] io2 swap did not activate on {device}; '
          f'swapon --show: {out!r}'
      )
    logging.info('[swap_encryption] EKS: io2 EBS swap active on %s', device)
