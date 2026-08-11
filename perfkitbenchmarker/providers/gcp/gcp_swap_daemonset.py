
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
"""GCP-specific Swap DaemonSet resource."""

import logging
import textwrap

from perfkitbenchmarker import errors
from perfkitbenchmarker.resources.container_service import swap_daemonset


class GcpSwapDaemonSet(swap_daemonset.SwapDaemonSet):
  """GCP implementation of SwapDaemonSet."""

  CLOUD = 'GCP'

  def SetupSwap(
      self,
      swap_type: str,
      enable_dmcrypt: bool,
      swap_size_gb: int,
      io2_volume_id: str = '',
  ) -> None:
    """Configure swap on the cluster node (GKE)."""
    del io2_volume_id  # Unused for GCP
    self._SetupGkeSwap(swap_type, enable_dmcrypt, swap_size_gb)

  def _SetupGkeSwap(
      self, swap_type: str, enable_dmcrypt: bool, swap_size_gb: int
  ) -> None:
    """Dispatch to the correct GKE storage-type sub-function."""
    if swap_type == 'auto':
      lssd_out, _ = self.PodExec(
          "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
          "grep -v 'nvme0' | awk '{print $1}' | head -1",
          ignore_failure=True,
      )
      swap_type = 'lssd' if lssd_out.strip() else 'hyperdisk'
    if swap_type == 'lssd':
      self._SetupGkeLssdSwap(enable_dmcrypt, swap_size_gb)
    elif swap_type == 'boot_disk':
      self._SetupGkeBootdiskSwap(swap_size_gb, enable_dmcrypt)
    else:
      self._SetupGkeHyperdiskSwap(enable_dmcrypt, swap_size_gb)

  def _SetupGkeHyperdiskSwap(
      self, enable_dmcrypt: bool, swap_size_gb: int
  ) -> None:
    """Configure dm-crypt swap on hyperdisk-balanced (GKE default).

    Falls back to a loop device when no dedicated data disk is attached.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
      swap_size_gb: Swap size in GB.
    """
    logging.info('[swap_encryption] GKE: setting up dm-crypt on hyperdisk')
    boot_out, _ = self.PodExec(
        'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
        ignore_failure=True,
    )
    boot_base = boot_out.strip() or 'nvme0n1'
    disk_out, _ = self.PodExec(
        'lsblk -d -o NAME,TYPE | awk \'$2=="disk"{print $1}\' '
        f"| grep -v '^{boot_base}$' | head -1",
        ignore_failure=True,
    )
    disk_name = disk_out.strip()
    if not disk_name:
      logging.info(
          '[swap_encryption] No dedicated data disk — falling back to '
          'loop device on /mnt/stateful_partition (dmcrypt=%s)',
          enable_dmcrypt,
      )
      self._SetupGkeLoopDeviceSwap(swap_size_gb)
      return
    disk = f'/dev/{disk_name}'
    logging.info(
        '[swap_encryption] GKE: swap target disk: %s  dmcrypt=%s',
        disk,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    wipefs -a {disk} 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {disk})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{disk}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: dm-crypt swap active on '
          '/dev/mapper/swap_encrypted'
      )
    else:
      self.PodExec(f'mkswap {disk} && swapon {disk}')
      logging.info('[swap_encryption] GKE: plain swap active on %s', disk)

  def _SetupGkeLoopDeviceSwap(self, swap_size_gb: int) -> None:
    """Plain loop-device swap for single-disk GKE nodes.

    dm-crypt is skipped — blocked by COS kernel namespace restrictions.
    Phase 1 (fio) is skipped for plain loop devices.

    Args:
      swap_size_gb: Swap size in GB.
    """
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    self.PodExec(
        textwrap.dedent(f"""
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | \
      while read dev; do swapoff "$dev" 2>/dev/null || true
        losetup -d "$dev" 2>/dev/null || true; done
    rm -f {backing}
  """),
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] GKE: creating %dG backing file', swap_size_gb
    )
    self.PodExec(
        f'fallocate -l {swap_size_gb}G {backing} 2>/dev/null || '
        f'truncate -s {swap_size_gb}G {backing}',
    )
    loop_out, _ = self.PodExec(
        f'LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} '
        '&& echo "$LOOP"',
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] losetup failed – output: {loop_out!r}'
      )
    self.PodExec(f'mkswap {loop_dev}')
    self.PodExec(f'swapon {loop_dev}')
    logging.info(
        '[swap_encryption] GKE: plain loop swap active on %s (Phase 1 skipped)',
        loop_dev,
    )

  def _SetupGkeBootdiskSwap(
      self, swap_size_gb: int, enable_dmcrypt: bool
  ) -> None:
    """Swap on the OS boot disk — methodology Table 0 rows 1-4.

    Creates a loop-backed swap file on /mnt/stateful_partition.
    Requires Ubuntu node image (dm-crypt blocked on COS from pod).

    Args:
      swap_size_gb: Swap size in GB.
      enable_dmcrypt: Whether to apply dm-crypt.
    """
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    logging.info(
        '[swap_encryption] GKE: boot-disk swap (%dG, dmcrypt=%s)',
        swap_size_gb,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | while read d
    do swapoff "$d" 2>/dev/null || true; losetup -d "$d" 2>/dev/null || true; done
    rm -f {backing}
  """),
        ignore_failure=True,
    )
    self.PodExec(
        f'fallocate -l {swap_size_gb}G {backing} 2>/dev/null || '
        f'truncate -s {swap_size_gb}G {backing}',
    )
    loop_out, _ = self.PodExec(
        f'LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} '
        '&& echo "$LOOP"',
    )
    loop_dev = (
        loop_out.strip().splitlines()[-1].strip() if loop_out.strip() else ''
    )
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] boot-disk losetup failed: {loop_out!r}'
      )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: boot-disk dm-crypt swap active on '
          '/dev/mapper/swap_encrypted'
      )
    else:
      self.PodExec(f'mkswap {loop_dev} && swapon {loop_dev}')
      logging.info(
          '[swap_encryption] GKE: boot-disk plain swap active on %s', loop_dev
      )

  def _SetupGkeLssdSwap(self, enable_dmcrypt: bool, swap_size_gb: int) -> None:
    """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd).

    Falls back to hyperdisk path if no clean LSSD found, or to the
    stateful-loop path if all LSSDs are partitioned.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
      swap_size_gb: Swap size in GB.
    """
    logging.info('[swap_encryption] GKE: setting up LSSD RAID-0 swap')
    self.PodExec(
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    topo, _ = self.PodExec(
        'lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null',
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] block device topology:\n%s', (topo or '').strip()
    )
    lssd_out, _ = self.PodExec(
        textwrap.dedent("""
        for d in $(lsblk -dno NAME,ROTA | awk '$2==0{print $1}')
        do
          if lsblk -no TYPE "/dev/$d" 2>/dev/null | grep -q '^part$'; then continue; fi
          if lsblk -no MOUNTPOINT "/dev/$d" 2>/dev/null | grep -q '[^[:space:]]'; then continue; fi
          echo "/dev/$d"
        done
      """),
        ignore_failure=True,
    )
    devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
    if not devices:
      logging.info(
          '[swap_encryption] No clean local SSD — falling back to hyperdisk'
      )
      self._SetupGkeHyperdiskSwap(enable_dmcrypt, swap_size_gb)
      return
    device_list = ' '.join(devices)
    n = len(devices)
    logging.info(
        '[swap_encryption] GKE: LSSD RAID-0 across %d device(s): %s '
        ' dmcrypt=%s',
        n,
        device_list,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    for dev in {device_list}
    do
      test -b "$dev" || continue
      devname=$(basename "$dev")
      for holder in /sys/block/$devname/holders/*
      do
        test -e "$holder" || continue
        h=$(basename "$holder")
        if echo "$h" | grep -q "^md"; then
          mdadm --stop /dev/$h 2>/dev/null || true
        else
          dmsetup remove --force --noudevrules --noudevsync /dev/$h 2>/dev/null || true
        fi
      done
      mounts=$(awk -v d="$dev" '$1==d{{print $2}}' /proc-host/mounts 2>/dev/null || true)
      for mp in $mounts; do umount -f "$mp" 2>/dev/null || true; done
    done
    swapoff -a 2>/dev/null || true
    swapoff /dev/mapper/pkb_swap 2>/dev/null || true
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync pkb_swap 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop --scan 2>/dev/null || true
    mdadm --zero-superblock {device_list} 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f /mnt/stateful_partition/pkb_swap.img 2>/dev/null || true
    sleep 2
  """),
        ignore_failure=True,
    )
    raw_check_out, _ = self.PodExec(
        textwrap.dedent(f"""
        for dev in {device_list}
        do
          if lsblk -ln -o TYPE "$dev" 2>/dev/null | grep -q '^part$'
          then echo "[pkb-lssd] $dev is partitioned" >&2
          else echo "$dev"
          fi
        done
      """),
        ignore_failure=True,
    )
    raw_devices = [
        d.strip() for d in raw_check_out.strip().splitlines() if d.strip()
    ]
    if not raw_devices:
      logging.info(
          '[swap_encryption] GKE: all LSSD partitioned — falling back to '
          'stateful-loop'
      )
      self._SetupGkeLssdStatefulLoopSwap(enable_dmcrypt)
      return
    devices = raw_devices
    device_list = ' '.join(devices)
    n = len(devices)
    if n > 1:
      self.PodExec(
          textwrap.dedent(f"""
      mdadm --create /dev/md0 --force \\
        --level=0 --raid-devices={n} {device_list}
      test -b /dev/md0 || {{ echo "mdadm: /dev/md0 not created" >&2; exit 1; }}
    """),
      )
      swap_block_dev = '/dev/md0'
    else:
      swap_block_dev = devices[0]
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {swap_block_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{swap_block_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: LSSD dm-crypt swap active on %s',
          swap_block_dev,
      )
    else:
      self.PodExec(f'mkswap {swap_block_dev} && swapon {swap_block_dev}')
      logging.info(
          '[swap_encryption] GKE: LSSD plain swap active on %s', swap_block_dev
      )

  def _SetupGkeLssdStatefulLoopSwap(self, enable_dmcrypt: bool) -> None:
    """Set up swap on the LSSD partition via a loop device.

    Used when the local NVMe device is partitioned by GKE startup scripts.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
    """
    img_path = '/mnt/stateful_partition/pkb_swap.img'
    self.PodExec(
        textwrap.dedent(f"""
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f {img_path} 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    size_out, _ = self.PodExec(
        "df -P /mnt/stateful_partition | awk 'NR==2{print $4}'",
        ignore_failure=True,
    )
    avail_kb = int(size_out.strip() or '0')
    swap_gb = max(16, int(avail_kb * 0.8 / 1024 / 1024))
    logging.info(
        '[swap_encryption] GKE: LSSD stateful-loop: %d GB at %s',
        swap_gb,
        img_path,
    )
    self.PodExec(
        textwrap.dedent(f"""
    fallocate -l {swap_gb}G {img_path} 2>/dev/null || \\
      dd if=/dev/zero of={img_path} bs=1G count={swap_gb}
    chmod 600 {img_path}
    losetup --direct-io=on -f {img_path}
  """),
        timeout=300,
    )
    loop_out, _ = self.PodExec(
        f"losetup -j {img_path} | awk -F: '{{print $1}}' | head -1",
        ignore_failure=True,
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] losetup failed for {img_path}: {loop_out!r}'
      )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: LSSD stateful-loop dm-crypt active: %s',
          loop_dev,
      )
    else:
      self.PodExec(f'mkswap {loop_dev} && swapon {loop_dev}')
      logging.info(
          '[swap_encryption] GKE: LSSD stateful-loop plain swap active: %s',
          loop_dev,
      )
