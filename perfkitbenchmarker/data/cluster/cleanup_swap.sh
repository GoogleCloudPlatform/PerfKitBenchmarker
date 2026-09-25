#!/bin/bash
swapoff -a 2>/dev/null || true
swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
for backing in /var/pkb_swap_backing /run/pkb_swap_backing /mnt/stateful_partition/pkb_swap_backing; do
  losetup -j "$backing" 2>/dev/null | awk -F: '{print $1}' | while read dev; do
    losetup -d "$dev" 2>/dev/null || true
  done
  rm -f "$backing"
done
pkill -9 'stress-ng|fio' 2>/dev/null || true
