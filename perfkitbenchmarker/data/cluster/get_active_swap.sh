#!/bin/bash
ACTIVE=$(awk 'NR==2{print $1}' /proc/swaps 2>/dev/null)
if [ -n "$ACTIVE" ]; then
  echo "$ACTIVE"
elif test -e /dev/mapper/swap_encrypted; then
  echo /dev/mapper/swap_encrypted
fi
