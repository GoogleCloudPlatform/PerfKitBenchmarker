#!/bin/bash
set -e
SIZE_GB=$1
SWAPFILE_PATH=$2
fallocate -l ${SIZE_GB}G ${SWAPFILE_PATH}
chmod 600 ${SWAPFILE_PATH}
LOOP=$(losetup -f)
losetup "$LOOP" ${SWAPFILE_PATH}
mkswap "$LOOP"
swapon "$LOOP"
echo "swap loop device: $LOOP"
