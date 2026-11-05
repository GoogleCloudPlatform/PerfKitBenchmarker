#!/bin/bash
set -e
echo 1 > /sys/module/zswap/parameters/enabled
echo lz4 > /sys/module/zswap/parameters/compressor
echo 20 > /sys/module/zswap/parameters/max_pool_percent
echo z3fold > /sys/module/zswap/parameters/zpool
