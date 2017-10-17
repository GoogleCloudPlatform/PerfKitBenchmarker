#!/bin/bash

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

# Prepare data for object_storage_benchmark and copy_benchmark. File size
# distribution simulates cloud storage workload. All files add up to ~256MB.
mkdir data;
cd data;
if [ -z "$1" ] || [ "$1" == "normal" ]; then
  for i in {0..12}; do dd bs=1KB count=16 if=/dev/urandom of=file-$i.dat; done
  for i in {13..26}; do dd bs=1KB count=32 if=/dev/urandom of=file-$i.dat; done
  for i in {27..53}; do dd bs=1KB count=64 if=/dev/urandom of=file-$i.dat; done
  for i in {54..62}; do dd bs=2KB count=64 if=/dev/urandom of=file-$i.dat; done
  for i in {63..71}; do dd bs=8KB count=64 if=/dev/urandom of=file-$i.dat; done
  for i in {72..89}; do dd bs=16KB count=64 if=/dev/urandom of=file-$i.dat; done
  for i in {90..95}; do dd bs=1MB count=$((32-(99-i)*2)) if=/dev/urandom of=file-$i.dat; done
  for i in {96..99}; do dd bs=1MB count=32 if=/dev/urandom of=file-$i.dat; done
elif [ "$1" == "large" ]; then
  dd bs=1M count=3072 if=/dev/urandom of=file_large_3gib.dat
fi
