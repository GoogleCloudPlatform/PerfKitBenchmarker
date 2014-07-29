#!/bin/bash
# Prepare data for object_storage_benchmark and copy_benchmark. File size
# distribution simulates cloud storage workload. All files add up to ~256MB.
mkdir data;
cd data;
for i in {0..12}; do dd bs=1KB count=16 if=/dev/urandom of=file-$i.dat; done
for i in {13..26}; do dd bs=1KB count=32 if=/dev/urandom of=file-$i.dat; done
for i in {27..53}; do dd bs=1KB count=64 if=/dev/urandom of=file-$i.dat; done
for i in {54..62}; do dd bs=2KB count=64 if=/dev/urandom of=file-$i.dat; done
for i in {63..71}; do dd bs=8KB count=64 if=/dev/urandom of=file-$i.dat; done
for i in {72..89}; do dd bs=16KB count=64 if=/dev/urandom of=file-$i.dat; done
for i in {90..95}; do dd bs=1MB count=$((32-(99-i)*2)) if=/dev/urandom of=file-$i.dat; done
for i in {96..99}; do dd bs=1MB count=32 if=/dev/urandom of=file-$i.dat; done
