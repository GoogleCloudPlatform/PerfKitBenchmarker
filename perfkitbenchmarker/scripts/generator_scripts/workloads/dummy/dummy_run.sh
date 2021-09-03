#!/bin/bash
sudo sysctl -w vm.nr_hugepages=$1

mkdir -p ~/dummy_results

cat /proc/meminfo | tee ~/dummy_results/results.txt