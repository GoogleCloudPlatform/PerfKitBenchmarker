#!/bin/bash

wget https://github.com/TPC-Council/HammerDB/releases/download/v4.3/HammerDB-4.3-Linux.tar.gz

sudo tar -zxvf HammerDB-4.3-Linux.tar.gz -C /var/lib/google

sudo mv /var/lib/google/HammerDB-4.3/* /var/lib/google/HammerDB
