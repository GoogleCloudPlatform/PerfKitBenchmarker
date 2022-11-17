#!/bin/bash

wget https://github.com/TPC-Council/HammerDB/releases/download/v4.5/HammerDB-4.5-Linux.tar.gz

sudo tar -zxvf HammerDB-4.5-Linux.tar.gz -C /var/lib/google

sudo mv /var/lib/google/HammerDB-4.5/* /var/lib/google/HammerDB
