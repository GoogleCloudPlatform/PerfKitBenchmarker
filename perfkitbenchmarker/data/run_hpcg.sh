#!/bin/bash

# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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

HPCG_DIR=`pwd`

DATETIME=`hostname`.`date +"%Y%m%d.%H%M%S"`

# Just to get rid of warning on psg cluster node wo proper IB sw installed
# Use mca btl_tcp_if_exclude to skip docker network in DLVM.
MPIFLAGS="--mca btl tcp,self --mca btl_tcp_if_exclude docker0,lo"
LD_LIBRARY_PATH_FLAGS="-x LD_LIBRARY_PATH=/usr/local/cuda/lib64:/opt/amazon/efa/lib64:/opt/amazon/openmpi/lib64:/opt/aws-ofi-nccl/lib:/usr/local/nccl-rdma-sharp-plugins/lib:$LD_LIBRARY_PATH"
HPCG_BIN="hpcg"

echo " ****** running HPCG 9-3-20 binary=$HPCG_BIN on $NUM_GPUS GPUs ***************************** "
mpirun {{ ALLOW_RUN_AS_ROOT }} -np $NUM_GPUS $MPIFLAGS -hostfile HOSTFILE $LD_LIBRARY_PATH_FLAGS $HPCG_DIR/$HPCG_BIN | tee ./results/xhpcg-$DATETIME-output.txt
