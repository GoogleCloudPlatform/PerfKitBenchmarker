#!/bin/bash
HPCG_DIR=`pwd`

DATETIME=`hostname`.`date +"%Y%m%d.%H%M%S"`

MPIFLAGS="--mca btl tcp,sm,self"   # just to get rid of warning on psg cluster node wo proper IB sw installed
HPCG_BIN="hpcg"

echo " ****** running HPCG 3-28-17 binary=$HPCG_BIN on $NUM_GPUS GPUs ***************************** "
mpirun {{ ALLOW_RUN_AS_ROOT }} -np $NUM_GPUS $MPIFLAGS -hostfile HOSTFILE $HPCG_DIR/$HPCG_BIN | tee ./results/xhpcg-$DATETIME-output.txt
