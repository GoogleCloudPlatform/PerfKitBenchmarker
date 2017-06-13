#!/bin/bash
HPCG_DIR=`pwd`

DATETIME=`hostname`.`date +"%Y%m%d.%H%M%S"`

MPIFLAGS="--mca btl tcp,sm,self"   # just to get rid of warning on psg cluster node wo proper IB sw installed

HPCG_BIN=xhpcg-3.1_gcc_485_cuda8061_ompi_1_10_2_sm_35_sm_50_sm_60_ver_3_28_17

echo " ****** running HPCG 3-28-17 binary=$HPCG_BIN on $NUM_GPUS GPUs ***************************** "
mpirun -np $NUM_GPUS $MPIFLAGS -hostfile HOSTFILE $HPCG_DIR/$HPCG_BIN | tee ./results/xhpcg-$DATETIME-output.txt
