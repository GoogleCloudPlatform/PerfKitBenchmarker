#!/bin/bash
export FI_PROVIDER="verbs;ofi_rxm"
export FI_OFI_RXM_USE_RNDV_WRITE=1
export FI_VERBS_INLINE_SIZE=39
export I_MPI_FABRICS="shm:ofi"
export FI_UNIVERSE_SIZE=3072
