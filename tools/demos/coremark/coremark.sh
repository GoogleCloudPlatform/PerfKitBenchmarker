#!/bin/bash
# Run Perfkitbenchmarker

exe=$HOME/PerfKitBenchmarker/pkb.py

# array of arguments to pass to PKB
args=()
# benchmark
args+=(--benchmarks=coremark)
args+=(--coremark_parallelism_method=SOCKET)
args+=(--os_type=ubuntu1804)
args+=(--run_stage_iterations=5)
# GCP args
args+=(--cloud=GCP)
args+=(--machine_type=n1-standard-4)
args+=(--gcp_min_cpu_platform=skylake)
args+=(--zone=us-central1-c)

"${exe}" "${args[@]}"
