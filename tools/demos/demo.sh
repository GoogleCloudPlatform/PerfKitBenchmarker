#!/bin/bash

set -e

RUN_URI=$(uuidgen | head -c8)
BENCHMARK="$1"
LABEL="machine_type"
SCRIPT_TYPE="raw"

if [ "$BENCHMARK" = "boot_time" ]; then
METRIC="Boot Time"
XAXIS="Boot Time (seconds)"
NUM_RUNS=20
elif [ "$BENCHMARK" = "networking" ]; then
METRIC="TCP_RR_Latency_Histogram"
XAXIS="TCP_RR Latency (us)"
NUM_RUNS=10
SCRIPT_TYPE='histogram'
LABEL="vm_1_machine_type"
elif [ "$BENCHMARK" = "memory" ]; then
METRIC="latency"
XAXIS="Latency (ns)"
NUM_RUNS=10
elif [ "$BENCHMARK" = "compute" ]; then
METRIC="STRESS_NG_GEOMEAN"
XAXIS="Geometric Mean of operations per second"
NUM_RUNS=10
else
echo "Benchmark name not recognized."
exit 1
fi

PROCESS_SCRIPT="./process_${SCRIPT_TYPE}_samples.py"
PLOT_SCRIPT="./plot_${SCRIPT_TYPE}_data.R"

RESULTS_FILE="/tmp/perfkitbenchmarker/runs/${RUN_URI}/perfkitbenchmarker_results.json"

echo 'Running PKB...'
../../pkb.py --benchmark_config_file=demo_config.yml --benchmarks="$BENCHMARK" --flag_zip=default --run_processes=200 --num_benchmark_copies="$NUM_RUNS" --run_uri="$RUN_URI"
echo 'Processing results...'
"$PROCESS_SCRIPT" "$RESULTS_FILE" "$METRIC" "$LABEL" > "${BENCHMARK}_data.csv"
echo 'Creating graph...'
Rscript "$PLOT_SCRIPT" "$BENCHMARK" "$XAXIS"


