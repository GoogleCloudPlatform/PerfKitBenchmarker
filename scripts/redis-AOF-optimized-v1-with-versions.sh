#!/bin/bash

# Script to run Redis AOF Optimized V1 Benchmark (Session Storage)
# Scenario: Session Storage (1:1 R/W Ratio, AOF Enabled)
# Based on Optimized V5 (includes Pod Monitoring)
# Uses GKE's Mutating Admission Controller to inject optimization config

set -e

# Default Configuration
TIMESTAMP=$(date "+%d%b%H%M")
OUTPUT_CSV="gke_redis_aof_optimized_v1_results_${TIMESTAMP}.csv"
POD_METRICS_CSV="pod_metrics_aof_optimized_${TIMESTAMP}.csv"
TEMPLATE_FILE="utils/parse-csv/Redis_Template.csv"
PARSER_SCRIPT="utils/parse-csv/redis_parser.py"

# Default Parameter Values
SERVER_MACHINE_TYPE="c4d-standard-8"
CLIENT_MACHINE_TYPE="c4d-standard-32"
ZONE="us-east1-b"
PROJECT=""
DURATION="300"
LOG_LEVEL="info"
ITERATIONS="10"
RUN_URI_BASE=""
RATIO="1:1"
AOF="True"
SNAPSHOTS="True"
SCENARIO="redis_aof_optimized_v1"
RUN_STAGE=""
REDIS_TYPE="redis"
REDIS_VERSION="8.0.5"
POLL_INTERVAL=30

# Help Function
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -m, --machine_type <type>   Server machine type (default: c4d-standard-8)"
    echo "  -c, --client_type <type>    Client machine type (default: c4d-standard-32)"
    echo "  -z, --zone <zone>           GCP Zone (default: us-east1-b)"
    echo "  -p, --project <project>     GCP Project ID (default: use gcloud config)"
    echo "  -d, --duration <seconds>    Run duration (default: 300)"
    echo "  -n, --iterations <count>    Number of sequential runs (default: 10)"
    echo "  -r, --run_uri <uri>         Base run URI (appended with iteration number)"
    echo "  -l, --log_level <level>     Log level (default: info)"
    echo "  --ratio <set:get>           Write:Read ratio (default: 1:1)"
    echo "  --aof <True|False>          Enable AOF persistence (default: True)"
    echo "  --snapshots <True|False>    Enable RDB Snapshots (default: True)"
    echo "  --run_stage <stages>        PKB run stages (e.g., 'provision,prepare' or 'run,cleanup')"
    echo "  --redis_type <type>         Redis type: redis or valkey (default: redis)"
    echo "  --redis_version <version>   Version to install (default: 8.0.5)"
    echo "  --poll_interval <seconds>   Pod monitoring poll interval (default: 30)"
    echo "  -h, --help                  Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --machine_type c4-standard-4 --client_type c4-standard-32 \\"
    echo "     --zone us-east1-b --project sada-gke-benchmarking2 \\"
    echo "     --duration 300 --iterations 10 --run_uri v5c4 --log_level error"
    exit 1
}

# Parse Arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -m|--machine_type) SERVER_MACHINE_TYPE="$2"; shift ;;
        -c|--client_type) CLIENT_MACHINE_TYPE="$2"; shift ;;
        -z|--zone) ZONE="$2"; shift ;;
        -p|--project) PROJECT="$2"; shift ;;
        -d|--duration) DURATION="$2"; shift ;;
        -n|--iterations) ITERATIONS="$2"; shift ;;
        -r|--run_uri) RUN_URI_BASE="$2"; shift ;;
        -l|--log_level) LOG_LEVEL="$2"; shift ;;
        --ratio) RATIO="$2"; shift ;;
        --aof) AOF="$2"; shift ;;
        --snapshots) SNAPSHOTS="$2"; shift ;;
        --run_stage) RUN_STAGE="$2"; shift ;;
        --redis_type) REDIS_TYPE="$2"; shift ;;
        --redis_version) REDIS_VERSION="$2"; shift ;;
        --poll_interval) POLL_INTERVAL="$2"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# Set git repo based on redis type
if [ "$REDIS_TYPE" = "valkey" ]; then
    REDIS_GIT_REPO="https://github.com/valkey-io/valkey.git"
else
    REDIS_GIT_REPO="https://github.com/redis/redis.git"
fi

# Update output CSV with version info
OUTPUT_CSV="gke_redis_aof_optimized_v1_${REDIS_TYPE}_${REDIS_VERSION}_results_${TIMESTAMP}.csv"
POD_METRICS_CSV="pod_metrics_aof_optimized_${REDIS_TYPE}_${REDIS_VERSION}_${TIMESTAMP}.csv"

# Validate run_uri if provided (must be alphanumeric)
if [ -n "$RUN_URI_BASE" ]; then
    if [[ ! "$RUN_URI_BASE" =~ ^[a-zA-Z0-9]+$ ]]; then
        echo "Error: run_uri must be alphanumeric only (letters and numbers)"
        echo "Provided: $RUN_URI_BASE"
        exit 1
    fi
fi

# Determine Disk Type based on Machine Type (Server)
if [[ "$SERVER_MACHINE_TYPE" == c4* ]]; then
    SERVER_DISK_TYPE="hyperdisk-balanced"
else
    SERVER_DISK_TYPE="pd-ssd"
fi

# Determine Disk Type based on Machine Type (Client)
if [[ "$CLIENT_MACHINE_TYPE" == c4* ]]; then
    CLIENT_DISK_TYPE="hyperdisk-balanced"
else
    CLIENT_DISK_TYPE="pd-ssd"
fi

# Construct Project Flag
PROJECT_FLAG=""
if [ -n "$PROJECT" ]; then
    PROJECT_FLAG="--project=$PROJECT"
fi

# Initialize pod metrics CSV with headers
echo "timestamp,run_uri,iteration,pod_name,pod_type,cpu_millicores,memory_mib,peak_cpu_millicores,peak_memory_mib" > "$POD_METRICS_CSV"

# Display Configuration
echo "======================================================="
echo "Starting Redis AOF Optimized V1 Benchmark (Session Storage)"
echo "======================================================="
echo "Configuration:"
echo "  Iterations:       $ITERATIONS (each with new cluster)"
echo "  Redis Type:       $REDIS_TYPE"
echo "  Redis Version:    $REDIS_VERSION"
echo "  Server Type:      $SERVER_MACHINE_TYPE"
echo "  Client Type:      $CLIENT_MACHINE_TYPE"
echo "  Server Disk:      $SERVER_DISK_TYPE"
echo "  Client Disk:      $CLIENT_DISK_TYPE"
echo "  Zone:             $ZONE"
echo "  Project:          ${PROJECT:-[Using Default]}"
echo "  Duration:         $DURATION sec"
echo "  Ratio:            $RATIO"
echo "  AOF Enabled:      $AOF"
echo "  Snapshots:        $SNAPSHOTS"
echo "  Log Level:        $LOG_LEVEL"
echo "  Poll Interval:    ${POLL_INTERVAL}s"
echo "  Run URI Base:     ${RUN_URI_BASE:-[Auto-generated]}"
echo "  Output CSV:       $OUTPUT_CSV"
echo "  Pod Metrics CSV:  $POD_METRICS_CSV"
echo "======================================================="
echo ""

# Ensure parser script exists
if [ ! -f "$PARSER_SCRIPT" ]; then
    echo "Error: Parser script not found at $PARSER_SCRIPT"
    exit 1
fi

# Function to monitor pods
monitor_pods() {
    local RUN_URI=$1
    local ITERATION=$2
    local PEAK_SERVER_CPU=0
    local PEAK_SERVER_MEM=0
    local PEAK_CLIENT_CPU=0
    local PEAK_CLIENT_MEM=0
    local PODS_SEEN=false
    
    # Wait for pods to appear (up to 5 minutes)
    local WAIT_RETRIES=30
    local PODS_FOUND=false
    
    for ((w=1; w<=WAIT_RETRIES; w++)); do
        if kubectl top pod --namespace=default 2>/dev/null | grep -q "$RUN_URI"; then
            PODS_FOUND=true
            break
        fi
        sleep 10
    done
    
    if [ "$PODS_FOUND" = false ]; then
        echo "  [Monitoring] Timeout waiting for pods to appear."
        return
    fi
    
    while true; do
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        
        # Get metrics for all pods in the run
        POD_METRICS=$(kubectl top pod --namespace=default 2>/dev/null | grep "$RUN_URI" || true)
        
        # If no pods found
        if [ -z "$POD_METRICS" ]; then
            if [ "$PODS_SEEN" = true ]; then
                # Pods were running but are now gone -> Benchmark finished
                break
            fi
            # Pods haven't started yet -> Keep waiting
            sleep "$POLL_INTERVAL"
            continue
        fi

        # Pods found, mark as seen
        PODS_SEEN=true
        
        # Process each pod
        while IFS= read -r line; do
            POD_NAME=$(echo "$line" | awk '{print $1}')
            CPU_RAW=$(echo "$line" | awk '{print $2}')
            MEM_RAW=$(echo "$line" | awk '{print $3}')
            
            # Extract numeric values (remove 'm' and 'Mi')
            CPU=$(echo "$CPU_RAW" | sed 's/[^0-9]//g')
            MEM=$(echo "$MEM_RAW" | sed 's/[^0-9]//g')
            
            # Determine if server or client and track peaks
            if [[ "$POD_NAME" == *"-3" ]] || [[ "$POD_NAME" == *"client"* ]]; then
                # Client pod
                POD_TYPE="client"
                if [ "$MEM" -gt "$PEAK_CLIENT_MEM" ]; then
                    PEAK_CLIENT_MEM=$MEM
                fi
                if [ "$CPU" -gt "$PEAK_CLIENT_CPU" ]; then
                    PEAK_CLIENT_CPU=$CPU
                fi
                PEAK_CPU=$PEAK_CLIENT_CPU
                PEAK_MEM=$PEAK_CLIENT_MEM
            else
                # Server pod
                POD_TYPE="server"
                if [ "$MEM" -gt "$PEAK_SERVER_MEM" ]; then
                    PEAK_SERVER_MEM=$MEM
                fi
                if [ "$CPU" -gt "$PEAK_SERVER_CPU" ]; then
                    PEAK_SERVER_CPU=$CPU
                fi
                PEAK_CPU=$PEAK_SERVER_CPU
                PEAK_MEM=$PEAK_SERVER_MEM
            fi
            
            # Write to CSV
            echo "$TIMESTAMP,$RUN_URI,$ITERATION,$POD_NAME,$POD_TYPE,$CPU,$MEM,$PEAK_CPU,$PEAK_MEM" >> "$POD_METRICS_CSV"
        done <<< "$POD_METRICS"
        
        # Wait before next poll
        sleep "$POLL_INTERVAL"
    done
    
    # Log peak values
    echo "  [Monitoring] Peak Server: CPU=${PEAK_SERVER_CPU}m, MEM=${PEAK_SERVER_MEM}Mi"
    echo "  [Monitoring] Peak Client: CPU=${PEAK_CLIENT_CPU}m, MEM=${PEAK_CLIENT_MEM}Mi"
}

# Loop for specified iterations
for ((i=1; i<=ITERATIONS; i++)); do
    # Generate custom Run URI
    if [ -n "$RUN_URI_BASE" ]; then
        RUN_URI="${RUN_URI_BASE}${i}"
    else
        DAY=$(date +%d)
        TYPE="o"  # optimized
        
        # Determine version shorthand
        REDIS_MAJOR=$(echo $REDIS_VERSION | cut -d. -f1)
        if [ "$REDIS_TYPE" = "redis" ]; then
            VERSION_SHORT="r${REDIS_MAJOR}"
        else
            VERSION_SHORT="v${REDIS_MAJOR}"
        fi
        
        # Determine machine type shorthand
        if [[ "$SERVER_MACHINE_TYPE" == n2-standard-4 ]]; then
            MACHINE_SHORT="n24"
        elif [[ "$SERVER_MACHINE_TYPE" == n2-standard-8 ]]; then
            MACHINE_SHORT="n28"
        elif [[ "$SERVER_MACHINE_TYPE" == c4-standard-4 ]]; then
            MACHINE_SHORT="c44"
        elif [[ "$SERVER_MACHINE_TYPE" == c4-standard-8 ]]; then
            MACHINE_SHORT="c48"
        elif [[ "$SERVER_MACHINE_TYPE" == c4d-standard-8 ]]; then
            MACHINE_SHORT="c48"
        else
            # Fallback: extract first letter and last digit
            MACHINE_SHORT=$(echo "$SERVER_MACHINE_TYPE" | sed 's/[^a-z0-9]//g' | head -c 3)
        fi
        
        MINUTE=$(date +%M)
        RUN_URI="${DAY}${TYPE}${VERSION_SHORT}${MACHINE_SHORT}${MINUTE}${i}"
    fi

    echo "-------------------------------------------------------"
    echo "Run $i/$ITERATIONS - URI: $RUN_URI"
    echo "Timestamp: $(date)"
    echo "-------------------------------------------------------"

    # Start pod monitoring in background
    echo "  Starting pod monitoring (${POLL_INTERVAL}s interval)..."
    monitor_pods "$RUN_URI" "$i" &
    MONITOR_PID=$!

    # Build run_stage flag if specified
    RUN_STAGE_FLAG=""
    if [ -n "$RUN_STAGE" ]; then
        RUN_STAGE_FLAG="--run_stage=$RUN_STAGE"
    fi

    # Run PKB Benchmark (Full lifecycle: provision → prepare → run → cleanup)
    python3 pkb.py \
        --benchmarks=gke_optimized_redis_memtier_v2 \
        --cloud=GCP \
        --vm_platform=Kubernetes \
        --zone=$ZONE \
        $PROJECT_FLAG \
        --os_type=ubuntu2404 \
        --gke_release_channel=rapid \
        --gke_max_cpu=1000 \
        --gke_max_memory=4000 \
        --gke_redis_v2_machine_type=$SERVER_MACHINE_TYPE \
        --gke_redis_v2_server_machine_type=$SERVER_MACHINE_TYPE \
        --gke_redis_v2_client_machine_type=$CLIENT_MACHINE_TYPE \
        --gke_redis_v2_enable_optimization=True \
        --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.servers.vm_spec.GCP.boot_disk_type=$SERVER_DISK_TYPE \
        --config_override=gke_optimized_redis_memtier_v2.container_cluster.nodepools.clients.vm_spec.GCP.boot_disk_type=$CLIENT_DISK_TYPE \
        --config_override=gke_optimized_redis_memtier_v2.vm_groups.servers.vm_spec.Kubernetes.host_network=True \
        --memtier_key_pattern=R:R \
        --memtier_distinct_client_seed=True \
        --memtier_key_maximum=6400000 \
        --redis_server_enable_snapshots=$SNAPSHOTS \
        --redis_server_version=$REDIS_VERSION \
        --redis_git_repo=$REDIS_GIT_REPO \
        --redis_type=$REDIS_TYPE \
        --redis_eviction_policy=allkeys-lru \
        --iostat=True \
        --sar=False \
        --metadata=cloud:GCP \
        --metadata=geo:${ZONE%-*} \
        --metadata=load_key_ratio:0.4 \
        --metadata=max_key_ratio:0.4 \
        --memtier_data_size=1024 \
        --memtier_ratio=$RATIO \
        --memtier_threads=32 \
        --memtier_clients=12 \
        --memtier_run_duration=$DURATION \
        --memtier_run_count=1 \
        --memtier_pipeline=1 \
        --redis_aof=$AOF \
        --metadata=scenario:$SCENARIO \
        --metadata=redis_type:$REDIS_TYPE \
        --metadata=redis_version:$REDIS_VERSION \
        --metadata=iteration:$i \
        --create_and_boot_post_task_delay=180 \
        --temp_dir=./pkb_temp \
        --owner=$(whoami | tr '.' '-') \
        --log_level=$LOG_LEVEL \
        --run_uri=$RUN_URI \
        $RUN_STAGE_FLAG \
        --accept_licenses

    # Wait for monitor to finish (pods are gone)
    echo "  Waiting for pod monitoring to complete..."
    wait $MONITOR_PID 2>/dev/null || true

    # Parse and Append Results
    JSON_RESULTS="./pkb_temp/runs/$RUN_URI/perfkitbenchmarker_results.json"

    if [ -f "$JSON_RESULTS" ]; then
        echo "Parsing results for run $RUN_URI..."
        python3 "$PARSER_SCRIPT" "$JSON_RESULTS" "$TEMPLATE_FILE" "$OUTPUT_CSV"
        echo "Results appended to $OUTPUT_CSV"
    else
        echo "Error: Results file not found at $JSON_RESULTS"
    fi

    echo "Run $i/$ITERATIONS completed."
    echo ""

    # Optional sleep between runs to allow GCP cleanup
    if [ $i -lt $ITERATIONS ]; then
        echo "Waiting 30 seconds before next iteration..."
        sleep 30
    fi
done

echo "======================================================="
echo "All $ITERATIONS runs completed."
if [ -n "$RUN_URI_BASE" ]; then
    echo "Run URIs used: ${RUN_URI_BASE}1 through ${RUN_URI_BASE}${ITERATIONS}"
fi
echo "Final Results:     $OUTPUT_CSV"
echo "Pod Metrics:       $POD_METRICS_CSV"
echo "======================================================="
