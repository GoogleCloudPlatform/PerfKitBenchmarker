#!/bin/bash

# Script to monitor Kubernetes pod memory and CPU usage
# Usage: ./monitor_pods.sh <run_uri> <output_file> <poll_interval_seconds>

set -e

RUN_URI=${1:-"unknown"}
OUTPUT_FILE=${2:-"pod_metrics_${RUN_URI}.csv"}
POLL_INTERVAL=${3:-30}

# Initialize peak tracking variables
PEAK_SERVER_MEM=0
PEAK_SERVER_CPU=0
PEAK_CLIENT_MEM=0
PEAK_CLIENT_CPU=0

# Create output file with headers
echo "timestamp,run_uri,pod_name,cpu_millicores,memory_mib,peak_cpu_millicores,peak_memory_mib" > "$OUTPUT_FILE"

echo "==================================================="
echo "Pod Monitoring Started"
echo "==================================================="
echo "Run URI:        $RUN_URI"
echo "Output File:    $OUTPUT_FILE"
echo "Poll Interval:  ${POLL_INTERVAL}s"
echo "==================================================="

# Function to extract numeric value from kubectl top output
extract_metric() {
    local value=$1
    # Remove 'm' for millicores or 'Mi' for MiB
    echo "$value" | sed 's/[^0-9]//g'
}

# Monitor until pods are gone (benchmark finished)
while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    
    # Get metrics for all pods in the run
    POD_METRICS=$(kubectl top pod --namespace=default 2>/dev/null | grep "$RUN_URI" || true)
    
    # If no pods found, continue waiting (pods might be starting up)
    if [ -z "$POD_METRICS" ]; then
        sleep "$POLL_INTERVAL"
        continue
    fi
    
    # Process each pod
    while IFS= read -r line; do
        POD_NAME=$(echo "$line" | awk '{print $1}')
        CPU_RAW=$(echo "$line" | awk '{print $2}')
        MEM_RAW=$(echo "$line" | awk '{print $3}')
        
        # Extract numeric values
        CPU=$(extract_metric "$CPU_RAW")
        MEM=$(extract_metric "$MEM_RAW")
        
        # Determine if server or client and track peaks
        if [[ "$POD_NAME" == *"-3" ]] || [[ "$POD_NAME" == *"client"* ]]; then
            # Client pod
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
        echo "$TIMESTAMP,$RUN_URI,$POD_NAME,$CPU,$MEM,$PEAK_CPU,$PEAK_MEM" >> "$OUTPUT_FILE"
        
        echo "[$TIMESTAMP] $POD_NAME: CPU=${CPU}m (peak: ${PEAK_CPU}m), MEM=${MEM}Mi (peak: ${PEAK_MEM}Mi)"
    done <<< "$POD_METRICS"
    
    # Wait before next poll
    sleep "$POLL_INTERVAL"
done

echo ""
echo "==================================================="
echo "Monitoring Complete - Peak Values"
echo "==================================================="
echo "Server - Peak CPU: ${PEAK_SERVER_CPU}m"
echo "Server - Peak MEM: ${PEAK_SERVER_MEM}Mi"
echo "Client - Peak CPU: ${PEAK_CLIENT_CPU}m"
echo "Client - Peak MEM: ${PEAK_CLIENT_MEM}Mi"
echo "==================================================="
echo "Full metrics saved to: $OUTPUT_FILE"
