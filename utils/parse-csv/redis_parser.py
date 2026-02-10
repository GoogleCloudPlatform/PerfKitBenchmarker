#!/usr/bin/env python3
"""
Redis benchmark results parser for PerfKitBenchmarker (PKB) output.

This script parses Redis benchmark results from PKB's JSON output format and generates
a standardized CSV report for analysis and comparison.

Features:
- Appends to existing CSV files (preserves previous results)
- Extracts comprehensive Redis and Memtier metrics
- Supports PKB JSONL output format
- Template-based column ordering

Usage:
    python3 redis_parser.py <json_file> [template_file] [output_csv]

Example:
    python3 redis_parser.py pkb_temp/runs/602420af/perfkitbenchmarker_results.json Redis_Template.csv redis_results.csv
"""

import os
import re
import json
import csv
import sys
from datetime import datetime

def parse_labels(label_string):
    """Parse pipe-delimited label string into dictionary."""
    labels = {}
    if not label_string:
        return labels
    
    # Split by |, then by :
    parts = label_string.split('|')
    for part in parts:
        part = part.strip()
        if ':' in part:
            key, value = part.split(':', 1)
            labels[key.strip()] = value.strip()
    
    return labels

def extract_metrics_from_json(json_file):
    """Extract Redis benchmark metrics from PKB JSON results file."""
    all_metrics = []
    
    try:
        print(f"Reading JSON file: {json_file}")
        
        # Read the JSON file line by line (JSONL format)
        metrics_data = {}
        run_uri = None
        owner = None
        product_name = None
        timestamp = None
        
        with open(json_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    
                    # Extract common metadata
                    if 'run_uri' in record and not run_uri:
                        run_uri = record['run_uri']
                    if 'owner' in record and not owner:
                        owner = record['owner']
                    if 'product_name' in record and not product_name:
                        product_name = record['product_name']
                    
                    # Extract timestamp (use the first one found)
                    if 'timestamp' in record and not timestamp:
                        timestamp = record.get('timestamp')

                    # Parse labels
                    labels = parse_labels(record.get('labels', ''))
                    
                    # Get metric name and value
                    metric_name = record.get('metric', '')
                    metric_value = record.get('value', 0)
                    
                    # Store key metrics
                    if metric_name == 'Ops Throughput':
                        metrics_data['ops_throughput'] = metric_value
                    elif metric_name == 'KB Throughput':
                        metrics_data['kb_throughput'] = metric_value
                    elif metric_name == 'Latency':
                        metrics_data['latency_avg'] = metric_value
                        # Also extract percentile latencies from labels
                        if 'p50_latency' in labels:
                            metrics_data['latency_p50'] = float(labels['p50_latency'])
                        if 'p90_latency' in labels:
                            metrics_data['latency_p90'] = float(labels['p90_latency'])
                        if 'p95_latency' in labels:
                            metrics_data['latency_p95'] = float(labels['p95_latency'])
                        if 'p99_latency' in labels:
                            metrics_data['latency_p99'] = float(labels['p99_latency'])
                        if 'p99.5_latency' in labels:
                            metrics_data['latency_p99_5'] = float(labels['p99.5_latency'])
                        if 'p99.9_latency' in labels:
                            metrics_data['latency_p99_9'] = float(labels['p99.9_latency'])
                        if 'p99.950_latency' in labels:
                            metrics_data['latency_p99_95'] = float(labels['p99.950_latency'])
                        if 'p99.990_latency' in labels:
                            metrics_data['latency_p99_99'] = float(labels['p99.990_latency'])
                    elif metric_name == 'End to End Runtime':
                        metrics_data['total_runtime'] = metric_value
                    elif metric_name == 'Time to Create':
                        metrics_data['time_to_create'] = metric_value
                    elif metric_name == 'Time to Delete':
                        metrics_data['time_to_delete'] = metric_value
                    
                    # Store labels for later use (from any record with complete labels)
                    if labels and len(labels) > len(metrics_data.get('labels', {})):
                        metrics_data['labels'] = labels
                        
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse JSON line: {e}")
                    continue
        
        if not metrics_data:
            print("No metrics found in JSON file")
            return []
        
        # Extract information from labels
        labels = metrics_data.get('labels', {})
        
        # Build the metrics dictionary
        test_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if timestamp:
            try:
                test_date = datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                pass

        metrics = {
            'Test_ID': f"redis-{run_uri}" if run_uri else "redis-test",
            'Test tool - PKB or direct': 'PKB',
            'Benchmark_Type': 'Redis Memtier',
            'Test_Date': test_date,
            'Test_Duration': labels.get('memtier_run_duration', '300'),
            'Workload_Type': labels.get('scenario', 'redis_memtier_caching_single_node'),
            'Test_Description': f"Redis benchmark using Memtier on {labels.get('cloud', 'GCP')}",
            'Platform': f"{labels.get('cloud', 'GCP')} - {labels.get('container_cluster_cluster_type', 'Kubernetes')}",
            'Region_Zone': labels.get('container_cluster_zone', labels.get('zone', '')),
            
            # Redis Configuration
            'Redis_Version': labels.get('redis_server_version', ''),
            'Memtier_Version': labels.get('memtier_version', ''),
            'Thread_Count': labels.get('memtier_threads', ''),
            'Client_Count': labels.get('memtier_clients', ''),
            'Pipeline': labels.get('memtier_pipeline', ''),
            'Data_Size_Bytes': labels.get('memtier_data_size', ''),
            'Key_Maximum': labels.get('memtier_key_maximum', ''),
            'Run_Duration_Sec': labels.get('memtier_run_duration', ''),
            'Read_Write_Ratio': labels.get('memtier_ratio', ''),
            'Cluster_Mode': labels.get('memtier_cluster_mode', 'False'),
            'AOF_Enabled': labels.get('redis_aof', 'False'),
            'IO_Threads': labels.get('redis_server_io_threads', ''),
            
            # Performance Metrics
            'Ops_Throughput': metrics_data.get('ops_throughput', ''),
            'KB_Throughput': metrics_data.get('kb_throughput', ''),
            'Latency_Avg_ms': metrics_data.get('latency_avg', ''),
            'Latency_p50_ms': metrics_data.get('latency_p50', ''),
            'Latency_p90_ms': metrics_data.get('latency_p90', ''),
            'Latency_p95_ms': metrics_data.get('latency_p95', ''),
            'Latency_p99_ms': metrics_data.get('latency_p99', ''),
            'Latency_p99_5_ms': metrics_data.get('latency_p99_5', ''),
            'Latency_p99_9_ms': metrics_data.get('latency_p99_9', ''),
            'Latency_p99_95_ms': metrics_data.get('latency_p99_95', ''),
            'Latency_p99_99_ms': metrics_data.get('latency_p99_99', ''),
            
            # Server Information
            'Server_Machine_Type': labels.get('servers_vm_platform', '') + ' - ' + labels.get('container_cluster_machine_type', ''),
            'Server_CPU_Cores': labels.get('servers_num_cpus', ''),
            'Server_NUMA_Nodes': labels.get('servers_numa_node_count', ''),
            
            # Client Information
            'Client_Machine_Type': labels.get('clients_vm_platform', '') + ' - ' + extract_client_machine_type(labels),
            'Client_CPU_Cores': labels.get('clients_num_cpus', ''),
            'Client_NUMA_Nodes': labels.get('clients_numa_node_count', ''),
            
            # Container/Kubernetes Information
            'Container_Cluster_Version': labels.get('container_cluster_version', ''),
            'Container_Cluster_Type': labels.get('container_cluster_cluster_type', ''),
            'Container_Cluster_Zone': labels.get('container_cluster_zone', ''),
            'Container_Cluster_Project': labels.get('container_cluster_project', ''),
            'Nodepools': labels.get('container_cluster_nodepools', ''),
            
            # Timing Metrics
            'Total_Runtime_Sec': metrics_data.get('total_runtime', ''),
            'Time_to_Create_Sec': metrics_data.get('time_to_create', ''),
            'Time_to_Delete_Sec': metrics_data.get('time_to_delete', ''),
            
            # Metadata
            'run_uri': run_uri or '',
            'owner': owner or '',
            'product_name': product_name or 'PerfKitBenchmarker',
            'perfkitbenchmarker_version': labels.get('perfkitbenchmarker_version', ''),
            'scenario': labels.get('scenario', ''),
        }
        
        all_metrics.append(metrics)
        print(f"Successfully extracted metrics from {json_file}")
        
    except Exception as e:
        print(f"Error processing JSON file {json_file}: {e}")
        import traceback
        traceback.print_exc()
    
    return all_metrics

def extract_client_machine_type(labels):
    """Extract client machine type from nodepools info."""
    nodepools = labels.get('container_cluster_nodepools', '')
    if 'clients' in nodepools:
        # Try to extract machine_type from the nodepools string
        match = re.search(r"'clients'.*?'machine_type':\s*'([^']+)'", nodepools)
        if match:
            return match.group(1)
    return ''

def get_next_test_number(output_file):
    """Get the next test number by reading existing CSV file."""
    if not os.path.exists(output_file):
        return 1
    
    try:
        with open(output_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                # Get the last test number
                last_test_num = rows[-1].get('Test #', '0')
                try:
                    return int(last_test_num) + 1
                except (ValueError, TypeError):
                    return len(rows) + 1
            return 1
    except Exception as e:
        print(f"Warning: Could not read existing file for test numbering: {e}")
        return 1

def apply_template_formatting(metrics, template_file, output_file):
    """Apply template column order and formatting."""
    if not template_file or not os.path.exists(template_file):
        print(f"Warning: Template file {template_file} not found")
        return metrics, None
    
    try:
        # Read template columns
        with open(template_file, 'r', newline='') as f:
            reader = csv.reader(f)
            template_columns = next(reader, [])
        
        if not template_columns:
            print("Warning: Template file has no headers")
            return metrics, None
        
        print(f"Found {len(template_columns)} columns in template")
        
        # Normalize template columns (strip whitespace)
        template_columns = [col.strip() for col in template_columns]
        
        # Get next test number
        next_test_num = get_next_test_number(output_file)
        
        # Add Test # to each metric
        for i, metric in enumerate(metrics):
            metric['Test #'] = str(next_test_num + i)
        
        # Filter metrics to only include template columns
        filtered_metrics = []
        for metric in metrics:
            filtered_metric = {}
            for col in template_columns:
                filtered_metric[col] = metric.get(col, '')
            filtered_metrics.append(filtered_metric)
        
        return filtered_metrics, template_columns
        
    except Exception as e:
        print(f"Error applying template formatting: {e}")
        return metrics, None

def write_metrics_to_csv(metrics_data, output_file):
    """Write metrics to CSV file (append mode)."""
    if isinstance(metrics_data, tuple):
        metrics, template_columns = metrics_data
    else:
        metrics = metrics_data
        template_columns = None
    
    if not metrics:
        print("No metrics to write")
        return
    
    # Determine fieldnames
    if template_columns:
        fieldnames = template_columns
    else:
        # Use all fields from first metric
        fieldnames = list(metrics[0].keys())
    
    # Check if file exists
    file_exists = os.path.exists(output_file)
    
    # Write/append to CSV
    mode = 'a' if file_exists else 'w'
    with open(output_file, mode, newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # Write header only if file is new
        if not file_exists:
            writer.writeheader()
            print(f"Created new CSV file: {output_file}")
        else:
            print(f"Appending to existing CSV file: {output_file}")
        
        writer.writerows(metrics)
    
    action = "Appended" if file_exists else "Wrote"
    print(f"{action} {len(metrics)} benchmark run(s) to {output_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 redis_parser.py <json_file> [template_file] [output_csv]")
        print("Example: python3 redis_parser.py perfkitbenchmarker_results.json Redis_Template.csv redis_results.csv")
        sys.exit(1)
    
    # Parse arguments
    json_file = sys.argv[1]
    template_file = sys.argv[2] if len(sys.argv) > 2 else "Redis_Template.csv"
    output_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Handle relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Make paths absolute if relative
    if not os.path.isabs(json_file) and not os.path.exists(json_file):
        possible_path = os.path.join(os.getcwd(), json_file)
        if os.path.exists(possible_path):
            json_file = possible_path
    
    if not os.path.isabs(template_file) and not os.path.exists(template_file):
        template_path = os.path.join(script_dir, template_file)
        if os.path.exists(template_path):
            template_file = template_path
    
    # If output file not specified, save in same directory as input file
    if output_file is None:
        input_dir = os.path.dirname(os.path.abspath(json_file))
        output_file = os.path.join(input_dir, "redis_results.csv")
    
    # Check if input file exists
    if not os.path.exists(json_file):
        print(f"Error: Input file not found: {json_file}")
        sys.exit(1)
    
    # Extract metrics
    print(f"Processing Redis benchmark results from: {json_file}")
    metrics = extract_metrics_from_json(json_file)
    
    if not metrics:
        print("No metrics extracted. Exiting.")
        sys.exit(1)
    
    # Apply template formatting
    print(f"Applying template formatting from: {template_file}")
    formatted_metrics = apply_template_formatting(metrics, template_file, output_file)
    
    # Write to CSV
    print(f"Writing results to: {output_file}")
    write_metrics_to_csv(formatted_metrics, output_file)
    
    print("Parsing completed successfully!")

if __name__ == "__main__":
    main()
