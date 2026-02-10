# Redis Benchmark Results Parser

This tool parses Redis benchmark results from PerfKitBenchmarker (PKB) JSON output and generates standardized CSV reports for performance analysis and comparison.

## Features

- Extracts comprehensive Redis and Memtier metrics from PKB JSONL format
- **Appends to existing CSV files** - preserves previous test results
- **Saves output in the same directory as input file by default** - easy to organize results
- Supports multiple latency percentiles (p50, p90, p95, p99, p99.5, p99.9, p99.95, p99.99)
- Template-based column ordering for consistent output
- Automatic test numbering that increments across runs
- Extracts infrastructure and configuration details
- **Streamlined output with only relevant metrics** (43 columns)

## Files

- `redis_parser.py` - Main parser script
- `Redis_Template.csv` - Template defining output CSV structure and column order (43 columns)
- `README_Redis.md` - This documentation

## Usage

```bash
python3 redis_parser.py <json_file> [template_file] [output_csv]
```

### Parameters

- `json_file` - Path to PKB results JSON file (required)
- `template_file` - Path to template CSV file (optional, defaults to "Redis_Template.csv")
- `output_csv` - Path to output CSV file (optional, defaults to "redis_results.csv" in the same directory as input file)

### Examples

#### Basic usage - Run from the benchmark results folder
```bash
# Navigate to the runs folder
cd pkb_temp/runs/602420af

# Run parser (output saved in current directory as redis_results.csv)
python3 ../../../utils/parse-csv/redis_parser.py perfkitbenchmarker_results.json
```

#### Run from project root
```bash
# Output will be saved in pkb_temp/runs/602420af/redis_results.csv
python3 utils/parse-csv/redis_parser.py pkb_temp/runs/602420af/perfkitbenchmarker_results.json
```

#### Specify custom output file
```bash
python3 utils/parse-csv/redis_parser.py \
  pkb_temp/runs/602420af/perfkitbenchmarker_results.json \
  utils/parse-csv/Redis_Template.csv \
  my_custom_results.csv
```

## Input Format

The parser expects PKB's JSONL (JSON Lines) output format where each line is a valid JSON object representing a metric. Example:

```json
{"metric": "Ops Throughput", "value": 131350.36, "unit": "ops/s", "timestamp": 1763230013.637874, "test": "redis_memtier", "labels": "|memtier_threads:32|redis_server_version:7.2.6|..."}
```

## Output CSV Columns

The output CSV includes 43 streamlined columns (as defined in `Redis_Template.csv`):

### Test Information (7 columns)
- Test #, Test_ID, Test tool - PKB or direct, Benchmark_Type
- Test_Date, Test_Duration, Workload_Type, Test_Description

### Infrastructure (2 columns)
- Platform, Region_Zone

### Redis Configuration (2 columns)
- Cluster_Mode, AOF_Enabled

### Performance Metrics (10 columns)
- **Ops_Throughput** - Operations per second
- **KB_Throughput** - Kilobytes per second
- **Latency_Avg_ms** - Average latency in milliseconds
- **Latency_p50_ms** through **Latency_p99_99_ms** - Latency percentiles

### Server/Client Details (6 columns)
- Server_Machine_Type, Server_CPU_Cores, Server_NUMA_Nodes
- Client_Machine_Type, Client_CPU_Cores, Client_NUMA_Nodes

### Container/Kubernetes Details (5 columns)
- Container_Cluster_Version, Container_Cluster_Type
- Container_Cluster_Zone, Container_Cluster_Project, Nodepools

### Timing Metrics (3 columns)
- Total_Runtime_Sec, Time_to_Create_Sec, Time_to_Delete_Sec

### Metadata (5 columns)
- run_uri, owner, product_name, perfkitbenchmarker_version, scenario, Notes

## Append Behavior

The parser automatically **appends** to existing CSV files instead of overwriting them:

- If the output CSV file exists, new results are appended as additional rows
- Test numbers automatically increment from the last existing test number
- Headers are only written when creating a new file
- This allows you to accumulate results from multiple benchmark runs in a single CSV

### Example Workflow

```bash
# Navigate to first run folder
cd pkb_temp/runs/run1

# First run - creates file with Test #1
python3 ../../../utils/parse-csv/redis_parser.py perfkitbenchmarker_results.json

# Navigate to second run folder
cd ../run2

# Second run - appends with Test #2 to run2/redis_results.csv
python3 ../../../utils/parse-csv/redis_parser.py perfkitbenchmarker_results.json

# Or to combine multiple runs into one CSV:
cd pkb_temp/runs
python3 ../../utils/parse-csv/redis_parser.py run1/perfkitbenchmarker_results.json Redis_Template.csv combined_results.csv
python3 ../../utils/parse-csv/redis_parser.py run2/perfkitbenchmarker_results.json Redis_Template.csv combined_results.csv
python3 ../../utils/parse-csv/redis_parser.py run3/perfkitbenchmarker_results.json Redis_Template.csv combined_results.csv
```

## Customizing the Template

You can modify `Redis_Template.csv` to:
- Change column order
- Add new columns (update parser code accordingly)
- Remove columns you don't need

The parser will match fields from the benchmark results to template columns and preserve the exact order defined in the template.

## What Changed from Original Template

The template has been streamlined from 67 to 43 columns by removing fields that were consistently empty:
- Machine_Type, CPU_Cores, Memory_GB, Storage_Type (replaced by Server/Client specific fields)
- Redis_Version, Memtier_Version, Thread_Count, Client_Count, Pipeline (label extraction issues)
- Data_Size_Bytes, Key_Maximum, Run_Duration_Sec, Read_Write_Ratio
- IO_Threads, Latency_Min_ms, Latency_Max_ms
- CPU utilization metrics (Server/Client CPU stats)

All meaningful metrics and infrastructure details are retained in the streamlined version.

## Troubleshooting

### "No metrics found in JSON file"
- Verify the JSON file is from a Redis benchmark run
- Check that the file contains lines with `"metric": "Ops Throughput"` or similar

### Missing columns in output
- Ensure all desired columns are defined in the template CSV
- Check that the JSON file contains the corresponding label data

### File path issues
- Use absolute paths or paths relative to your current directory
- The script will search for the template in the script's directory if not found
- By default, output is saved in the same directory as the input JSON file

## Notes

- The parser is designed specifically for PKB's Redis Memtier benchmark output
- Metrics are extracted from the labels field in the JSONL format
- The script handles missing fields gracefully by leaving those columns empty
- All numeric values are preserved as-is from the JSON file
- Output CSV is saved in the same directory as input JSON by default for easy organization

## Support

For issues or questions:
1. Check that your JSON file matches the expected PKB output format
2. Verify the template file path is correct
3. Review the console output for specific error messages
