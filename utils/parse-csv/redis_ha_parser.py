import json
import csv
import sys
import os


def parse_results(json_file, output_prefix):
  data = []
  try:
    with open(json_file, 'r') as f:
      for line in f:
        line = line.strip()
        if line:
          try:
            data.append(json.loads(line))
          except json.JSONDecodeError as e:
            print(f'Warning: Skipping invalid JSON line: {e}')
            continue
  except FileNotFoundError:
    print(f'Error: File {json_file} not found.')
    return

  # Define the 3 workload types and their corresponding output files
  workloads = {
      'baseline_primary_1:1': f'{output_prefix}_baseline_1_1.csv',
      'ha_writes_primary': f'{output_prefix}_writes_1_0.csv',
      'ha_reads_replica': f'{output_prefix}_reads_0_1.csv',
  }

  # Initialize CSV writers for each workload
  writers = {}
  files = {}

  # Headers based on standard memtier results
  headers = [
      'Run URI',
      'Timestamp',
      'Workload Type',
      'Throughput (ops/s)',
      'Avg Latency (ms)',
      'P99 Latency (ms)',
      'KB/sec',
  ]

  for w_type, filename in workloads.items():
    f = open(filename, 'w', newline='')
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writers[w_type] = writer
    files[w_type] = f

  print(f'Processing results from {json_file}...')

  count = 0
  for sample in data:
    metadata = sample.get('metadata', {})
    workload_type = metadata.get('workload_type')
    metric = sample.get('metric')

    # We are primarily interested in 'Ops Throughput' as the main record
    # But memtier results in PKB are split across multiple samples (throughput, latency, etc.)
    # This simple parser assumes we want to aggregate or just dump the main metrics.
    # However, PKB's JSON structure usually has one sample per metric.
    # To make a clean CSV row, we need to group by iteration/timestamp.

    # SIMPLIFIED APPROACH: Just dump the Ops Throughput samples for now,
    # as they contain most metadata. For a full table, we'd need more complex grouping.

    if metric == 'Ops Throughput' and workload_type in writers:
      row = {
          'Run URI': sample.get('run_uri'),
          'Timestamp': sample.get('timestamp'),
          'Workload Type': workload_type,
          'Throughput (ops/s)': sample.get('value'),
          'Avg Latency (ms)': metadata.get('avg_latency'),
          'P99 Latency (ms)': metadata.get('p99_latency'),
          'KB/sec': metadata.get('kb_per_sec'),
      }
      writers[workload_type].writerow(row)
      count += 1

  # Close all files
  for f in files.values():
    f.close()

  print(f'Successfully processed {count} throughput records.')
  print(f'Created files:')
  for filename in workloads.values():
    print(f' - {filename}')


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print(
        'Usage: python3 redis_ha_parser.py <json_results_file>'
        ' <output_csv_prefix>'
    )
    sys.exit(1)

  json_file = sys.argv[1]
  output_prefix = sys.argv[2]
  parse_results(json_file, output_prefix)
