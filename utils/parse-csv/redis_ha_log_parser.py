#!/usr/bin/env python3
"""
Parse Redis HA HAProxy benchmark log files to extract detailed metrics.
Designed to parse pkb.log files from gke_redis_ha_haproxy benchmark runs.
"""

import re
import csv
import sys
import os


def parse_log_file(log_path, run_uri):
  """
  Parse a single pkb.log file and extract ALL STATS tables.
  Returns a list of result dictionaries.
  """
  results = []

  with open(log_path, 'r') as f:
    content = f.read()

  # Find all "ALL STATS" blocks
  # Pattern to match the complete stats table
  pattern = (
      r'ALL STATS.*?Type\s+Ops/sec.*?Totals\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)\s+([\d.]+|---)'
  )

  matches = re.finditer(pattern, content, re.DOTALL)

  for i, match in enumerate(matches, 1):
    total_ops = float(match.group(1))
    hits = float(match.group(2))
    misses = float(match.group(3))
    avg_lat = match.group(4) if match.group(4) != '---' else None
    p50_lat = match.group(5) if match.group(5) != '---' else None
    p90_lat = match.group(6) if match.group(6) != '---' else None
    p95_lat = match.group(7) if match.group(7) != '---' else None
    p99_lat = match.group(8) if match.group(8) != '---' else None
    p99_5_lat = match.group(9) if match.group(9) != '---' else None
    p99_9_lat = match.group(10) if match.group(10) != '---' else None
    p99_950_lat = match.group(11) if match.group(11) != '---' else None
    p99_990_lat = match.group(12) if match.group(12) != '---' else None
    kb_sec = float(match.group(13))

    # Determine workload type based on pattern
    # Need to look at Sets/Gets breakdown above Totals line
    # Extract the Sets and Gets lines
    stats_block = match.group(0)
    sets_match = re.search(r'Sets\s+([\d.]+)', stats_block)
    gets_match = re.search(r'Gets\s+([\d.]+)', stats_block)

    sets_ops = float(sets_match.group(1)) if sets_match else 0.0
    gets_ops = float(gets_match.group(1)) if gets_match else 0.0

    # Infer workload type
    if sets_ops > 0 and gets_ops > 0:
      workload_type = 'baseline_primary_1:1'
    elif sets_ops > 0 and gets_ops == 0:
      workload_type = 'ha_writes_primary'
    elif sets_ops == 0 and gets_ops > 0:
      workload_type = 'ha_reads_replica'
    else:
      workload_type = 'unknown'

    result = {
        'run_uri': run_uri,
        'test_number': i,
        'workload_type': workload_type,
        'total_ops_sec': total_ops,
        'sets_ops_sec': sets_ops,
        'gets_ops_sec': gets_ops,
        'hits_sec': hits,
        'misses_sec': misses,
        'avg_latency_ms': float(avg_lat) if avg_lat else None,
        'p50_latency_ms': float(p50_lat) if p50_lat else None,
        'p90_latency_ms': float(p90_lat) if p90_lat else None,
        'p95_latency_ms': float(p95_lat) if p95_lat else None,
        'p99_latency_ms': float(p99_lat) if p99_lat else None,
        'p99_5_latency_ms': float(p99_5_lat) if p99_5_lat else None,
        'p99_9_latency_ms': float(p99_9_lat) if p99_9_lat else None,
        'p99_950_latency_ms': float(p99_950_lat) if p99_950_lat else None,
        'p99_990_latency_ms': float(p99_990_lat) if p99_990_lat else None,
        'kb_sec': kb_sec,
    }
    results.append(result)

  return results


def parse_runs(run_uris, output_prefix, base_dir='./pkb_temp/runs'):
  """
  Parse log files for multiple run URIs and generate CSV outputs.
  """
  all_results = []

  for uri in run_uris:
    log_path = os.path.join(base_dir, uri, 'pkb.log')

    if not os.path.exists(log_path):
      print(f'Warning: Log file not found for URI {uri} at {log_path}')
      continue

    print(f'Parsing log for URI: {uri}')
    try:
      results = parse_log_file(log_path, uri)
      all_results.extend(results)
      print(f'  Found {len(results)} test results')
    except Exception as e:
      print(f'  Error parsing log for {uri}: {e}')

  if not all_results:
    print('No results found.')
    return

  # Group by workload type
  workload_groups = {}
  for result in all_results:
    wtype = result['workload_type']
    if wtype not in workload_groups:
      workload_groups[wtype] = []
    workload_groups[wtype].append(result)

  # Define output files
  workload_files = {
      'baseline_primary_1:1': f'{output_prefix}_baseline_1_1.csv',
      'ha_writes_primary': f'{output_prefix}_writes_1_0.csv',
      'ha_reads_replica': f'{output_prefix}_reads_0_1.csv',
      'unknown': f'{output_prefix}_unknown.csv',
  }

  # Headers
  headers = [
      'Run URI',
      'Test Number',
      'Workload Type',
      'Total Ops/sec',
      'Sets Ops/sec',
      'Gets Ops/sec',
      'Hits/sec',
      'Misses/sec',
      'Avg Latency (ms)',
      'P50 Latency (ms)',
      'P90 Latency (ms)',
      'P95 Latency (ms)',
      'P99 Latency (ms)',
      'P99.5 Latency (ms)',
      'P99.9 Latency (ms)',
      'P99.950 Latency (ms)',
      'P99.990 Latency (ms)',
      'KB/sec',
  ]

  # Write to CSV files
  for wtype, filename in workload_files.items():
    if wtype not in workload_groups:
      continue

    with open(filename, 'w', newline='') as f:
      writer = csv.writer(f)
      writer.writerow(headers)

      for result in workload_groups[wtype]:
        row = [
            result['run_uri'],
            result['test_number'],
            result['workload_type'],
            result['total_ops_sec'],
            result['sets_ops_sec'],
            result['gets_ops_sec'],
            result['hits_sec'],
            result['misses_sec'],
            result['avg_latency_ms'],
            result['p50_latency_ms'],
            result['p90_latency_ms'],
            result['p95_latency_ms'],
            result['p99_latency_ms'],
            result['p99_5_latency_ms'],
            result['p99_9_latency_ms'],
            result['p99_950_latency_ms'],
            result['p99_990_latency_ms'],
            result['kb_sec'],
        ]
        writer.writerow(row)

    print(f'Created: {filename} ({len(workload_groups[wtype])} records)')


if __name__ == '__main__':
  if len(sys.argv) < 3:
    print(
        'Usage: python3 redis_ha_log_parser.py <output_csv_prefix> <run_uri_1>'
        ' [run_uri_2 ...]'
    )
    print(
        '       python3 redis_ha_log_parser.py <output_csv_prefix> --file'
        ' <uri_list_file.txt>'
    )
    sys.exit(1)

  output_prefix = sys.argv[1]

  run_uris = []
  if sys.argv[2] == '--file':
    if len(sys.argv) < 4:
      print('Error: Missing URI list file.')
      sys.exit(1)
    with open(sys.argv[3], 'r') as f:
      run_uris = [line.strip() for line in f if line.strip()]
  else:
    run_uris = sys.argv[2:]

  parse_runs(run_uris, output_prefix)
