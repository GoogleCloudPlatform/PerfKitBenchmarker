"""Gets CPU, Memory, and IO pressure statistics."""

import copy
import datetime
import json
import signal
import sys
import time

SOME = 'some'
FULL = 'full'
CPU = 'cpu'
MEMORY = 'memory'
IO = 'io'

# Global variable to store network statistics
all_stats = {}


def get_line_stats(line):
  stats = {}
  for token in line[1:]:
    stat, value = token.split('=')
    if stat == 'total':
      stats[stat] = int(value)
    else:
      stats[stat] = float(value)
  return stats


def get_pressure_stall_stats(filepath):
  """Gets CPU pressure stall statistics."""
  with open(filepath) as f:
    lines = f.readlines()
  some = get_line_stats(lines[0].strip().split())
  full = get_line_stats(lines[1].strip().split())
  return {SOME: some, FULL: full}


def get_all_pressure_stall_stats():
  return {
      CPU: get_pressure_stall_stats('/proc/pressure/cpu'),
      MEMORY: get_pressure_stall_stats('/proc/pressure/memory'),
      IO: get_pressure_stall_stats('/proc/pressure/io'),
  }


def data_diff(prev, current):
  diffed = copy.deepcopy(current)

  def diff(resource, cover):
    cur_total = int(current[resource][cover]['total'])
    prev_total = int(prev[resource][cover]['total'])
    return cur_total - prev_total

  for res in [CPU, MEMORY, IO]:
    for cov in [SOME, FULL]:
      diffed[res][cov]['total'] = diff(res, cov)
  return diffed


def signal_handler(sig, frame):
  """Handles SIGINT (Ctrl+C)."""
  del sig, frame
  print(json.dumps(all_stats, indent=4))
  sys.exit(0)


def main():
  # Register the signal handler
  signal.signal(signal.SIGINT, signal_handler)

  # Get initial network statistics
  prev_data = get_all_pressure_stall_stats()
  all_stats['stats'] = []

  while True:
    time.sleep(10)
    current_data = get_all_pressure_stall_stats()

    diffed_data = data_diff(prev_data, current_data)
    all_stats['stats'].append({
        'timestamp': datetime.datetime.now().strftime('%H:%M:%S'),
        'statistics': diffed_data,
    })
    prev_data = current_data


if __name__ == '__main__':
  main()
