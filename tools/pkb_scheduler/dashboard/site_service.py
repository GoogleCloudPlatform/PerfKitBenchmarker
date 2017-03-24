#!/usr/bin/env python3
import itertools
import json
import multiprocessing as mp
import os
import pickle
import sys
import time
import traceback
import yaml

from collections import deque
from socketserver import BaseRequestHandler, ThreadingMixIn, TCPServer

from service_util import ServiceConnection

########################################

class StatusArray:
  """ A recursive structure describing the completion statuses of every
  config in a PKB flag matrix.
  """

  def __init__(self, flag_name, subarrays, statuses=None):
    """ Create a StatusArray.

    Args:
      flag_name: the name of this node's flag axis
      subarrays: { <flag value> : StatusArray }
      statuses: { <flag value> : <completion status>
    """
    self.flag_name = flag_name
    self.statuses = statuses or dict(StatusArray._aggregate_statuses(subarrays))
    self.subarrays = subarrays

  def _aggregate_statuses(subarrays):
    for flag_value, subarray in subarrays.items():
      statuses = set()
      for status in subarray.statuses.values():
        assert status is not None
        statuses.add(status)
      if len(statuses) > 1:
        yield (flag_value, "mixed")
      else:
        assert len(statuses) == 1
        yield (flag_value, list(statuses)[0])

  def as_dict(self):
    return {
      "flag_name": self.flag_name,
      "statuses": self.statuses,
      "subarrays": {flag_value: subarray.as_dict()
                     for flag_value, subarray in self.subarrays.items()},
    }

def build_status_array(run_configs, flag_matrix, flag_axes, flag_values=None):
  flag_name = flag_axes[0]
  flag_values = flag_values or {}

  statuses = None
  subarrays = {}
  if len(flag_axes) == 1:
    statuses = {config["flags"][flag_name]: config["status"]
                for config in run_configs}
  else:
    next_flag_name = flag_axes[1]
    for flag_value in flag_matrix[flag_name]:
      next_flag_values = flag_values.copy()
      next_flag_values[flag_name] = flag_value

      # Filter out irrelevant run configs
      next_run_configs = [c for c in run_configs
                          if c["flags"].items() >= next_flag_values.items()]
      if not next_run_configs:
        continue

      subarrays[flag_value] = build_status_array(next_run_configs, flag_matrix,
                                                 flag_axes[1:],
                                                 next_flag_values)
  return StatusArray(flag_name, subarrays, statuses)

def load_completion_statuses(flag_matrix, flag_axes, completion_status_path):
  status_array = None
  print("Building status matrix %s:" % completion_status_path)
  try:
    with open(completion_status_path) as status_file:
      run_configs = [json.loads(line) for line in status_file if line]
      status_array = build_status_array(run_configs, flag_matrix, flag_axes)
  except:
    print("Failed to build status matrix %s:" % completion_status_path)
    traceback.print_exc()
  return status_array

########################################

def _load_yaml(yaml_path):
  yaml_dict = None
  with open(yaml_path) as yaml_file:
    # Try to parse the chart page spec yaml
    try:
      yaml_dict = yaml.load(yaml_file.read())
    except Exception as e:
      sys.stderr.write("ERROR: invalid yaml: %s: %s\n" % (self.spec_path, e))
  if yaml_dict is None:
    sys.stderr.write("ERROR: Failed to open: %s\n" % self.spec_path)
  return yaml_dict

class Dashboard:
  def __init__(self, spec_path):
    self.spec_path = spec_path
    self.last_spec_update = None

    self.completion_status_dir = None

    # {<category name> : [<benchmark name>]}
    self.categories = {}
    # {<benchmark name> : <category name>}
    self.benchmark_categories = {}
    # {<benchmark name> : {<timestamp> : StatusArray}}
    self.status_arrays = {}
    # {<benchmark name> : <flag matrix>}
    self.flag_matrices = {}
    # {<benchmark name> : [<flag axis>]}
    self.diff_axes = {}

  def maybe_refresh_spec(self):
    spec_dir = os.path.dirname(self.spec_path)

    modified_time = os.path.getmtime(self.spec_path)
    if modified_time != self.last_spec_update:
      print("Refreshing dashboard spec: %s" % self.spec_path)
      self.last_spec_update = modified_time

      spec = _load_yaml(self.spec_path)
      if spec is None:
        return False

      # Spec loaded properly - now let's read it
      scheduler_config = _load_yaml(spec["scheduler_config"])
      benchmark_config = _load_yaml(scheduler_config["benchmark_config"])

      # Get PKB completion status path
      self.completion_status_dir = scheduler_config["completion_status_path"]

      def get_benchmark_name(benchmark, flag_matrix):
        return "%s_%s" % (benchmark, flag_matrix.lower())

      # Load the flag matrix definitions
      for benchmark_name in scheduler_config["run_benchmarks"]:
        flag_matrix_defs = benchmark_config[benchmark_name]["flag_matrix_defs"]
        for flag_matrix_name, flag_matrix in flag_matrix_defs.items():
          full_benchmark_name = (
              get_benchmark_name(benchmark_name, flag_matrix_name))
          self.flag_matrices[full_benchmark_name] = flag_matrix
          print(full_benchmark_name)
      # Load the categories, benchmark_categories, and differentiating axes
      for category_name, benchmarks in spec["categories"].items():
        self.categories[category_name] = []
        for benchmark in benchmarks:
          for flag_matrix_name, flag_axes in benchmark["flag_axes"].items():
            benchmark_name = get_benchmark_name(benchmark["benchmark"],
                                                flag_matrix_name)
            self.categories[category_name].append(benchmark_name)
            self.benchmark_categories[benchmark_name] = category_name
            self.diff_axes[benchmark_name] = flag_axes
      return True

    return False

  def update_site_context(self, site_context):
    site_context["categories"] = self.categories
    site_context["benchmark_categories"] = self.benchmark_categories
    # {benchmark_name: [timestamp_string]}
    site_context["runs"] = {
        benchmark: list(reversed(sorted(timestamps.keys())))
        for benchmark, timestamps in self.status_arrays.items()
    }

  def load_completion_statuses(self):
    for benchmark_name in os.listdir(self.completion_status_dir):
      if benchmark_name not in self.diff_axes:
        continue
      if benchmark_name not in self.status_arrays:
        self.status_arrays[benchmark_name] = {}
      benchmark_path = os.path.join(self.completion_status_dir, benchmark_name)
      for timestamp_file in os.listdir(benchmark_path):
        # Get the run's timestamp
        timestamp, _ = timestamp_file.rsplit(".", 1)
        completion_status_path = os.path.join(benchmark_path, timestamp_file)

        if timestamp not in self.status_arrays[benchmark_name]:
          # Build the run's completion status array
          flag_matrix = self.flag_matrices[benchmark_name]
          diff_axes = self.diff_axes[benchmark_name]
          status_array = load_completion_statuses(flag_matrix, diff_axes,
                                                  completion_status_path)
          if status_array is None:
            continue
          self.status_arrays[benchmark_name][timestamp] = \
              status_array
          yield (benchmark_name, timestamp, status_array)

########################################

class DashboardServerHandler(BaseRequestHandler):
  def handle(self):
    status_array_store = self.server.status_array_store
    site_context = self.server.site_context
    request = ServiceConnection(self.request)

    split_cmd = request.recv_str().split(" ", 1)
    if len(split_cmd) == 1:
      split_cmd.append("")
    cmd, args = split_cmd
    if cmd == "run":
      benchmark, timestamp = args.split(",")
      print("Sending run context %s/%s to %s" % (
          benchmark, timestamp, self.request.getsockname()))

      category = site_context["benchmark_categories"][benchmark]
          
      page_context = {
        "category": category,
        "benchmark_name": benchmark,
        "timestamp": timestamp,
        "categories": site_context["categories"],
        "status_array": status_array_store[(benchmark, timestamp)],
      }
      request.send_pickle(page_context)
    elif cmd == "benchmark":
      benchmark = args
      print("Sending benchmark context %s to %s" % (
          benchmark, self.request.getsockname()))
      category = site_context["benchmark_categories"][benchmark]
      page_context = {
        "category": category,
        "benchmark_name": benchmark,
        "categories": site_context["categories"],
        "runs": site_context["runs"][benchmark],
      }
      request.send_pickle(page_context)
    elif cmd == "get_categories":
      print("Sending categories to %s" % str(self.request.getsockname()))
      request.send_pickle(site_context["categories"])

class ThreadedTCPServer(ThreadingMixIn, TCPServer):
  pass

########################################

def run_site_service(host, port, spec_path):
  # Create the shared page store
  mp_manager = mp.Manager()
  status_array_store = mp_manager.dict()
  site_context = mp_manager.dict()

  def _dashboard_refresh():
    dashboard = Dashboard(spec_path)
    while True:
      dashboard.maybe_refresh_spec()
      dashboard.load_completion_statuses()
      for benchmark, timestamp, status_array in \
          dashboard.load_completion_statuses():
        print("Loaded new status array: %s/%s" % (benchmark, timestamp))
        # Build a dict version of the status array and store it
        status_array_store[(benchmark, timestamp)] = \
            status_array.as_dict()
      dashboard.update_site_context(site_context)
      # Only try to refresh dashboard spec every second
      time.sleep(1)

  # Start the dashboard refresher
  dashboard_refresh_proc = mp.Process(target=_dashboard_refresh)
  dashboard_refresh_proc.daemon = True
  dashboard_refresh_proc.start()

  # Start the page server
  page_server = ThreadedTCPServer((host, port), DashboardServerHandler)
  page_server.status_array_store = status_array_store
  page_server.site_context = site_context
  page_server.serve_forever()
        
def main():
  if len(sys.argv) != 2:
    print("Usage: python3 site_service.py <dashboard_spec_yaml>")
    exit()
  spec_path = sys.argv[1]
  run_site_service("localhost", 32422, spec_path)

########################################

if __name__ == "__main__":
  main()
