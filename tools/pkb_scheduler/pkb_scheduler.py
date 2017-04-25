#!/usr/bin/env python3

import os
import signal
import sys
import time
import traceback
import yaml

from google.cloud import storage as gcs

from src.run_config import add_pkb_run_configs, PkbRunConfig
from src.log import log_error, log_info, log_warning

########################################

class Config:
  """Loads the configuration file and stores the settings.
  """

  def __init__(self, config_path):
    self.config_path = config_path
    self.last_reload = None

    self.pkb_executable = None
    self.benchmark_config_path = None
    self.logs_path = None
    self.results_path = None
    self.completion_status_path = None

    self.gcs_client = None
    self.results_gcs_bucket = None

    self.run_benchmarks = None

  def load(self, run_configs):
    """Load the scheduler config from a file.

    If the load fails the old configuration will be kept.

    Args:
      run_configs: the dict storing the current PkbRunConfigs

    Returns:
      True if the config file was successfully loaded. False otherwise.
    """
    self.last_reload = os.path.getmtime(self.config_path)

    config_dict = _load_config_yaml(self.config_path)

    pkb_executable = config_dict["pkb_executable"]
    benchmark_config_path = config_dict["benchmark_config"]
    logs_path = config_dict.get("logs_path") or "logs/"
    results_path = config_dict.get("results_path") or "results/"
    completion_status_path = (
        config_dict.get("completion_status_path") or ".pkb_completion_status")

    if not os.path.exists(pkb_executable):
      log_error("Specified pkb_executable file does not exist")
      return False
    if not os.path.exists(benchmark_config_path):
      log_error("Specified benchmark_config file does not exist.")
      return False
    
    gcs_client = None
    results_gcs_bucket = None
    results_gcs_bucket_name = config_dict.get("results_gcs_bucket")
    if results_gcs_bucket_name:
      gcs_client = gcs.Client()
      results_gcs_bucket = gcs.Bucket(gcs_client, results_gcs_bucket_name)
      if not results_gcs_bucket.exists():
        log_error("Specified results_gcs_bucket %s does not exist." % (
            results_gcs_bucket))
        return False

    run_benchmarks = config_dict["run_benchmarks"]

    # Create the new run configs
    new_run_configs = {}
    for name, run_dict in run_benchmarks.items():
      add_pkb_run_configs(
          new_run_configs, benchmark_config_path, name, run_dict)

    # Carry over the state from the old run configs
    for name, run_config in run_configs.items():
      new_run_config = new_run_configs.get(name)
      if new_run_config is not None:
        new_run_config.last_launch_time = run_config.last_launch_time

    # Load succeeded, store all the new config values
    self.benchmark_config_path = benchmark_config_path
    self.pkb_executable = pkb_executable
    self.logs_path = logs_path
    self.results_path = results_path
    self.completion_status_path = completion_status_path
    self.run_benchmarks = run_benchmarks
    self.gcs_client = gcs_client
    self.results_gcs_bucket = results_gcs_bucket

    # Replace the old run configs with the new ones
    run_configs.clear()
    run_configs.update(new_run_configs)

    return True

  def maybe_reload(self, run_configs):
    """Reloads the config file if it was modified since we last loaded it.

    If the load fails, the old configuration is kept.
    
    Args:
      run_configs: the dict storing the current PkbRunConfigs
    """
    # Maybe reload scheduler config
    modified_time = os.path.getmtime(self.config_path)
    if modified_time != self.last_reload:
      log_info("Reloading scheduler config")
      try:
        if not self.load(run_configs):
          log_error("Failed to reload scheduler config. Sticking with the "
                    "already-loaded configuration.")
      except:
        log_error("Failed to reload scheduler config. Sticking with the "
                  "already-loaded configuration. Traceback:\n%s" % (
                      traceback.format_exc()))

########################################

class ExitHandler:
  """Tells the scheduler to shutdown if our process receives SIGINT or SIGTERM.
  """

  def __init__(self):
    self.should_exit = False
    signal.signal(signal.SIGTERM, self._handle_signal)
    signal.signal(signal.SIGINT, self._handle_signal)

  def _handle_signal(self, sig, frame):
    """The callback for SIGINT and SIGTERM.
    """
    self.should_exit = True

########################################

def _load_config_yaml(yaml_path):
  config_dict = None
  try:
    with open(yaml_path) as config_yaml:
      config_dict = yaml.load(config_yaml.read())
  except Exception as e:
    sys.stderr.write("Failed to open yaml %s: %s\n" % (yaml_path, e))
  assert config_dict is not None
  return config_dict

def main():
  if len(sys.argv) != 2:
    print("USAGE: pkb_scheduler.py <config_yaml>")
    exit()

  # Load the config yaml
  config_path = sys.argv[1]
  config_dict = _load_config_yaml(config_path)

  # Build the Config object
  config = Config(config_path)

  run_configs = {}
  try:
    if not config.load(run_configs):
      log_error("Failed to load scheduler config. Aborting.")
      return 1
  except:
    log_error("Failed to load scheduler config. Aborting. Traceback:\n%s" % (
        traceback.format_exc()))
    return 1

  # Try to read the state from the last run
  try:
    with open(".pkb_scheduler_state") as state_file:
      for line in state_file:
        run_name, last_launch_time = [s.strip() for s in line.split("=", 1)]
        if run_name in run_configs:
          run_configs[run_name].last_launch_time = float(last_launch_time)
  except:
    log_warning("Failed to load .pkb_scheduler_state:\n"
                "%s" % traceback.format_exc())

  exit_handler = ExitHandler()
  runs = [] # Currently running PKB processes
  try:
    while True:
      config.maybe_reload(run_configs)
      # Maybe launch some runs
      for run_config in run_configs.values():
        new_run = run_config.maybe_launch(config)
        if new_run is not None:
          dup_runs = [run for run in runs if run.name == new_run.name]
          if len(dup_runs) != 0:
            log_warning("Launching another %s run even though there are %d "
                        "existing runs" % (new_run.name, len(dup_runs)))
          runs.append(new_run)
      # Maybe publish some results to GCS
      for run in runs:
        run.maybe_publish_results_to_gcs(config)
      # Maybe finish some runs
      runs = [run for run in runs if not run.maybe_finish(config)]
      # Write the state file
      with open(".pkb_scheduler_state", "w") as state_file:
        for run_config in run_configs.values():
          state_file.write("%s=%d\n" % (run_config.name,
                                        run_config.last_launch_time))
      # Sleep for 5 seconds
      time.sleep(5)
      # Maybe stop
      if exit_handler.should_exit:
        raise Exception("Received either SIGINT or SIGTERM.")
  except:
    log_error("Exception occurred, waiting for all runs to finish:\n%s" % \
              traceback.format_exc())
    # Send SIGINT to all of the runs' process groups
    for run in runs:
      try:
        os.killpg(os.getpgid(run.process.pid), signal.SIGINT)
      except:
        pass
    # Wait for all of the runs to finish
    for run in runs:
      if run.process:
        run.process.wait()
  log_info("pkb_scheduler shutdown successfully")
  return 0

########################################

if __name__ == "__main__":
  exit(main())
