import datetime
import os
import subprocess
import time

from .log import log_info
from .run import PkbRun

########################################

class PkbRunConfig:
  """Represents a benchmark flag matrix defined in a PKB benchark config file.

  Each instance of this class is associated with a flag matrix defined in a
  user specified PKB benchmark config file. This class stores how to run the
  associated benchmark (flags to pass to PKB) and how often to run it. It also
  provides a method (maybe_launch) that launches the benchmark if enough time
  has passed since the previous launch.
  """

  def __init__(self, name, flags, seconds_between_launches):
    self.name = name
    self.flags = flags
    self.seconds_between_launches = seconds_between_launches
    self.last_launch_time = 0

  def out_dir(self, base_path):
    """Returns the path to a folder where output files from PKB can be placed.

    Args:
      base_path: The parent folder path.
    """
    return os.path.join(base_path, self.name)

  def maybe_create_out_dir(self, base_path):
    """Create a directory for output files from PKB if necessary.

    Args:
      base_path: The parent folder path.
    """
    out_dir = self.out_dir(base_path)
    if not os.path.exists(out_dir):
      os.makedirs(out_dir)

  def maybe_launch(self, config):
    """Launch a new run if enough time has passed since the previous launch.

    Args:
      config: The scheduler config.

    Returns:
      A PkbRun if a new run was launched. None if not.
    """
    if time.time() - self.last_launch_time < self.seconds_between_launches:
      # It's not time yet
      return None
    # It's time to launch a new process

    self.last_launch_time = time.time()

    # Open the log file
    self.maybe_create_out_dir(config.logs_path)
    timestamp = (datetime.datetime
        .fromtimestamp(time.time()).strftime("%Y-%m-%d_%H-%M"))
    log_path = os.path.join(self.out_dir(config.logs_path),
                            "%s.log" % timestamp)
    log_file = open(log_path, "w")

    log_info("Launching %s\nlog file: %s" % (self.name, log_path))

    # Create file path for the completion status file
    self.maybe_create_out_dir(config.completion_status_path)
    completion_status_path = os.path.join(
        self.out_dir(config.completion_status_path), '%s.json' % timestamp)

    # Create file path for json results file
    self.maybe_create_out_dir(config.results_path)
    results_path = os.path.join(self.out_dir(config.results_path),
                                '%s.json' % timestamp)

    # Build the flags for the pkb command
    flags = [
        "--completion_status_file=%s" % completion_status_path,
        "--json_path=%s" % results_path, "--json_write_mode=ab",
    ]
    flags += ["--%s=%s" % (flag, value) for flag, value in self.flags.items()]

    def _detach_from_process_group():
      # The subprocess calls this function before executing. It detaches from
      # the parent's (this script) process group.
      # We do this so that the launched process doesn't receive signals sent
      # to the pkb_scheduler.py process
      os.setpgrp()

    process = subprocess.Popen([config.pkb_executable] + flags,
                               preexec_fn=_detach_from_process_group,
                               stderr=subprocess.STDOUT,
                               stdout=log_file)
    return PkbRun(self.name, process, log_path, results_path,
                  completion_status_path)

########################################

def add_pkb_run_configs(run_configs, benchmark_config_path,
                        benchmark_name, run_dict):
  """Create a PkbRunConfig for each flag matrix in the specified benchmark.

  Args:
    run_configs: The dict to store PkbRunConfigs in
    benchmark_config_path: The path to the PKB benchmark config file
    benchmark_name: The name of the benchmark in the PKB benchmark config file.
    run_dict: The scheduler options for this benchmark defined in the scheduler
      config file.
  """
  for flag_matrix, options in run_dict.items():
    flags = {
        "benchmark_config_file": benchmark_config_path,
        "flag_matrix": flag_matrix,
        "benchmarks": benchmark_name,
        "run_processes": options.get("run_processes") or 1
    }
    name = "%s_%s" % (benchmark_name, flag_matrix.lower())
    run_config = PkbRunConfig(name, flags, options["seconds_between_launches"])
    run_configs[name] = run_config
