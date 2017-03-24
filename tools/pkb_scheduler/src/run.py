import datetime
import fcntl
import json
import os
import time
import uuid

from .log import log_error, log_info, log_warning

########################################

class PkbResultsWatcher:
  """Manages loading new results from a results json file from PKB.

  This is needed so we can publish results from long-running benchmarks as soon
  as they are available.
  """

  def __init__(self, path):
    self.path = path
    self.last_refresh = None
    self.next_seek = 0

  def maybe_load_results(self, process_results):
    """Process any new results using a caller-specified function.

    If our file was modified since we last read it, seek to where we last left
    off and pass the file and the number of bytes to be processed to the
    caller-specified function.

    Args:
      process_results: A function with following parameters:
        (file object, number of bytes to process)

    Returns:
      True if there were new results to process. False otherwise.
    """
    try:
      modified_time = os.path.getmtime(self.path)
    except:
      return False
    if modified_time and modified_time != self.last_refresh:
      # Need to pull from this data source again
      with open(self.path) as samples_json:
        fcntl.flock(samples_json, fcntl.LOCK_EX)
        # Get the number of new bytes
        samples_json.seek(0, 2)
        results_size = samples_json.tell() - self.next_seek
        # Seek to most recent data and process it
        if results_size > 0:
          samples_json.seek(self.next_seek)
          process_results(samples_json, results_size)
          self.next_seek += results_size
      self.last_refresh = modified_time
      return True
    return False

########################################

class PkbRun:
  """Manages a single run of a PKB benchmark.

  - Publishes results to GCS objects grouped by benchmark and day.
  - Reports when the run has finished.
  - Counts the number of succeeded, failed, and skipped sub-runs.
  """

  def __init__(self, name, process, log_path,
               results_path, completion_status_path):
    self.name = name
    self.process = process
    self.log_path = log_path
    self.completion_status_path = completion_status_path
    self.results_watcher = PkbResultsWatcher(results_path)

  def maybe_finish(self, config):
    """Process the results of a benchmark if its process has finished.
    
    If the run completed:
    - Load the completion status file and count the number of succeeded, failed,
      and skipped sub-runs.
    - Log whether the run succeeded, partially succeeded, or failed.
    - Publish any unpublished results to a GCS object.

    Args:
      config: the scheduler config

    Returns:
      True if the benchmark run completed. False otherwise.
    """
    return_code = self.process.poll()
    if return_code is None:
      # Process is still running
      return False
    # The current run has finished

    # Aggregate completion statuses
    success_count = 0
    fail_count = 0
    skip_count = 0
    try:
      with open(self.completion_status_path) as status_file:
        status_dicts = [json.loads(line) for line in status_file]

        success_count = (
            sum(1 for s in status_dicts if s['status'] == "SUCCEEDED"))
        fail_count = sum(1 for s in status_dicts if s['status'] == "FAILED")
        skip_count = sum(1 for s in status_dicts if s['status'] == "SKIPPED")
    except:
      pass

    if return_code == 0:
      # Run finished successfully
      log_info("%s finished successfully\n"
               "log file: %s" % (self.name, self.log_path))
      self.maybe_publish_results_to_gcs(config)
    else:
      if success_count > 0:
        # Some benchmarks succeeded
        log_warning("%s partially succeeded:"
                    "%d succeeded, %d failed, %d skipped\n"
                    "log file: %s" % (self.name,
                                      success_count, fail_count, skip_count,
                                      self.log_path))
        self.maybe_publish_results_to_gcs(config)
      else:
        # Run failed
        log_error("%s failed. Returned %d\n"
                  "log file: %s" % (self.name, return_code,
                                    self.log_path))
    return True

  def maybe_publish_results_to_gcs(self, config):
    """Publish any new results to a file in the user-specified GCS bucket.

    The files are named: <benchmark name>.<year>.<month>.<day>/<some random id>

    Args:
      config: the scheduler config
    """
    if config.results_gcs_bucket is None:
      return

    timestamp = datetime.datetime.fromtimestamp(time.time())
    results_blob_name = ("{benchmark}.{year}.{month}.{day}/{name}").format(
        benchmark=self.name.rsplit("_", 1)[0],
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
        name=str(uuid.uuid4()))

    results_blob = config.results_gcs_bucket.blob(results_blob_name)
    upload = lambda f, size: results_blob.upload_from_file(f, size=size)

    if self.results_watcher.maybe_load_results(upload):
      log_info("uploaded some results from %s to gs://%s/%s\n" % (
          self.name, config.results_gcs_bucket.name, results_blob_name))
