"""Driver for running a script against a EDW cluster.

Driver compiles the provider specific script execution command and returns the
time taken to execute the script in seconds or -1 if the script fails.
"""

__author__ = 'p3rf@google.com'

import json
import logging
from subprocess import call
import time
from absl import app
from absl import flags
import provider_specific_script_driver

flags.DEFINE_string('script', None, 'SQL script which contains the query.')
flags.DEFINE_string('logfile_suffix', 'log', 'Suffix to use for the output and '
                                             'error files.')
flags.DEFINE_multi_string('failing_scripts', [],
                          'List of failing scripts whose execution should be '
                          'skipped.')


FLAGS = flags.FLAGS
DRIVER_NAME = './script_runner.sh'
JOB_ID_KEY = 'INFO:googleapiclient.model:jobId:'
API_LOG_FILE = 'apilog.out'


def default_logfile_names(script, suffix):
  """Method to return the names for output and error log files."""
  suffix = script.split('.')[0] if suffix is None else suffix
  output_logfile = '{}_out.txt'.format(suffix)
  error_logfile = '{}_err.txt'.format(suffix)
  return output_logfile, error_logfile


def execute_script(script, logfile_suffix):
  """Method to execute a sql script on a EDW cluster.

  Arguments:
    script: SQL script which contains the query.
    logfile_suffix: Suffix to use for the output and error files.

  Returns:
    Dictionary containing the name of the script and its execution time (-1 if
    the script fails)
  """
  response_status = 1  # assume failure by default
  job_id = 'undefined_job'
  if script not in FLAGS.failing_scripts:
    output, error = default_logfile_names(script, logfile_suffix)
    cmd = provider_specific_script_driver.generate_provider_specific_cmd_list(
        script, DRIVER_NAME, output, error)
    start_time = time.time()
    response_status = call(cmd)
  execution_time = -1 if (response_status != 0) else round((time.time() -
                                                            start_time), 2)
  try:
    with open(API_LOG_FILE) as fp:
      line = fp.readline()
      while line:
        line_tokens = line.strip().split()
        if len(line_tokens) > 1 and line_tokens[0] == JOB_ID_KEY:
          job_id = line.strip().split()[1]
          break
        line = fp.readline()
  except IOError:
    pass

  script_execution_details = {'execution_time': execution_time,
                              'job_id': job_id}
  results = {script: script_execution_details}
  return json.dumps(results)


def main(argv):
  del argv
  print (execute_script(FLAGS.script, FLAGS.logfile_suffix))  # pylint: disable=superfluous-parens


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  app.run(main)
