"""Driver for running a sequence of scripts against a Big Query cluster.

The sequence of scripts is identified by a profile, which is defined in the
tpc_profile_details module.
"""

__author__ = 'p3rf@google.com'

import json
import logging
import time
from absl import app
from absl import flags
import script_driver
import tpc_profile_details

flags.DEFINE_string('profile', None, 'Profile to identify the sequence of '
                                     'scripts.')

FLAGS = flags.FLAGS


def execute_profile(profile):
  """Method to execute a profile (list of sql scripts) on an edw cluster.

  Execute a profile (list of sql scripts, identified by names) on a cluster and
  report a dictionary with the execution time.

  Arguments:
    profile: Profile to identify the sequence of scripts.

  Returns:
    A dictionary containing
      1. Individual script metrics: script name and its execution time (-1 if
         the script fails).
      2. Aggregated execution time: profile name and cumulative execution time.
  """
  execution_times = {}
  start_time = time.time()

  for script_index in tpc_profile_details.profile_dictionary[profile]:
    logfile_suffix = '{}_{}'.format(profile, str(script_index))
    script = '{}.sql'.format(str(script_index))
    script_performance = script_driver.execute_script(script, logfile_suffix)
    execution_times.update(json.loads(script_performance))
  execution_times['wall_time'] = round((time.time() - start_time), 2)
  return json.dumps(execution_times)


def main(argv):
  del argv
  print execute_profile(FLAGS.profile)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  app.run(main)
