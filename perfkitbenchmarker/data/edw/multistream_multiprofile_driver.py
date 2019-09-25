"""Driver for running multiple profiles concurrently against a cluster.

The list of profiles are passed via flag each of which are defined in the
profile_details module.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__author__ = 'p3rf@google.com'

import json
import logging
from multiprocessing import Process
from multiprocessing import Queue
import time
from absl import app
from absl import flags
import unistream_profile_driver


flags.DEFINE_list('profile_list', None, 'List of profiles. Each will be run on '
                                        'its own process to simulate '
                                        'concurrency.')

flags.mark_flags_as_required(['profile_list'])

FLAGS = flags.FLAGS


def process_profile(profile, response_q):
  """Method to execute a profile (list of sql scripts) on a cluster.

  Args:
    profile: The profile to run.
    response_q: Communication channel between processes.
  """
  profile_output = unistream_profile_driver.execute_profile(profile)
  response_q.put([profile, profile_output])


def manage_streams():
  """Method to launch concurrent execution of multiple profiles.

  Returns:
    A dictionary containing
    1. wall_time: Total time taken for all the profiles to complete execution.
    2. profile details:
      2.1. profile_execution_time: Time taken for all scripts in the profile to
      complete execution.
      2.2. Individual script metrics: script name and its execution time (-1 if
      the script fails)
  """
  profile_handling_process_list = []
  profile_performance = Queue()

  start_time = time.time()

  for profile in FLAGS.profile_list:
    profile_handling_process = Process(target=process_profile,
                                       args=(profile, profile_performance,))
    profile_handling_process.start()
    profile_handling_process_list.append(profile_handling_process)

  for profile_handling_process in profile_handling_process_list:
    profile_handling_process.join()

  # All processes have joined, implying all profiles have been completed
  execution_time = round((time.time() - start_time), 2)

  num_profiles = len(FLAGS.profile_list)

  overall_performance = {}

  while num_profiles:
    temp_performance_response = profile_performance.get()
    profile = temp_performance_response[0]
    overall_performance[profile] = json.loads(temp_performance_response[1])
    num_profiles -= 1

  overall_performance['wall_time'] = execution_time
  return json.dumps(overall_performance)


def main(argv):
  del argv
  print(manage_streams())


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  app.run(main)
