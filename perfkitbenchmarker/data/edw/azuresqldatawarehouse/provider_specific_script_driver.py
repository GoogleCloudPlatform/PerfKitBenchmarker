# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Command generator for running a script against a Azure SQL cluster.

Contains the method to compile the Azure SQL data warehouse specific script
execution command based on generic arguments (sql script, output destination)
and Azure SQL specific arguments (flag values).
"""

__author__ = 'p3rf@google.com'

from absl import flags

flags.DEFINE_string('server', None, 'SQL server.')
flags.DEFINE_string('database', None, 'SQL Database.')
flags.DEFINE_string('user', None, 'SQL User.')
flags.DEFINE_string('password', None, 'SQL Password.')

flags.mark_flags_as_required(['server', 'database', 'user', 'password'])

FLAGS = flags.FLAGS


def generate_provider_specific_cmd_list(script, driver, output, error):
  """Method to compile the Azure SQL specific script execution command.

  Arguments:
    script: SQL script which contains the query.
    driver: Driver that contains the Azure SQL data warehouse specific script
      executor.
    output: Output log file.
    error: Error log file.

  Returns:
    Command list to execute the supplied script.
  """
  return [driver, FLAGS.server, FLAGS.database, FLAGS.user, FLAGS.password,
          script, output, error]
