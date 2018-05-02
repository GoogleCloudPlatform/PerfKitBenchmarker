"""Command generator for running a script against a Redshift cluster.

Contains the method to compile the Redshift specific script execution command
based on generic arguments (sql script, output destination) and Redshift
specific arguments (flag values).
"""

__author__ = 'p3rf@google.com'

from absl import flags

flags.DEFINE_string('host', None, 'Redshift host.')
flags.DEFINE_string('database', None, 'Redshift Database.')
flags.DEFINE_string('user', None, 'Redshift User.')
flags.DEFINE_string('password', None, 'Redshift Password.')
flags.DEFINE_string('script', None, 'SQL script which contains the query.')
flags.DEFINE_string('logfile_suffix', 'log', 'Suffix to use for the output and '
                                             'error files.')

flags.mark_flags_as_required(['host', 'database', 'user', 'password'])

FLAGS = flags.FLAGS


def generate_provider_specific_cmd_list(script, driver, output, error):
  """Method to compile the Redshift specific script execution command.

  Arguments:
    script: SQL script which contains the query.
    driver: Driver that contains the Redshift specific script executor.
    output: Output log file.
    error: Error log file.

  Returns:
    Command list to execute the supplied script.
  """
  return [driver, FLAGS.host, FLAGS.database, FLAGS.user, FLAGS.password,
          script, output, error]
