"""Command generator for running a script against a BigQuery cluster.

Contains the method to compile the BigQuery specific script execution command
based on generic arguments (sql script, output destination) and BigQuery
specific arguments (flag values).
"""

__author__ = 'p3rf@google.com'

from absl import flags

flags.DEFINE_string('database', None,
                    'The database within which the command or query executes.')
flags.DEFINE_string('query_timeout', '600', 'Query timeout in seconds.')
flags.DEFINE_string(
    'athena_query_output_bucket', None,
    'Specifies where to save the results of the query execution.')
flags.DEFINE_string('athena_region', 'us-east-1', 'Region to use Athena in.')
flags.mark_flags_as_required(['database', 'athena_query_output_bucket'])

FLAGS = flags.FLAGS


def generate_provider_specific_cmd_list(script, driver, output, error):
  """Method to compile the BigQuery specific script execution command.

  Arguments:
    script: SQL script which contains the query.
    driver: Driver that contains the BigQuery specific script executor.
    output: Output log file.
    error: Error log file.

  Returns:
    Command list to execute the supplied script.
  """
  del error
  cmd_list = [
      driver, script, FLAGS.athena_region, FLAGS.database, FLAGS.query_timeout,
      FLAGS.athena_query_output_bucket, output, 'athena.err'
  ]
  return cmd_list
