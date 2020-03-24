"""SQL script execution command generator for Snowflake Virtual Warehouse.

Contains the method to compile the Snowflake specific script execution command
based on generic arguments (sql script) and Snowflake specific arguments (flag
values).
"""

__author__ = 'p3rf@google.com'

from absl import flags

flags.DEFINE_string(
    'connection', None,
    'Named Snowflake connection to use. See https://docs.snowflake.net/manuals/user-guide/snowsql-start.html#using-named-connections for more details.'
)

FLAGS = flags.FLAGS


def generate_provider_specific_cmd_list(script, driver, output, error):
  """Method to compile the Snowflake specific script execution command.

  Arguments:
    script: SQL script which contains the query.
    driver: Driver that contains the BigQuery specific script executor.
    output: Output log file.
    error: Error log file.

  Returns:
    Command list to execute the supplied script.
  """
  del output, error
  cmd_list = [driver, FLAGS.connection, script]
  return cmd_list
