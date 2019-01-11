"""Command generator for running a script against a BigQuery cluster.

Contains the method to compile the BigQuery specific script execution command
based on generic arguments (sql script, output destination) and BigQuery
specific arguments (flag values).
"""

__author__ = 'p3rf@google.com'

from absl import flags

flags.DEFINE_string('bq_project_id', None, 'Project Id which contains the query'
                                           ' dataset and table.')
flags.DEFINE_string('bq_dataset_id', None, 'Dataset Id which contains the query'
                                           ' table.')
flags.DEFINE_string('bq_job_id_prefix', None, 'Job id prefix used for the'
                                              ' request.')
flags.DEFINE_string('bq_job_id_postfix', None, 'Job id postfix used for the'
                                              ' request.')
flags.DEFINE_boolean('collect_output', False, 'Flag indicating if query results'
                                              ' should be collected.')
flags.DEFINE_string('output_table', 'result_table', 'Table for query output.')

flags.mark_flags_as_required(['bq_project_id', 'bq_dataset_id'])

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
  script_name = script.split('.')
  cmd_list = [driver, FLAGS.bq_project_id, FLAGS.bq_dataset_id,
              '%s_%s_%s' % (FLAGS.bq_job_id_prefix, script_name[0],
                            FLAGS.bq_job_id_postfix),
              script, output, error]
  if FLAGS.collect_output:
    cmd_list.extend(['true', FLAGS.output_table])
  return cmd_list
