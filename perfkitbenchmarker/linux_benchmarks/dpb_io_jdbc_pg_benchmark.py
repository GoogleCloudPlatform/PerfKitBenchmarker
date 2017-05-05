# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a read from postgres using the jdbc IO transform on beam data processing backends.

As currently written, this assumes that the postgres instance already exists.

For dataflow jobs, please build the dpb_dataflow_jar based on
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
"""

import copy
import datetime
import os
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService
from perfkitbenchmarker.providers.aws import aws_dpb_emr
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataflow

BENCHMARK_NAME = 'dpb_io_jdbc_pg_benchmark'

# This is the pkb metadata about this benchmark
BENCHMARK_CONFIG = """
dpb_io_jdbc_pg_benchmark:
  description: Run jdbc read from postgres
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

# These are flags that the user can pass in to this benchmark
flags.DEFINE_string('pg_ip', None, 'Postgres Server IP')
flags.DEFINE_string('pg_port', None, 'Postgres Server Port')
flags.DEFINE_string('pg_username', None, 'Postgres Server Username')
flags.DEFINE_string('pg_password', None, 'Postgres Server Password')
flags.DEFINE_string('pg_database_name', None, 'Postgres Server database name')
flags.DEFINE_string('pg_ssl', None, 'Postgres Server SSL supported?')

FLAGS = flags.FLAGS

def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.
  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if (FLAGS.pg_ip is None ):
    raise errors.Config.InvalidValue('Invalid postgres IP address')
  # TODO - check the rest of the params.

def Prepare(benchmark_spec):
  # TODO - here we would create the postgres service instead of taking params.
  pass


def Run(benchmark_spec):

  # Configure default values
#  if FLAGS.dpb_wordcount_input is None:
#    input_location = gcp_dpb_dataflow.DATAFLOW_WC_INPUT
#  else:
#    input_location = '{}://{}'.format(FLAGS.dpb_wordcount_fs,
#                                      FLAGS.dpb_wordcount_input)

  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='dpb_io_jdbc_pg_benchmark',
                                            delete=False)
  stdout_file.close()

  # Switch the parameters for submit job function of specific dpb service
  pipeline_options = []

  pipeline_options.append('--postgresIp={}'.format(FLAGS.pg_ip))
  pipeline_options.append('--postgresPort={}'.format(FLAGS.pg_port))
  pipeline_options.append('--postgresUsername={}'.format(FLAGS.pg_username))
  pipeline_options.append('--postgresPassword={}'.format(FLAGS.pg_password))
  pipeline_options.append('--postgresDatabaseName={}'.format(FLAGS.pg_database_name))
  pipeline_options.append('--postgresSsl={}'.format(FLAGS.pg_ssl))

  # TODO (saksena): Finalize more stats to gather
  results = []
  #metadata = copy.copy(dpb_service_instance.GetMetadata())
  #metadata.update({'useful_info_about_the_run': my_useful_info_here})

  start = datetime.datetime.now()
  dpb_service_instance.RunTest(testname,
                               pipeline_options,
                               job_stdout_file=stdout_file)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass


