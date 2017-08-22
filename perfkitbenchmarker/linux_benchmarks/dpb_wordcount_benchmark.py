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

"""Runs the word count job on data processing backends.

WordCount example reads text files and counts how often words occur. The input
is text files and the output is text files, each line of which contains a word
and the count of how often it occurs, separated by a tab.
The disk size parameters that are being passed as part of vm_spec are actually
used as arguments to the dpb service creation commands and the concrete
implementations (dataproc, emr, dataflow, etc.) control using the disk size
during the cluster setup.

dpb_wordcount_out_base: The output directory to capture the word count results

For dataflow jobs, please build the dpb_job_jarfile based on
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
"""

import copy
import datetime
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

BENCHMARK_NAME = 'dpb_wordcount_benchmark'

BENCHMARK_CONFIG = """
dpb_wordcount_benchmark:
  description: Run word count on dataflow and dataproc
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

WORD_COUNT_CONFIGURATION = dict(
    [
        (dpb_service.DATAPROC, (gcp_dpb_dataproc.SPARK_SAMPLE_LOCATION,
                                'org.apache.spark.examples.JavaWordCount',
                                BaseDpbService.SPARK_JOB_TYPE)),
        (dpb_service.DATAFLOW, (None,
                                'org.example.WordCount',
                                BaseDpbService.DATAFLOW_JOB_TYPE)),
        (dpb_service.EMR, (aws_dpb_emr.SPARK_SAMPLE_LOCATION,
                           'org.apache.spark.examples.JavaWordCount',
                           BaseDpbService.SPARK_JOB_TYPE))
    ]
)

flags.DEFINE_string('dpb_wordcount_input', None, 'Input for word count')
flags.DEFINE_enum('dpb_wordcount_fs', BaseDpbService.GCS_FS,
                  [BaseDpbService.GCS_FS, BaseDpbService.S3_FS],
                  'File System to use for the job output')
flags.DEFINE_string('dpb_wordcount_out_base', None,
                    'Base directory for word count output')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if (FLAGS.dpb_wordcount_input is None and
          FLAGS.dpb_wordcount_fs != BaseDpbService.GCS_FS):
    raise errors.Config.InvalidValue('Invalid default input directory.')
  # Get handle to the dpb service
  dpb_service_class = dpb_service.GetDpbServiceClass(
      benchmark_config.dpb_service.service_type)
  dpb_service_class.CheckPrerequisites(benchmark_config)



def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):

  # Configuring input location for the word count job
  if FLAGS.dpb_wordcount_input is None:
    input_location = gcp_dpb_dataflow.DATAFLOW_WC_INPUT
  else:
    input_location = '{}://{}'.format(FLAGS.dpb_wordcount_fs,
                                      FLAGS.dpb_wordcount_input)

  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='dpb_wordcount_benchmark',
                                            delete=False)
  stdout_file.close()

  # Switch the parameters for submit job function of specific dpb service
  job_arguments = []
  jarfile, classname, job_type = _GetJobArguments(
      dpb_service_instance.SERVICE_TYPE)
  if FLAGS.dpb_job_classname:
    classname = FLAGS.dpb_job_classname
  if dpb_service_instance.SERVICE_TYPE == dpb_service.DATAFLOW:
    jarfile = FLAGS.dpb_job_jarfile
    job_arguments.append('--stagingLocation={}'.format(
        FLAGS.dpb_dataflow_staging_location))
    job_arguments.append('--runner={}'.format(FLAGS.dpb_dataflow_runner))
    job_arguments.append('--inputFile={}'.format(input_location))
    if not FLAGS.dpb_wordcount_out_base:
      base_out = FLAGS.dpb_dataflow_staging_location
    else:
      base_out = 'gs://{}'.format(FLAGS.dpb_wordcount_out_base)
    job_arguments.append('--output={}/output/'.format(base_out))
  else:
    job_arguments = [input_location]

  # TODO (saksena): Finalize more stats to gather
  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())
  metadata.update({'input_location': input_location})

  start = datetime.datetime.now()
  dpb_service_instance.SubmitJob(jarfile, classname,
                                 job_arguments=job_arguments,
                                 job_stdout_file=stdout_file,
                                 job_type=job_type)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass


def _GetJobArguments(dpb_service_type):
  """Returns the arguments for word count job based on runtime service."""
  if dpb_service_type not in WORD_COUNT_CONFIGURATION:
    raise NotImplementedError
  else:
    return WORD_COUNT_CONFIGURATION[dpb_service_type]
