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
import logging
import os

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import temp_dir
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
          machine_type: n1-standard-4
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: pd-standard
          mount_point: /scratch_ts
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

# TODO(odiego): Deprecate WORD_COUNT_CONFIGURATION (not used in practice).
WORD_COUNT_CONFIGURATION = dict([
    (
        dpb_constants.DATAPROC,
        (
            'org.apache.spark.examples.JavaWordCount',
            dpb_constants.SPARK_JOB_TYPE,
        ),
    ),
    (
        dpb_constants.DATAPROC_FLINK,
        ('org.example.WordCount', dpb_constants.FLINK_JOB_TYPE),
    ),
    (
        dpb_constants.DATAPROC_SERVERLESS,
        (
            'org.apache.spark.examples.JavaWordCount',
            dpb_constants.SPARK_JOB_TYPE,
        ),
    ),
    (
        dpb_constants.DATAFLOW,
        ('org.example.WordCount', dpb_constants.DATAFLOW_JOB_TYPE),
    ),
    (
        dpb_constants.EMR,
        (
            'org.apache.spark.examples.JavaWordCount',
            dpb_constants.SPARK_JOB_TYPE,
        ),
    ),
    (
        dpb_constants.KUBERNETES_FLINK_CLUSTER,
        (
            'org.apache.beam.examples.WordCount',
            dpb_constants.FLINK_JOB_TYPE,
        ),
    ),
])

flags.DEFINE_string('dpb_wordcount_input', None, 'Input for word count')
flags.DEFINE_enum(
    'dpb_wordcount_fs',
    dpb_constants.GCS_FS,
    [dpb_constants.GCS_FS, dpb_constants.S3_FS],
    'File System to use for the job output',
)
flags.DEFINE_string(
    'dpb_wordcount_out_base', None, 'Base directory for word count output'
)
flags.DEFINE_list(
    'dpb_wordcount_additional_args',
    [],
    'Additional arguments '
    "which should be passed to job. If the string ':BASE_DIR:' "
    'is contained in these arguments, it will get expanded to '
    'the root of the object storage bucket created for the run.',
)
flags.DEFINE_bool(
    'dpb_wordcount_force_beam_style_job_args',
    False,
    'Force thejob arguments passed in beam supported style.',
)

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if (
      FLAGS.dpb_wordcount_input is None
      and FLAGS.dpb_wordcount_fs != dpb_constants.GCS_FS
  ):
    raise errors.Config.InvalidValue('Invalid default input directory.')


def Prepare(benchmark_spec):
  """Make the jarfile available if using Dataflow."""
  dpb_service_instance = benchmark_spec.dpb_service
  storage_service = dpb_service_instance.storage_service
  benchmark_spec.dpb_wordcount_jarfile = FLAGS.dpb_job_jarfile
  if dpb_service_instance.SERVICE_TYPE == dpb_constants.DATAFLOW:
    if FLAGS.dpb_job_jarfile and FLAGS.dpb_job_jarfile.startswith('gs://'):
      local_path = os.path.join(
          temp_dir.GetRunDirPath(), os.path.basename(FLAGS.dpb_job_jarfile)
      )
      storage_service.Copy(FLAGS.dpb_job_jarfile, local_path)
      benchmark_spec.dpb_wordcount_jarfile = local_path


def Run(benchmark_spec):
  # Configuring input location and output for the word count job
  if FLAGS.dpb_wordcount_input is None:
    input_location = gcp_dpb_dataflow.DATAFLOW_WC_INPUT
  else:
    input_location = '{}://{}'.format(
        FLAGS.dpb_wordcount_fs, FLAGS.dpb_wordcount_input
    )

  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Switch the parameters for submit job function of specific dpb service
  job_arguments = []
  classname, job_type = _GetJobArguments(dpb_service_instance.SERVICE_TYPE)
  if FLAGS.dpb_job_classname:
    classname = FLAGS.dpb_job_classname
  if FLAGS.dpb_job_type:
    job_type = FLAGS.dpb_job_type
  if (
      dpb_service_instance.SERVICE_TYPE
      in [
          dpb_constants.DATAFLOW,
          dpb_constants.DATAPROC_FLINK,
          dpb_constants.KUBERNETES_FLINK_CLUSTER,
      ]
      or FLAGS.dpb_wordcount_force_beam_style_job_args
  ):
    jarfile = benchmark_spec.dpb_wordcount_jarfile
    job_arguments.append('--inputFile={}'.format(input_location))
    # Add the output argument for Dataflow
    if dpb_service_instance.SERVICE_TYPE == dpb_constants.DATAFLOW:
      if not FLAGS.dpb_wordcount_out_base:
        base_out = dpb_service_instance.GetStagingLocation()
      else:
        base_out = f'gs://{FLAGS.dpb_wordcount_out_base}'
      job_arguments.append(f'--output={os.path.join(base_out, "output")}')
  else:
    # Use user-provided jar file if present; otherwise use the default example
    if not benchmark_spec.dpb_wordcount_jarfile:
      jarfile = dpb_service_instance.GetExecutionJar('spark', 'examples')
    else:
      jarfile = benchmark_spec.dpb_wordcount_jarfile

    job_arguments = [input_location]
  job_arguments.extend(_GetJobAdditionalArguments(dpb_service_instance))

  # TODO (saksena): Finalize more stats to gather
  results = []

  start_time = datetime.datetime.now()
  job_result = dpb_service_instance.SubmitJob(
      jarfile=jarfile,
      classname=classname,
      job_arguments=job_arguments,
      job_type=job_type,
  )
  end_time = datetime.datetime.now()

  # Update metadata after job run to get job id
  metadata = copy.copy(dpb_service_instance.GetResourceMetadata())
  metadata.update({
      'input_location': input_location,
      'dpb_wordcount_additional_args': ','.join(
          FLAGS.dpb_wordcount_additional_args
      ),
      'dpb_wordcount_force_beam_style_job_args': (
          FLAGS.dpb_wordcount_force_beam_style_job_args
      ),
      'dpb_job_type': job_type,
  })
  dpb_service_instance.metadata.update(metadata)

  run_time = (end_time - start_time).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  if job_result is not None:
    results.append(
        sample.Sample(
            'reported_run_time', job_result.run_time, 'seconds', metadata
        )
    )
    if job_result.pending_time is not None:
      results.append(
          sample.Sample(
              'reported_pending_time',
              job_result.pending_time,
              'seconds',
              metadata,
          )
      )

  # TODO(odiego): Refactor to avoid explicit service type checks.
  if FLAGS.dpb_export_job_stats:
    if dpb_service_instance.SERVICE_TYPE == dpb_constants.DATAFLOW:
      avg_cpu_util = dpb_service_instance.GetAvgCpuUtilization(
          start_time, end_time
      )
      results.append(sample.Sample('avg_cpu_util', avg_cpu_util, '%', metadata))
      stats = dpb_service_instance.job_stats
      for name, value in stats.items():
        results.append(sample.Sample(name, value, 'number', metadata))

    costs = dpb_service_instance.CalculateLastJobCosts()
    results += costs.GetSamples(metadata=metadata)
  else:
    logging.info(
        '--dpb_export_job_stats flag is False (which is the new default). Not '
        'exporting job stats.'
    )

  return results


def Cleanup(benchmark_spec):
  pass


def _GetJobArguments(dpb_service_type):
  """Returns the arguments for word count job based on runtime service."""
  if dpb_service_type not in WORD_COUNT_CONFIGURATION:
    raise NotImplementedError
  else:
    return WORD_COUNT_CONFIGURATION[dpb_service_type]


def _GetJobAdditionalArguments(dpb_service_instance):
  return [
      arg.replace(':BASE_DIR:', dpb_service_instance.base_dir)
      for arg in FLAGS.dpb_wordcount_additional_args
  ]
