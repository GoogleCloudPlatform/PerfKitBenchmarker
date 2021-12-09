# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Executes the 3 stages of Teasort stages on a Apache Hadoop MapReduce cluster.

TeraSort is a popular benchmark that measures the amount of time to sort a
configured amount of randomly distributed data on a given cluster. It is
commonly used to measure MapReduce performance of an Apache Hadoop cluster.
The following report compares performance of a YARN-scheduled TeraSort job on

A full TeraSort benchmark run consists of the following three steps:

* Generating the input data via TeraGen.
* Running the actual TeraSort on the input data.
* Validating the sorted output data via TeraValidate.

The benchmark reports the detailed latency of executing each stage.
"""

import logging
from typing import List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'dpb_terasort_benchmark'

BENCHMARK_CONFIG = """
dpb_terasort_benchmark:
  description: Run terasort on dataproc and emr
  dpb_service:
    service_type: unmanaged_dpb_svc_yarn_cluster
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
        AWS:
          machine_type: m5.xlarge
        Azure:
          machine_type: Standard_F4s_v2
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: pd-standard
          mount_point: /scratch_ts
        AWS:
          disk_size: 500
          disk_type: st1
          mount_point: /scratch_ts
        Azure:
          disk_size: 500
          disk_type: Standard_LRS
          mount_point: /scratch_ts
    worker_count: 2
"""

_FS_TYPE_EPHEMERAL = 'ephemeral'
_FS_TYPE_PERSISTENT = 'persistent'

flags.DEFINE_enum('dpb_terasort_storage_type', _FS_TYPE_PERSISTENT,
                  [_FS_TYPE_EPHEMERAL, _FS_TYPE_PERSISTENT],
                  'The type of storage for executing the Terasort benchmark')
flags.DEFINE_integer('dpb_terasort_num_records', 10000,
                     'Number of 100-byte rows to generate.')
flags.DEFINE_bool(
    'dpb_terasort_pre_cleanup', False,
    'Cleanup the terasort directories on the specified filesystem.')
flags.DEFINE_integer(
    'dpb_terasort_block_size_mb', None,
    'Virtual blocksize to use on the persistent file system. This controls '
    'the parallelism of the map stages of terasort and teravalidated. Defaults '
    'to cluster defined defaults. Does not support HDFS.')

FLAGS = flags.FLAGS

SUPPORTED_DPB_BACKENDS = [
    dpb_service.DATAPROC, dpb_service.EMR,
    dpb_service.UNMANAGED_DPB_SVC_YARN_CLUSTER
]
TERASORT_JAR = 'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
UNMANAGED_TERASORT_JAR = '/opt/pkb/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Args:
    benchmark_config: Config needed to run the terasort benchmark

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: On encountering invalid
    configuration.
  """
  dpb_service_type = benchmark_config.dpb_service.service_type
  if dpb_service_type not in SUPPORTED_DPB_BACKENDS:
    raise errors.Config.InvalidValue(
        'Invalid backend {} for terasort. Not in:{}'.format(
            dpb_service_type, str(SUPPORTED_DPB_BACKENDS)))

  if (FLAGS.dpb_terasort_block_size_mb and
      FLAGS.dpb_terasort_storage_type != _FS_TYPE_PERSISTENT):
    raise errors.Config.InvalidValue('You cannot set HDFS block size.')


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  del spec  # unused


def Run(spec: benchmark_spec.BenchmarkSpec):
  """Runs the 3 stages of the terasort benchmark.

  The following stages are executed based on the selected Job Type:
    * Generating the input data via TeraGen.
    * Running the actual TeraSort on the input data.
    * Validating the sorted output data via TeraValidate.

  The samples report the cumulative results along with the results for the
  individual stages.

  Args:
    spec: Spec needed to run the terasort benchmark

  Returns:
    A list of samples, comprised of the detailed run times of individual stages.
    The samples have associated metadata detailing the cluster details and used
    filesystem.

  Raises:
    JobSubmissionError if any job fails.
  """
  service = spec.dpb_service

  if FLAGS.dpb_terasort_storage_type == _FS_TYPE_PERSISTENT:
    base_dir = service.base_dir + '/'
  else:
    base_dir = '/'

  metadata = {}
  metadata.update(spec.dpb_service.GetMetadata())
  logging.info('metadata %s ', str(metadata))

  results = []
  # May not exist for preprovisioned cluster.
  if service.resource_ready_time and service.create_start_time:

    logging.info('Resource create_start_time %s ',
                 str(service.create_start_time))
    logging.info('Resource resource_ready_time %s ',
                 str(service.resource_ready_time))
    create_time = service.GetClusterCreateTime()
    logging.info('Resource create_time %s ', str(create_time))

    if create_time is not None:
      results.append(
          sample.Sample('dpb_cluster_create_time', create_time, 'seconds',
                        metadata.copy()))

  metadata.update({'base_dir': base_dir})
  metadata.update(
      {'dpb_terasort_storage_type': FLAGS.dpb_terasort_storage_type})
  metadata.update({'terasort_num_record': FLAGS.dpb_terasort_num_records})
  storage_in_gb = (FLAGS.dpb_terasort_num_records * 100) // (1000 * 1000 * 1000)
  metadata.update({'terasort_dataset_size_in_GB': storage_in_gb})
  if FLAGS.dpb_terasort_block_size_mb:
    # TODO(pclay): calculate default blocksize using configuration class?
    metadata.update(
        {'terasort_block_size_mb': FLAGS.dpb_terasort_block_size_mb})

  unsorted_dir = base_dir + 'unsorted'
  sorted_dir = base_dir + 'sorted'
  report_dir = base_dir + 'report'
  stages = [('teragen', [str(FLAGS.dpb_terasort_num_records), unsorted_dir]),
            ('terasort', [unsorted_dir, sorted_dir]),
            ('teravalidate', [sorted_dir, report_dir])]
  cumulative_runtime = 0
  for (stage, args) in stages:
    result = RunStage(spec, stage, args)
    logging.info(result)
    results.append(
        sample.Sample(stage + '_run_time', result.run_time, 'seconds',
                      metadata))
    results.append(
        sample.Sample(stage + '_wall_time', result.wall_time, 'seconds',
                      metadata))
    cumulative_runtime += result.run_time
  results.append(
      sample.Sample('cumulative_runtime', cumulative_runtime, 'seconds',
                    metadata))
  return results


def Cleanup(spec: benchmark_spec.BenchmarkSpec):
  """Cleans up the terasort benchmark."""
  del spec  # unused


def RunStage(spec: benchmark_spec.BenchmarkSpec, stage: str,
             stage_args: List[str]) -> dpb_service.JobResult:
  """Runs one of the 3 job stages of Terasort.

  Args:
    spec: BencharkSpec; the benchmark_spec
    stage: str; name of the stage being executed
    stage_args: List[str]; arguments for the stage.

  Returns:
    JobResult of running job.

  Raises:
    JobSubmissionError if job fails.
  """
  service = spec.dpb_service
  if service.dpb_service_type == dpb_service.UNMANAGED_DPB_SVC_YARN_CLUSTER:
    jar = UNMANAGED_TERASORT_JAR
  else:
    jar = TERASORT_JAR
  args = [stage]
  if FLAGS.dpb_terasort_block_size_mb:
    scheme = service.PERSISTENT_FS_PREFIX.strip(':/')
    args.append('-Dfs.{}.block.size={}'.format(
        scheme, FLAGS.dpb_terasort_block_size_mb * 1024 * 1024))
  args += stage_args
  return service.SubmitJob(
      jarfile=jar, job_arguments=args, job_type=BaseDpbService.HADOOP_JOB_TYPE)
