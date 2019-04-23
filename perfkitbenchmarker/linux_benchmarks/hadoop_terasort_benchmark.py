# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a jar using a cluster that supports Apache Hadoop MapReduce.

This benchmark takes runs the Apache Hadoop MapReduce Terasort benchmark on an
Hadoop YARN cluster. The cluster can be one supplied by a cloud provider,
such as Google's Dataproc or Amazon's EMR.

It records how long each phase (generate, sort, validate) takes to run.
For each phase, it reports the wall clock time, but this number should
be used with caution, as it some platforms (such as AWS's EMR) use polling
to determine when the job is done, so the wall time is inflated
Furthermore, if the standard output of the job is retrieved, AWS EMR's
time is again inflated because it takes extra time to get the output.

If available, it will also report a pending time (the time between when the
job was received by the platform and when it ran), and a runtime, which is
the time the job took to run, as reported by the underlying cluster.

For more on Apache Hadoop, see: http://hadoop.apache.org/
"""

import copy
import datetime
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import flags



BENCHMARK_NAME = 'hadoop_terasort'
BENCHMARK_CONFIG = """
hadoop_terasort:
  description: Run the Apache Hadoop MapReduce Terasort benchmark on a cluster.
  spark_service:
    service_type: managed
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          boot_disk_size: 500
        AWS:
          machine_type: m4.xlarge
      vm_count: 2
"""

TERAGEN = 'teragen'
TERASORT = 'terasort'
TERAVALIDATE = 'teravalidate'

flags.DEFINE_integer('terasort_num_rows', 10000,
                     'Number of 100-byte rows to generate.')
flags.DEFINE_string('terasort_unsorted_dir', 'tera_gen_data', 'Location of '
                    'the unsorted data. TeraGen writes here, and TeraSort '
                    'reads from here.')

flags.DEFINE_string('terasort_data_base', 'terasort_data/',
                    'The benchmark will append to this to create three '
                    'directories: one for the generated, unsorted data, '
                    'one for the sorted data, and one for the validate '
                    'data.  If using a static cluster or if using object '
                    'storage buckets, you must cleanup.')
flags.DEFINE_bool('terasort_append_timestamp', True, 'Append a timestamp to '
                  'the directories given by terasort_unsorted_dir, '
                  'terasort_sorted_dir, and terasort_validate_dir')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):
  """Executes the given jar on the specified Spark cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  spark_cluster = benchmark_spec.spark_service
  start = datetime.datetime.now()
  terasort_jar = spark_cluster.GetExampleJar(spark_service.HADOOP_JOB_TYPE)
  results = []
  metadata = copy.copy(spark_cluster.GetMetadata())
  logging.info('metadata %s ' % str(metadata))
  base_dir = FLAGS.terasort_data_base
  if FLAGS.terasort_append_timestamp:
    time_string = datetime.datetime.now().strftime('%Y%m%d%H%S')
    base_dir += time_string
    base_dir += '/'
  unsorted_dir = base_dir + 'unsorted'
  sorted_dir = base_dir + 'sorted'
  validate_dir = base_dir + 'validate'

  metadata.update({'terasort_num_rows': FLAGS.terasort_num_rows,
                   'terasort_sorted_dir': sorted_dir,
                   'terasort_unsorted_dir': unsorted_dir,
                   'terasort_validate_dir': validate_dir})
  gen_args = [TERAGEN, str(FLAGS.terasort_num_rows), unsorted_dir]
  sort_args = [TERASORT, unsorted_dir, sorted_dir]
  validate_args = [TERAVALIDATE, sorted_dir, validate_dir]

  stages = [('generate', gen_args),
            ('sort', sort_args),
            ('validate', validate_args)]
  cumulative_runtime = 0
  for (label, args) in stages:
    stats = spark_cluster.SubmitJob(terasort_jar,
                                    None,
                                    job_type=spark_service.HADOOP_JOB_TYPE,
                                    job_arguments=args)
    if not stats[spark_service.SUCCESS]:
      raise Exception('Stage {0} unsuccessful'.format(label))
    current_time = datetime.datetime.now()
    wall_time = (current_time - start).total_seconds()
    results.append(sample.Sample(label + '_wall_time',
                                 wall_time,
                                 'seconds', metadata))
    start = current_time

    if spark_service.RUNTIME in stats:
      results.append(sample.Sample(label + '_runtime',
                                   stats[spark_service.RUNTIME],
                                   'seconds', metadata))
    cumulative_runtime += stats[spark_service.RUNTIME]
    if spark_service.WAITING in stats:
      results.append(sample.Sample(label + '_pending_time',
                                   stats[spark_service.WAITING],
                                   'seconds', metadata))
  results.append(sample.Sample('cumulative_runtime',
                               cumulative_runtime,
                               'seconds', metadata))
  if not spark_cluster.user_managed:
    create_time = (spark_cluster.resource_ready_time -
                   spark_cluster.create_start_time)
    results.append(sample.Sample('cluster_create_time',
                                 create_time,
                                 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass
