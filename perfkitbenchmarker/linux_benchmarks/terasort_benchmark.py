# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a jar using a cluster that supports Apache Spark.

This benchmark takes a jarfile and class name, and runs that class
using an Apache Spark cluster.  The Apache Spark cluster can be one
supplied by a cloud provider, such as Google's Dataproc.

By default, it runs SparkPi.

It records how long the job takes to run.  It always reports the
wall clock time, but this number should be used with caution, as it
some platforms (such as AWS's EMR) use polling to determine when
the job is done, so the wall time is inflated.  Furthermore, if the standard
output of the job is retrieved, AWS EMR's time is again inflated because
it takes extra time to get the output.

If available, it will also report a pending time (the time between when the
job was received by the platform and when it ran), and a runtime, which is
the time the job took to run, as reported by the underlying cluster.

For more on Apache Spark, see: http://spark.apache.org/
"""

import datetime

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import flags



BENCHMARK_NAME = 'terasort_benchmark'
BENCHMARK_CONFIG = """
terasort_benchmark:
  description: Run the Apache hadoop terasort benchmark on a cluster.
  spark_service:
    service_type: managed
    num_workers: 4
"""

# This points to a file on the spark cluster.
TERASORT_JARFILE = ('file:///usr/lib/hadoop-mapreduce/'
                    'hadoop-mapreduce-examples.jar')
TERAGEN_CLASSNAME = 'org.apache.hadoop.examples.terasort.TeraGen'
TERASORT_CLASSNAME = 'org.apache.hadoop.examples.terasort.TeraSort'
TERAVALIDATE_CLASSNAME = 'org.apache.hadoop.examples.terasort.TeraValidate'

flags.DEFINE_integer('terasort_dataset_size', 10000,
                     'Data set size to generate')
flags.DEFINE_string('terasort_unsorted_dir', 'tera_gen_data', 'Location of '
                    'the unsorted data. TeraGen writes here, and TeraSort '
                    'reads from here.')

flags.DEFINE_string('terasort_sorted_dir', 'tera_sort_dir', 'Location for the '
                    'sorted data. TeraSort writes to here, TeraValidate reads '
                    'from here.')
flags.DEFINE_string('terasort_validate_dir', 'tera_validate_dir', 'Output of '
                    'the TeraValidate command')

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

  results = []
  metadata = spark_cluster.GetMetadata()
  gen_args = [str(FLAGS.terasort_dataset_size), FLAGS.terasort_unsorted_dir]
  sort_args = [FLAGS.terasort_unsorted_dir, FLAGS.terasort_sorted_dir]
  validate_args = [FLAGS.terasort_sorted_dir, FLAGS.terasort_validate_dir]

  stages = [('generate', TERAGEN_CLASSNAME, gen_args),
            ('sort', TERASORT_CLASSNAME, sort_args),
            ('validate', TERAVALIDATE_CLASSNAME, validate_args)]
  for (label, classname, args) in stages:
    stats = spark_cluster.SubmitJob(TERASORT_JARFILE,
                                    classname,
                                    job_type=spark_service.HADOOP_JOB_TYPE,
                                    job_arguments=args)
    if not stats[spark_service.SUCCESS]:
      raise Exception('Stage {0} unsuccessful'.format(label))
    current_time = datetime.datetime.now()
    results.append(sample.Sample(label + '_wall_time',
                                 (current_time - start).total_seconds(),
                                 'seconds', metadata))
    start = current_time

    if spark_service.RUNTIME in stats:
      results.append(sample.Sample(label + '_runtime',
                                   stats[spark_service.RUNTIME],
                                   'seconds', metadata))
    if spark_service.WAITING in stats:
      results.append(sample.Sample(label + '_pending_time',
                                   stats[spark_service.WAITING],
                                   'seconds', metadata))

  if not spark_cluster.user_managed:
    create_time = (spark_cluster.resource_ready_time -
                   spark_cluster.create_start_time)
    results.append(sample.Sample('cluster_create_time', create_time,
                                 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass
