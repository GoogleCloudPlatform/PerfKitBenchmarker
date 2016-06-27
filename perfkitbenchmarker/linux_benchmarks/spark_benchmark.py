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
import logging
import os
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import spark_service
from perfkitbenchmarker import flags



BENCHMARK_NAME = 'spark'
BENCHMARK_CONFIG = """
spark:
  description: Run a jar on a spark cluster.
  spark_service:
    service_type: managed
    num_workers: 4
"""

# This points to a file on the spark cluster.
DEFAULT_JARFILE = 'file:///usr/lib/spark/lib/spark-examples.jar'
DEFAULT_CLASSNAME = 'org.apache.spark.examples.SparkPi'

flags.DEFINE_string('spark_jarfile', DEFAULT_JARFILE,
                    'Jarfile to submit.')
flags.DEFINE_string('spark_classname', DEFAULT_CLASSNAME,
                    'Classname to be used')
flags.DEFINE_bool('spark_print_stdout', True, 'Print the standard '
                  'output of the job')
flags.DEFINE_list('spark_job_arguments', [], 'Arguments to be passed '
                  'to the class given by spark_classname')

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
  jar_start = datetime.datetime.now()

  stdout_path = None
  results = []
  try:
    if FLAGS.spark_print_stdout:
      # We need to get a name for a temporary file, so we create
      # a file, then close it, and use that path name.
      stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                                prefix='spark_benchmark',
                                                delete=False)
      stdout_path = stdout_file.name
      stdout_file.close()

    stats = spark_cluster.SubmitJob(FLAGS.spark_jarfile,
                                    FLAGS.spark_classname,
                                    job_arguments=FLAGS.spark_job_arguments,
                                    job_stdout_file=stdout_path)
    if not stats[spark_service.SUCCESS]:
      raise Exception('Class {0} from jar {1} did not run'.format(
          FLAGS.spark_classname, FLAGS.spark_jarfile))
    jar_end = datetime.datetime.now()
    if stdout_path:
      with open(stdout_path, 'r') as f:
        logging.info('The output of the job is ' + f.read())
    metadata = spark_cluster.GetMetadata()
    metadata.update({'jarfile': FLAGS.spark_jarfile,
                     'class': FLAGS.spark_classname,
                     'job_arguments': str(FLAGS.spark_job_arguments),
                     'print_stdout': str(FLAGS.spark_print_stdout)})

    results.append(sample.Sample('wall_time',
                                 (jar_end - jar_start).total_seconds(),
                                 'seconds', metadata))
    if spark_service.RUNTIME in stats:
      results.append(sample.Sample('runtime',
                                   stats[spark_service.RUNTIME],
                                   'seconds', metadata))
    if spark_service.WAITING in stats:
      results.append(sample.Sample('pending_time',
                                   stats[spark_service.WAITING],
                                   'seconds', metadata))


    if not spark_cluster.user_managed:
      create_time = (spark_cluster.resource_ready_time -
                     spark_cluster.create_start_time)
      results.append(sample.Sample('cluster_create_time', create_time,
                                   'seconds', metadata))
  finally:
    if stdout_path and os.path.isfile(stdout_path):
      os.remove(stdout_path)

  return results


def Cleanup(benchmark_spec):
  pass
