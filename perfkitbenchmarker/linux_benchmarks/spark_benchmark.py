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

It records how long the job takes to run.

For more on Apache Spark, see: http://spark.apache.org/
"""

import datetime

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
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
  success = spark_cluster.SubmitJob(FLAGS.spark_jarfile,
                                    FLAGS.spark_classname)
  if not success:
    raise Exception('Class {0} from jar {1} did not run'.format(
        FLAGS.spark_classname, FLAGS.spark_jarfile))
  jar_end = datetime.datetime.now()

  metadata = spark_cluster.GetMetadata().update(
      {'jarfile': FLAGS.spark_jarfile, 'class': FLAGS.spark_classname})

  results = []
  results.append(sample.Sample('jar_time',
                               (jar_end - jar_start).total_seconds(),
                               'seconds', metadata))

  if not spark_cluster.user_managed:
    create_time = (spark_cluster.create_end_time -
                   spark_cluster.create_start_time)
    results.append(sample.Sample('cluster_create_time', create_time, 'seconds',
                                 metadata))

  return results


def Cleanup(benchmark_spec):
  pass
