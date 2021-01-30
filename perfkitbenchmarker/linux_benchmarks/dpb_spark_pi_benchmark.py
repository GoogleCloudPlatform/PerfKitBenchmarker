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
r"""Runs a spark application on a cluster that computes an approximation to pi.

The spark application accepts one argument, which is the number of partitions
used for the task of computing Pi. In fact the level of parallelization is
100000 * partitions.

The benchmark reports
  - wall time: The total time to complete from submission.
  - running_time: Actual execution time (without the pending time).

The wall tme may be inflated due to the use of polling to ascertain job
completion.
"""
import logging
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'dpb_spark_pi_benchmark'
BENCHMARK_CONFIG = """
dpb_spark_pi_benchmark:
  flags:
    cloud: GCP
    dpb_service_zone: us-east1-b
  description: >
      Create a dpb cluster and Run Spark Pi application.
  dpb_service:
    service_type: dataproc
    worker_group:
      vm_spec:
        AWS:
          machine_type: m5.large
        Azure:
          machine_type: Standard_F2s_v2
        GCP:
          machine_type: n1-standard-2
      disk_spec:
        AWS:
          disk_type: st1
          disk_size: 500
          # Only used by unmanaged
          mount_point: /scratch
        Azure:
          disk_type: Standard_LRS
          disk_size: 500
          # Only used by unmanaged
          mount_point: /scratch
        GCP:
          disk_type: pd-standard
          disk_size: 500
          # Only used by unmanaged
          mount_point: /scratch
    worker_count: 2
"""

flags.DEFINE_integer('dpb_spark_pi_partitions', 100, 'Number of task'
                                                     ' partitions.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(unused_benchmark_spec):
  pass


def Run(benchmark_spec):
  """Executes the given jar on the specified Spark cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.

  Raises:
    JobSubmissionError if the job fails.
  """

  metadata = {}
  cluster = benchmark_spec.dpb_service
  metadata.update(cluster.GetMetadata())
  num_partitions = str(FLAGS.dpb_spark_pi_partitions)
  metadata.update({'spark_pi_partitions': num_partitions})

  results = []

  result = cluster.SubmitJob(
      job_type='spark',
      jarfile=cluster.GetExecutionJar('spark', 'examples'),
      classname='org.apache.spark.examples.SparkPi',
      job_arguments=[num_partitions])
  logging.info(result)
  results.append(
      sample.Sample('wall_time', result.wall_time, 'seconds', metadata))
  results.append(
      sample.Sample('run_time', result.run_time, 'seconds', metadata))
  return results


def Cleanup(unused_benchmark_spec):
  pass
