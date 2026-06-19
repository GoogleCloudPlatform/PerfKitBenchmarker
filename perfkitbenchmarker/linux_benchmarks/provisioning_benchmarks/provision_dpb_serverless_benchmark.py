# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

"""Measures provisioning times for Dataproc Serverless and competitors.

This benchmark submits a minimal dummy PySpark job and measures the total
latency from the moment the job is submitted until the client returns.
"""

import os
import time
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'provision_dpb_serverless'

BENCHMARK_CONFIG = """
provision_dpb_serverless:
  description: Measure provisioning time for DPB serverless services
  dpb_service:
    service_type: dataproc_serverless
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
        AWS:
          machine_type: m5.xlarge
      disk_spec:
        GCP:
          disk_size: 1000
          disk_type: pd-standard
        AWS:
          disk_size: 1000
          disk_type: gp2
    worker_count: 2
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Prepares the benchmark."""
  del benchmark_spec


def Run(benchmark_spec):
  """Runs the benchmark.

  Args:
    benchmark_spec: Spec needed to run the benchmark.

  Returns:
    A list of samples, comprised of the client-side wall time and
    service-reported times.
  """
  cluster = benchmark_spec.dpb_service
  storage_service = cluster.storage_service

  # Stage the PySpark script to bucket to ensure portability across providers.
  local_script_path = data.ResourcePath(
      os.path.join('spark_sql_test_scripts', 'spark_select_1.py')
  )
  bucket_script_path = os.path.join(
      cluster.base_dir, 'spark_sql_test_scripts', 'spark_select_1.py'
  )
  storage_service.Copy(local_script_path, bucket_script_path)

  metadata = cluster.GetResourceMetadata()
  metadata['job_details'] = 'select_1'

  # Timer Start: Just before calling the blocking execution.
  start_time = time.time()

  job_result = cluster.SubmitJob(
      pyspark_file=bucket_script_path,
      job_type=dpb_constants.PYSPARK_JOB_TYPE,
  )

  # Timer End: After the blocking call returned.
  end_time = time.time()

  total_elapsed_time = end_time - start_time

  samples = [
      sample.Sample(
          'spark_serverless_compute_job_completion_time',
          total_elapsed_time,
          'seconds',
          metadata,
      ),
  ]

  # Also capture service-reported times for deeper analysis.
  if job_result.run_time:
    samples.append(
        sample.Sample(
            'service_reported_job_run_time',
            job_result.run_time,
            'seconds',
            metadata,
        )
    )
  if job_result.pending_time:
    samples.append(
        sample.Sample(
            'service_reported_job_pending_time',
            job_result.pending_time,
            'seconds',
            metadata,
        )
    )

  return samples


def Cleanup(benchmark_spec):
  """Cleans up the benchmark."""
  del benchmark_spec
