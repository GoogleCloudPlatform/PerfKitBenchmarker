"""Example benchmark that creates a job, measures the time to create it, and deletes it."""

import time
from typing import Any
from absl import logging
from perfkitbenchmarker import benchmark_spec as benchmark_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import google_cloud_run_jobs


BENCHMARK_NAME = 'job_create_benchmark'
BENCHMARK_CONFIG = """
job_create_benchmark:
  description: Example benchmark that measures the time to create a cloud run job.
  container_registry: {}
"""


def GetConfig(user_config) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: benchmark_spec_lib.BenchmarkSpec) -> None:
  logging.info('Running Prepare phase of the job_create benchmark')
  del benchmark_spec  # to avoid unused variable warning


def Run(
    benchmark_spec: benchmark_spec_lib.BenchmarkSpec,
) -> list[sample.Sample]:
  """Runs the benchmark."""

  resource = google_cloud_run_jobs.GoogleCloudRunJob(
      'us-central1', 'us-docker.pkg.dev/cloudrun/container/hello-job'
  )
  logging.info('Running Run phase of the job_create benchmark')
  metadata = {'logged_message': 'hello from kenean!'}
  resource.Create()
  resource.Execute()
  samples = resource.GetSamples()

  execute_start_time = time.time()
  resource.Execute()
  execute_end_time = time.time()
  samples.append(
      sample.Sample(
          'resource_end_to_end_execution_time',
          execute_end_time - execute_start_time,
          'seconds',
          metadata,
      )
  )

  samples.append(
      sample.Sample(
          'execute_cli_time',
          execute_end_time - resource.create_start_time,
          'seconds',
          metadata,
      )
  )

  resource.Delete()
  del benchmark_spec  # to avoid unused variable warning

  return samples


def Cleanup(benchmark_spec: benchmark_spec_lib.BenchmarkSpec) -> None:
  logging.info('Running Cleanup phase of the job_create benchmark')
  del benchmark_spec  # to avoid unused variable warning
