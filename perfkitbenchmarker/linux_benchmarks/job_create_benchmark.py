"""Example benchmark that runs a simple hello world program on cloud run jobs."""

from typing import Any
from absl import logging
from perfkitbenchmarker import benchmark_spec as benchmark_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.providers.gcp import google_cloud_run_jobs


BENCHMARK_NAME = 'job_create_benchmark'
BENCHMARK_CONFIG = """
job_create_benchmark:
  description: Example benchmark that measures the time to create a cloud run job."""


# us-docker.pkg.dev/cloudrun/container/hello is a public image.


def GetConfig(user_config) -> dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: benchmark_spec_lib.BenchmarkSpec) -> None:
  logging.info('Running Prepare phase of the hello benchmark')
  del benchmark_spec  # to avoid unused variable warning


def Run(
    benchmark_spec: benchmark_spec_lib.BenchmarkSpec,
) -> list[sample.Sample]:
  """Runs the benchmark."""

  resource = google_cloud_run_jobs.GoogleCloudRunJob(
      'us-central1', 'us-docker.pkg.dev/cloudrun/container/hello'
  )
  resource.Create()
  logging.info('Running Run phase of the hello benchmark')
  metadata = {'logged_message': 'hello from kenean!'}
  samples = []
  samples.append(sample.Sample('Example Sample Latency', 0.5, 'ms', metadata))
  samples.append(
      sample.Sample(
          'resource_create_time',
          resource.create_end_time - resource.create_start_time,
          's',
          metadata,
      )
  )
  resource.Delete()
  del benchmark_spec  # to avoid unused variable warning
  return samples


def Cleanup(benchmark_spec: benchmark_spec_lib.BenchmarkSpec) -> None:
  logging.info('Running Cleanup phase of the hello benchmark')
  del benchmark_spec  # to avoid unused variable warning
