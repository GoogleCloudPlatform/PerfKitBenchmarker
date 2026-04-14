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

"""Run memtier_benchmark against a K8s cluster.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""

from collections.abc import Callable, Iterable
import itertools
import textwrap
from typing import Any, NamedTuple

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

NAMESPACE = flags.DEFINE_string(
    'kubernetes_redis_memtier_namespace',
    None,
    'Namespace to use for resources created by the benchmark. Intended for use'
    ' in development and testing.',
)
SAVE = flags.DEFINE_string(
    'kubernetes_redis_memtier_save',
    None,
    'Value of the `save` directive in redis.conf.',
)
BENCHMARK_NAME = 'kubernetes_redis_memtier'
BENCHMARK_CONFIG = """
kubernetes_redis_memtier:
  description: >
      Run memtier_benchmark against Redis on Kubernetes.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec: *default_dual_core
    nodepools:
      servers:
        vm_spec:
          GCP:
            machine_type: c4-standard-4
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-32
        vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def CheckPrerequisites(_):
  """Verifies that benchmark setup is correct."""
  if (
      redis_server.IO_THREADS.present
      and len(redis_server.IO_THREADS.value) != 1
  ):
    raise errors.Setup.InvalidFlagConfigurationError(
        'kubernetes_redis_memtier_benchmark only supports a single value for'
        ' redis_server_io_threads.'
    )


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_server_machine_type

  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_client_machine_type

  return config


def Prepare(_: _BenchmarkSpec) -> None:
  """Prepares a cluster to run the Redis benchmark."""
  # Set up the Kubernetes Service, Redis configuration, and Redis server.
  if not FLAGS['redis_server_io_threads_do_reads'].present:
    do_reads = None
  else:
    do_reads = 'yes' if FLAGS.redis_server_io_threads_do_reads else 'no'

  io_threads = (
      redis_server.IO_THREADS.value[0]
      if redis_server.IO_THREADS.present
      else None
  )

  kubernetes_commands.ApplyManifest(
      'container/kubernetes_redis_memtier/kubernetes_redis_memtier.yaml.j2',
      k8s_namespace=NAMESPACE.value,
      redis_parameters={
          'io-threads': io_threads,
          'io-threads-do-reads': do_reads,
          'save': SAVE.value,
      },
      redis_version=FLAGS.redis_server_version,
      redis_port=redis_server.DEFAULT_PORT,
  )
  kubernetes_commands.WaitForRollout(
      'statefulset/redis', namespace=NAMESPACE.value
  )


def _RunMemtier(
    clients: int, threads: int, pipeline: int
) -> list[sample.Sample]:
  """Runs memtier_benchmark against Redis."""
  # Run this before the run (instead of after) so that logs can be manually
  # inspected if a run fails.
  kubectl_args = ['delete', 'job', 'memtier', '--ignore-not-found']
  if NAMESPACE.value:
    kubectl_args.append(f'--namespace={NAMESPACE.value}')
  kubectl.RunKubectlCommand(kubectl_args)

  server = f'redis.{NAMESPACE.value or 'default'}.svc.cluster.local'
  memtier_command = memtier.BuildMemtierCommand(
      server=server,
      protocol=memtier.MEMTIER_PROTOCOL.value,
      clients=clients,
      threads=threads,
      ratio=memtier.MEMTIER_RATIO.value,
      data_size=memtier.MEMTIER_DATA_SIZE.value,
      pipeline=pipeline,
      key_minimum=1,
      key_maximum=memtier.MEMTIER_KEY_MAXIMUM.value,
      random_data=memtier.MEMTIER_RANDOM_DATA.value,
      distinct_client_seed=memtier.MEMTIER_DISTINCT_CLIENT_SEED.value,
      test_time=memtier.MEMTIER_RUN_DURATION.value,
  )
  logging.info('memtier command to be run: %s', ' '.join(memtier_command))

  created_resources = kubernetes_commands.ApplyManifest(
      'container/kubernetes_redis_memtier/kubernetes_memtier_job.yaml.j2',
      k8s_namespace=NAMESPACE.value,
      memtier_version='2.2.1',
      memtier_command=memtier_command[0],
      memtier_args=memtier_command[1:],
  )

  job_name = list(created_resources)[0].split('/')[1]
  # Make sure the pod is properly created
  kubernetes_commands.WaitForResource(
      'pod',
      condition_type='',
      condition_name='create',
      namespace=NAMESPACE.value,
      timeout=60,
      extra_args=['-l', f'job-name={job_name}'],
  )
  kubernetes_commands.WaitForResource(
      'pod',
      'Initialized',
      namespace=NAMESPACE.value,
      timeout=10,
      extra_args=['-l', f'job-name={job_name}'],
  )

  # Wait for the job to complete
  logging.info('Waiting for memtier Job %s to complete...', job_name)
  condition = kubernetes_commands.WaitForResourceForMultiConditions(
      f'jobs/{job_name}',
      ['condition=Complete', 'condition=Failed'],
      namespace=NAMESPACE.value,
      # Add 30 seconds to account for initial connection errors as Redis comes
      # up.
      timeout=memtier.MEMTIER_RUN_DURATION.value + 30,
  )
  if condition == 'condition=Failed':
    raise errors.Benchmarks.RunError(f"Memtier job '{job_name}' failed.")

  kubectl_args = ['logs', f'jobs/{job_name}']
  if NAMESPACE.value:
    kubectl_args.append(f'--namespace={NAMESPACE.value}')
  logs, _, _ = kubectl.RunKubectlCommand(kubectl_args, raise_on_failure=True)
  result = memtier.MemtierResult.Parse(logs, None)
  metadata = memtier.GetMetadata(
      clients=clients,
      threads=threads,
      pipeline=pipeline,
  )

  return result.GetSamples(metadata)


class MemtierRunConfig(NamedTuple):
  clients: int
  threads: int
  pipeline: int


def _CreateRunConfigMatrix[T](
    nt: Callable[..., T], **iterables: Iterable[Any]
) -> list[T]:
  """Creates a run matrix for the Redis benchmark.

  Args:
    nt: The NamedTuple class to use for the run config.
    **iterables: Keyword arguments where the values are parameters to generate
      the run config matrix.

  Returns:
    A list of NamedTuple instances representing the run configs.
  """
  run_configs = [nt(*p) for p in itertools.product(*iterables.values())]
  logging.info(
      'Generated run configs:\n%s',
      textwrap.indent('\n'.join([str(c) for c in run_configs]), '  '),
  )
  return run_configs


def Run(_: _BenchmarkSpec) -> list[sample.Sample]:
  """Run the benchmark."""
  samples: list[sample.Sample] = []

  run_configs = _CreateRunConfigMatrix(
      MemtierRunConfig,
      clients=FLAGS.memtier_clients,
      threads=FLAGS.memtier_threads,
      pipeline=FLAGS.memtier_pipeline,
  )

  for run_config in run_configs:
    run_samples = _RunMemtier(
        run_config.clients, run_config.threads, run_config.pipeline
    )
    samples.extend(run_samples)

  return samples


def Cleanup(_: _BenchmarkSpec) -> None:
  pass
