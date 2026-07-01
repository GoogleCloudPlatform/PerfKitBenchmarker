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

Supported clouds: GCP (GKE) and AWS (EKS).  Pass --cloud=AWS to run on EKS.

Swap encryption toggle
----------------------
Pass --kubernetes_redis_memtier_swap_enabled=true to run with dm-crypt
encrypted swap on the Redis servers nodepool.  When the flag is set:

* GetConfig() upgrades the servers nodepool to a high-memory machine type
  (n4-highmem-32 on GCP, r6i.8xlarge on AWS), sets the appropriate boot
  disk type (hyperdisk-balanced on GCP, gp3 on AWS), and injects a
  swap_config block so the nodepool is provisioned with swap enabled.
* Prepare() deploys a privileged SwapDaemonSet onto the servers nodepool
  that activates dm-crypt encrypted swap on every node.
* Run() attaches swap metadata (swap_enabled, swap_swappiness,
  swap_machine_type, ...) to every sample.
* Cleanup() tears down the SwapDaemonSet.

Note: EKS swap activation via nodeadm is deferred to PR #6780.
"""

from collections.abc import Callable, Iterable
import itertools
import textwrap
from typing import Any, NamedTuple, TypeVar

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
from perfkitbenchmarker.resources.container_service import (
    swap_daemonset as _swap_daemonset_lib
)

FLAGS = flags.FLAGS

NAMESPACE = flags.DEFINE_string(
    'kubernetes_redis_memtier_namespace',
    None,
    ('Namespace to use for resources created by the benchmark. Intended for'
     ' use in development and testing.'),
)
SAVE = flags.DEFINE_string(
    'kubernetes_redis_memtier_save',
    None,
    'Value of the `save` directive in redis.conf.',
)
SWAP_ENABLED = flags.DEFINE_boolean(
    'kubernetes_redis_memtier_swap_enabled',
    False,
    ('If True, runs the Redis servers nodepool with dm-crypt encrypted swap. '
     'Injects swap_config into the servers nodepool, deploys a SwapDaemonSet '
     'in Prepare(), and attaches swap metadata to all samples in Run().'),
)
SWAP_SWAPPINESS = flags.DEFINE_integer(
    'kubernetes_redis_memtier_swap_swappiness',
    100,
    'Kernel vm.swappiness value when --kubernetes_redis_memtier_swap_enabled.',
    lower_bound=0,
    upper_bound=200,
)

BENCHMARK_NAME = 'kubernetes_redis_memtier'
BENCHMARK_CONFIG = """
kubernetes_redis_memtier:
  description: >
      Run memtier_benchmark against Redis on Kubernetes.
      Supports GCP (GKE) and AWS (EKS). Use --cloud=AWS to run on EKS.
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
          AWS:
            machine_type: m5.xlarge
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-32
          AWS:
            machine_type: c5.8xlarge
        vm_count: 1
"""

_SWAP_SERVERS_NODEPOOL = 'servers'
# GCP swap defaults: n4-highmem-32 + hyperdisk-balanced
_SWAP_MACHINE_TYPE = 'n4-highmem-32'
_SWAP_BOOT_DISK_TYPE = 'hyperdisk-balanced'
_SWAP_BOOT_DISK_SIZE = 500
_SWAP_BOOT_DISK_IOPS = 160000
_SWAP_BOOT_DISK_THROUGHPUT = 2400
# AWS swap defaults: r6i.8xlarge (32 vCPU / 256 GiB) + gp3
_SWAP_AWS_MACHINE_TYPE = 'r6i.8xlarge'
_SWAP_AWS_BOOT_DISK_TYPE = 'gp3'
_SWAP_AWS_BOOT_DISK_SIZE = 500
_SWAP_AWS_BOOT_DISK_IOPS = 16000
_SWAP_AWS_BOOT_DISK_THROUGHPUT = 1000
# Shared sysctl defaults (GCP and AWS)
_SWAP_MIN_FREE_KBYTES = 67584
_SWAP_WATERMARK_SCALE_FACTOR = 500
_SWAP_DS_NAME = 'pkb-redis-memtier-swap'
_SWAP_DS_LABEL = 'pkb-redis-memtier-swap'
_SWAP_DS_IMAGE = 'ubuntu:22.04'
_SWAP_DS_NAMESPACE = 'kube-system'

_BenchmarkSpec = benchmark_spec.BenchmarkSpec
_T = TypeVar('_T')


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
  """Load and return benchmark config spec.

  When --kubernetes_redis_memtier_swap_enabled is True the servers nodepool is
  upgraded to a high-memory machine type and a swap_config block is injected so
  the nodepool provisions encrypted swap automatically.  Supports GCP and AWS:

  GCP: n4-highmem-32 + hyperdisk-balanced (160k IOPS / 2400 MiB/s)
  AWS: r6i.8xlarge + gp3 (16k IOPS / 1000 MiB/s); nodeadm swap deferred to
       PR #6780 -- EksSwapConfig._Create() is a stub that logs a warning.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['container_cluster']['nodepools']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_server_machine_type

  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_client_machine_type

  if SWAP_ENABLED.value:
    server_np = config['container_cluster']['nodepools'][_SWAP_SERVERS_NODEPOOL]
    for cloud in server_np.get('vm_spec', {}):
      if cloud == 'GCP':
        # --redis_memtier_server_machine_type takes precedence over the swap
        # default; only set the swap machine type if not explicitly overridden.
        if not FLAGS.redis_memtier_server_machine_type:
          server_np['vm_spec'][cloud]['machine_type'] = _SWAP_MACHINE_TYPE
        server_np['vm_spec'][cloud]['boot_disk_type'] = _SWAP_BOOT_DISK_TYPE
        server_np['vm_spec'][cloud]['boot_disk_size'] = _SWAP_BOOT_DISK_SIZE
      elif cloud == 'AWS':
        if not FLAGS.redis_memtier_server_machine_type:
          server_np['vm_spec'][cloud]['machine_type'] = _SWAP_AWS_MACHINE_TYPE
        server_np['vm_spec'][cloud]['boot_disk_type'] = _SWAP_AWS_BOOT_DISK_TYPE
        server_np['vm_spec'][cloud]['boot_disk_size'] = _SWAP_AWS_BOOT_DISK_SIZE
    # Shared sysctl settings apply to all clouds.
    # boot_disk_iops/throughput are cloud-specific provisioned I/O params.
    swap_config: dict[str, Any] = {
        'enabled': True,
        'swappiness': SWAP_SWAPPINESS.value,
        'min_free_kbytes': _SWAP_MIN_FREE_KBYTES,
        'watermark_scale_factor': _SWAP_WATERMARK_SCALE_FACTOR,
    }
    vm_spec = server_np.get('vm_spec', {})
    if 'GCP' in vm_spec:
      swap_config['boot_disk_iops'] = _SWAP_BOOT_DISK_IOPS
      swap_config['boot_disk_throughput'] = _SWAP_BOOT_DISK_THROUGHPUT
    elif 'AWS' in vm_spec:
      swap_config['boot_disk_iops'] = _SWAP_AWS_BOOT_DISK_IOPS
      swap_config['boot_disk_throughput'] = _SWAP_AWS_BOOT_DISK_THROUGHPUT
    server_np['swap_config'] = swap_config

  return config


def Prepare(_: _BenchmarkSpec) -> None:
  """Prepares a cluster to run the Redis benchmark."""
  # Deploy SwapDaemonSet before Redis so every servers node has encrypted swap
  # active when the Redis pods schedule.
  if SWAP_ENABLED.value:
    _swap_daemonset_lib.SwapDaemonSet(
        name=_SWAP_DS_NAME,
        namespace=_SWAP_DS_NAMESPACE,
        label=_SWAP_DS_LABEL,
        nodepool=_SWAP_SERVERS_NODEPOOL,
        image=_SWAP_DS_IMAGE,
    ).Create()

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

  server = f"redis.{NAMESPACE.value or 'default'}.svc.cluster.local"
  memtier_command = memtier.BuildMemtierCommand(
      server=server,
      # Redis protocol required; MEMTIER_PROTOCOL defaults to memcache_binary
      # which silently produces 0 ops against Redis.
      protocol=(
          memtier.MEMTIER_PROTOCOL.value
          if FLAGS['memtier_protocol'].present
          else 'redis'
      ),
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
      # Add 30 seconds to account for initial connection errors as Redis
      # comes up. When run_duration is None (request-count mode), fall back
      # to 3600s to accommodate image pulls, pod scheduling, and large
      # request counts.
      timeout=(
          memtier.MEMTIER_RUN_DURATION.value + 30
          if memtier.MEMTIER_RUN_DURATION.value is not None
          else 3600
      ),
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


def _CreateRunConfigMatrix(
    nt: Callable[..., _T], **iterables: Iterable[Any]
) -> list[_T]:
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


def _SwapMetadata() -> dict[str, Any]:
  """Returns metadata dict describing the active swap configuration.

  Picks cloud-specific machine type and disk constants based on FLAGS.cloud
  so samples carry accurate metadata whether run on GCP or AWS.
  """
  is_aws = FLAGS.cloud == 'AWS'
  return {
      'swap_enabled': True,
      'swap_swappiness': SWAP_SWAPPINESS.value,
      'swap_machine_type': (
          _SWAP_AWS_MACHINE_TYPE if is_aws else _SWAP_MACHINE_TYPE
      ),
      'swap_boot_disk_type': (
          _SWAP_AWS_BOOT_DISK_TYPE if is_aws else _SWAP_BOOT_DISK_TYPE
      ),
      'swap_boot_disk_size_gb': (
          _SWAP_AWS_BOOT_DISK_SIZE if is_aws else _SWAP_BOOT_DISK_SIZE
      ),
      'swap_boot_disk_iops': (
          _SWAP_AWS_BOOT_DISK_IOPS if is_aws else _SWAP_BOOT_DISK_IOPS
      ),
      'swap_boot_disk_throughput': (
          _SWAP_AWS_BOOT_DISK_THROUGHPUT
          if is_aws
          else _SWAP_BOOT_DISK_THROUGHPUT
      ),
      'swap_min_free_kbytes': _SWAP_MIN_FREE_KBYTES,
      'swap_watermark_scale_factor': _SWAP_WATERMARK_SCALE_FACTOR,
  }


def Run(_: _BenchmarkSpec) -> list[sample.Sample]:
  """Run the benchmark."""
  samples: list[sample.Sample] = []

  run_configs = _CreateRunConfigMatrix(
      MemtierRunConfig,
      clients=FLAGS.memtier_clients,
      threads=FLAGS.memtier_threads,
      pipeline=FLAGS.memtier_pipeline,
  )

  swap_meta = _SwapMetadata() if SWAP_ENABLED.value else {}

  for run_config in run_configs:
    run_samples = _RunMemtier(
        run_config.clients, run_config.threads, run_config.pipeline
    )
    if swap_meta:
      for s in run_samples:
        s.metadata.update(swap_meta)
    samples.extend(run_samples)

  return samples


def Cleanup(_: _BenchmarkSpec) -> None:
  """Tears down benchmark resources, including SwapDaemonSet if deployed."""
  if SWAP_ENABLED.value:
    _swap_daemonset_lib.SwapDaemonSet(
        name=_SWAP_DS_NAME,
        namespace=_SWAP_DS_NAMESPACE,
        label=_SWAP_DS_LABEL,
        nodepool=_SWAP_SERVERS_NODEPOOL,
        image=_SWAP_DS_IMAGE,
    ).Delete()
