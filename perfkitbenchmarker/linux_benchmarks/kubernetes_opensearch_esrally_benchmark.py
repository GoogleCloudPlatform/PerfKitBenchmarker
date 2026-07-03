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
"""Run esrally against OpenSearch on Kubernetes.

Architecture:
  OpenSearch runs as a single-pod StatefulSet on the 'servers' nodepool.
  esrally runs as a K8s Job on the 'clients' nodepool using
  --pipeline=benchmark-only against the OpenSearch ClusterIP Service.

  When --kubernetes_opensearch_esrally_swap_enabled is True the servers
  nodepool is provisioned with GKE linuxConfig.swapConfig (dm-crypt) so
  that OpenSearch bulk-index and query workloads execute under swap
  pressure (Phase 3c of the swap encryption methodology).

  This mirrors the pattern used in kubernetes_redis_memtier_benchmark for
  Phase 3a (Redis under swap): the same swap_config NodepoolSpec field
  introduced in PR1 is reused here, making this benchmark independent of
  PR2-PR5 and stackable directly on top of PR1.

Samples emitted (per esrally op_metric):
  <task>_throughput      docs/s or ops/s
  <task>_latency_mean    ms
  <task>_latency_p90     ms  (when available)
"""

import json
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec as bm_spec_lib
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS
_BenchmarkSpec = bm_spec_lib.BenchmarkSpec

BENCHMARK_NAME = 'kubernetes_opensearch_esrally'
BENCHMARK_CONFIG = """
kubernetes_opensearch_esrally:
  description: >
    Run esrally against OpenSearch on Kubernetes. Use
    --kubernetes_opensearch_esrally_swap_enabled to test OpenSearch
    bulk-index/query under GKE dm-crypt swap pressure (Phase 3c).
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
        zone: us-central1-a
      AWS:
        machine_type: t3.medium
        zone: us-east-1a
    nodepools:
      servers:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-8
            zone: us-central1-a
          AWS:
            machine_type: m6i.2xlarge
            zone: us-east-1a
      clients:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-central1-a
          AWS:
            machine_type: m6i.xlarge
            zone: us-east-1a
"""

_SWAP_ENABLED = flags.DEFINE_boolean(
    'kubernetes_opensearch_esrally_swap_enabled',
    False,
    ('When True, provision the servers nodepool with GKE dm-crypt swap '
     '(n4-highmem-32, hyperdisk-balanced, 160 k IOPS) to measure '
     'OpenSearch bulk-index and query latency under swap pressure. '
     'Requires PR1 swap_config support in NodepoolSpec.'),
)
_OPENSEARCH_VERSION = flags.DEFINE_string(
    'kubernetes_opensearch_esrally_opensearch_version',
    '2.13.0',
    'OpenSearch container image version to deploy as the benchmark target.',
)
_ESRALLY_TRACK = flags.DEFINE_string(
    'kubernetes_opensearch_esrally_track',
    'geonames',
    'esrally track to run against OpenSearch.',
)
_ESRALLY_CHALLENGE = flags.DEFINE_string(
    'kubernetes_opensearch_esrally_challenge',
    'append-no-conflicts',
    'esrally challenge to run within the specified track.',
)
_ESRALLY_VERSION = flags.DEFINE_string(
    'kubernetes_opensearch_esrally_version',
    '1.7.0',
    'opensearch-benchmark pip package version to install in the Job pod.',
)
_NAMESPACE = flags.DEFINE_string(
    'kubernetes_opensearch_esrally_namespace',
    '',
    ('Kubernetes namespace for all benchmark resources. '
     'Empty string uses the cluster default namespace.'),
)

# ---------------------------------------------------------------------------
# GCP swap constants -- mirrors kubernetes_redis_memtier_benchmark.py values
# ---------------------------------------------------------------------------
_SWAP_MACHINE_TYPE = 'n4-highmem-32'
_SWAP_BOOT_DISK_TYPE = 'hyperdisk-balanced'
_SWAP_BOOT_DISK_SIZE_GB = 500
_SWAP_BOOT_DISK_IOPS = 160000
_SWAP_BOOT_DISK_THROUGHPUT = 2400
_SWAP_MIN_FREE_KBYTES = 67584  # GKE minimum per Ajay review r3472513706

# AWS swap constants
_SWAP_AWS_MACHINE_TYPE = 'r6i.8xlarge'
_SWAP_AWS_BOOT_DISK_TYPE = 'gp3'
_SWAP_AWS_BOOT_DISK_SIZE_GB = 500

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------
_OPENSEARCH_HTTP_PORT = 9200
_RACE_ID = 'pkb-opensearch'
_RACE_JSON_BEGIN = '=== PKB_RACE_JSON_BEGIN ==='
_RACE_JSON_END = '=== PKB_RACE_JSON_END ==='


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Load config; inject swap_config on servers nodepool when flag is set."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if not _SWAP_ENABLED.value:
    return config

  server_np = config['container_cluster']['nodepools']['servers']

  # GCP: upgrade to high-memory machine with hyperdisk-balanced swap device.
  server_np['vm_spec']['GCP']['machine_type'] = _SWAP_MACHINE_TYPE
  server_np['vm_spec']['GCP']['boot_disk_type'] = _SWAP_BOOT_DISK_TYPE
  server_np['vm_spec']['GCP']['boot_disk_size'] = _SWAP_BOOT_DISK_SIZE_GB

  # AWS: upgrade to memory-optimised instance with gp3 swap volume.
  server_np['vm_spec']['AWS']['machine_type'] = _SWAP_AWS_MACHINE_TYPE
  server_np['vm_spec']['AWS']['boot_disk_type'] = _SWAP_AWS_BOOT_DISK_TYPE
  server_np['vm_spec']['AWS']['boot_disk_size'] = _SWAP_AWS_BOOT_DISK_SIZE_GB

  # swap_config is processed by PR1's NodepoolSpec (container_spec.py) and
  # google_kubernetes_engine.py which passes --system-config-from-file with
  # linuxConfig.swapConfig to gcloud during nodepool creation.
  server_np['swap_config'] = {
      'enabled': True,
      'swappiness': 100,
      'min_free_kbytes': _SWAP_MIN_FREE_KBYTES,
      'watermark_scale_factor': 500,
      'boot_disk_iops': _SWAP_BOOT_DISK_IOPS,
      'boot_disk_throughput': _SWAP_BOOT_DISK_THROUGHPUT,
  }
  return config


def Prepare(_: _BenchmarkSpec) -> None:
  """Deploy OpenSearch StatefulSet + Service; wait for pod Ready."""
  kubernetes_commands.ApplyManifest(
      'container/kubernetes_opensearch_esrally/opensearch.yaml.j2',
      k8s_namespace=_NAMESPACE.value,
      opensearch_version=_OPENSEARCH_VERSION.value,
      opensearch_port=_OPENSEARCH_HTTP_PORT,
  )
  kubernetes_commands.WaitForRollout(
      'statefulset/opensearch',
      namespace=_NAMESPACE.value,
      timeout=600,
  )
  logging.info('[kubernetes_opensearch_esrally] OpenSearch StatefulSet ready')


def Run(_: _BenchmarkSpec) -> list[sample.Sample]:
  """Run opensearch-benchmark as a K8s Job; parse results from pod logs."""
  ns = _NAMESPACE.value or 'default'

  # Job spec.template is immutable; delete any leftover Job from a prior run
  # so that ApplyManifest (kubectl apply) does not get an immutable-field error.
  kubectl.RunKubectlCommand(
      ['delete', 'job', 'esrally-pkb',
       f'--namespace={ns}', '--ignore-not-found=true'],
      raise_on_failure=False,
  )

  target = (
      f'opensearch.{ns}.svc.cluster.local:{_OPENSEARCH_HTTP_PORT}'
  )

  created = kubernetes_commands.ApplyManifest(
      'container/kubernetes_opensearch_esrally/esrally_job.yaml.j2',
      k8s_namespace=_NAMESPACE.value,
      race_id=_RACE_ID,
      target_hosts=target,
      esrally_track=_ESRALLY_TRACK.value,
      esrally_challenge=_ESRALLY_CHALLENGE.value,
      esrally_version=_ESRALLY_VERSION.value,
  )

  job_name = list(created)[0].split('/')[1]
  logging.info(
      '[kubernetes_opensearch_esrally] Waiting for esrally Job %s', job_name
  )
  condition = kubernetes_commands.WaitForResourceForMultiConditions(
      f'jobs/{job_name}',
      ['condition=Complete', 'condition=Failed'],
      namespace=_NAMESPACE.value,
      timeout=7200,
  )
  if condition == 'condition=Failed':
    raise errors.Benchmarks.RunError(
        f'[kubernetes_opensearch_esrally] esrally Job {job_name!r} failed.'
    )

  kubectl_args = ['logs', f'jobs/{job_name}']
  if _NAMESPACE.value:
    kubectl_args.append(f'--namespace={_NAMESPACE.value}')
  logs, _, _ = kubectl.RunKubectlCommand(
      kubectl_args, raise_on_failure=True
  )
  return _ParseResults(logs)


def Cleanup(_: _BenchmarkSpec) -> None:
  """PKB auto-deletes container_cluster resources on teardown."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ParseResults(logs: str) -> list[sample.Sample]:
  """Parse esrally race.json from PKB sentinel markers in Job pod logs.

  The esrally_job.yaml.j2 Job prints:
    === PKB_RACE_JSON_BEGIN ===
    { ... race.json ... }
    === PKB_RACE_JSON_END ===

  Args:
    logs: Full stdout from kubectl logs on the esrally Job pod.

  Returns:
    List of PKB samples parsed from op_metrics in race.json.
  """
  start = logs.find(_RACE_JSON_BEGIN)
  end = logs.find(_RACE_JSON_END)
  if start == -1 or end == -1:
    logging.warning(
        '[kubernetes_opensearch_esrally] race.json sentinel not found '
        'in Job logs -- no samples produced'
    )
    return []

  json_str = logs[start + len(_RACE_JSON_BEGIN):end].strip()
  race_data = None
  try:
    race_data = json.loads(json_str)
  except json.JSONDecodeError as e:
    logging.warning(
        '[kubernetes_opensearch_esrally] Failed to parse race.json: %s', e
    )
  if race_data is None:
    return []

  metadata: dict[str, Any] = {
      'rally_version': (
          race_data.get('benchmark-version')
          or race_data.get('rally-version', 'unknown')
      ),
      'rally_track': race_data.get('track', _ESRALLY_TRACK.value),
      'rally_challenge': race_data.get(
          'challenge', _ESRALLY_CHALLENGE.value
      ),
      'opensearch_version': _OPENSEARCH_VERSION.value,
      'swap_enabled': _SWAP_ENABLED.value,
  }

  samples: list[sample.Sample] = []
  for op in race_data.get('results', {}).get('op_metrics', []):
    task = op.get('task', 'unknown').replace('-', '_')
    tput = op.get('throughput', {}).get('mean', 0.0)
    tput_unit = op.get('throughput', {}).get('unit', 'docs/s')
    lat_mean = op.get('latency', {}).get('mean', 0.0)
    lat_unit = op.get('latency', {}).get('unit', 'ms')
    op_meta = dict(metadata, esrally_task=task)
    samples.append(
        sample.Sample(f'{task}_throughput', tput, tput_unit, op_meta)
    )
    samples.append(
        sample.Sample(
            f'{task}_latency_mean', lat_mean, lat_unit, op_meta
        )
    )
    p90 = op.get('latency', {}).get('90_0')
    if p90:
      samples.append(
          sample.Sample(f'{task}_latency_p90', p90, lat_unit, op_meta)
      )

  logging.info(
      '[kubernetes_opensearch_esrally] Parsed %d samples from race.json',
      len(samples),
  )
  return samples
