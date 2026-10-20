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
"""Dynamo + SGLang disaggregated serving on NVLink/NVL72-class GPU hardware.

Targets GKE a4x (GB200/NVL72) hardware, using the ComputeDomain custom
resource and Mooncake custom memory pool for NVLink-fabric KV transfer. See
dynamo_sglang_p2p_benchmark.py for the standard-networking variant.
"""

import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import dynamo_sglang_benchmark_helper
import yaml

# Note: dynamo_helm_values_override, dynamo_storage_bucket,
# dynamo_release_version, dynamo_hf_token, dynamo_workload_image, and
# dynamo_frontend_image are defined in dynamo_sglang_benchmark_helper, which
# both this benchmark and dynamo_sglang_p2p_benchmark.py import directly, so
# they're registered exactly once regardless of module load order.
_DYNAMO_SGLANG_FABRIC_WORKLOAD_PROFILE = flags.DEFINE_enum(
    'dynamo_sglang_fabric_workload_profile',
    'latency',
    ['latency', 'throughput'],
    'Which values file to use: latency uses dynamo_values_wo_deepep.yaml, '
    'throughput uses dynamo_values_deepep.yaml.',
)
_DYNAMO_SGLANG_FABRIC_MODEL_ID = flags.DEFINE_string(
    'dynamo_sglang_fabric_model_id',
    'deepseek-ai/DeepSeek-R1',
    'The Huggingface model ID to serve and benchmark against.',
)
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'dynamo_sglang_fabric'
BENCHMARK_CONFIG = """
dynamo_sglang_fabric:
  description: >
    Dynamo + SGLang disaggregated serving on NVLink/NVL72-class GPU hardware,
    defaulting to GKE a4x (GB200).
  flags:
    cluster_template_file: cluster/gke/dynamo_sglang_fabric.yaml.j2
  container_cluster:
    cloud: GCP
    type: gcluster
    vm_count: 1
    vm_spec:
      GCP:
        zone: us-central1-a
        machine_type: a4x-highgpu-4g
        gpu_type: nvidia-gb200
        gpu_count: 4
"""

_VALUES_FILE_BY_PROFILE = {
    'latency': 'dynamo/dynamo_values_wo_deepep.yaml',
    'throughput': 'dynamo/dynamo_values_deepep.yaml',
}


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the benchmark config."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if dynamo_sglang_benchmark_helper.DYNAMO_STORAGE_BUCKET.value:
    gcluster_vars = list(config.get('flags', {}).get('gcluster_vars', []))
    bucket_var = (
        'dynamo_storage_bucket='
        f'{dynamo_sglang_benchmark_helper.DYNAMO_STORAGE_BUCKET.value}'
    )
    if bucket_var not in gcluster_vars:
      gcluster_vars.append(bucket_var)
    config.setdefault('flags', {})['gcluster_vars'] = gcluster_vars
  return config


def _ExtractEnvVars(worker_config: dict[str, Any]) -> dict[str, Any]:
  env_vars = {}
  for env in worker_config.get('envs', []):
    if 'value' in env:
      env_vars[env['name']] = env['value']
  return env_vars


def _BuildValuesContext(cluster: Any) -> dict[str, Any]:
  """Reads the Dynamo values file and builds the CR-template context."""
  default_values = data.ResourcePath(
      _VALUES_FILE_BY_PROFILE[_DYNAMO_SGLANG_FABRIC_WORKLOAD_PROFILE.value]
  )
  values_file = (
      dynamo_sglang_benchmark_helper.DYNAMO_HELM_VALUES_OVERRIDE.value
      or default_values
  )

  with open(values_file, 'r') as f:
    values_yaml = yaml.safe_load(f)

  dynamo_config = values_yaml.get('dynamo', {})
  compute_domain = dynamo_config.get('computeDomain', {})
  decode_worker = dynamo_config.get('decodeWorker', {})
  prefill_worker = dynamo_config.get('prefillWorker', {})

  decode_env_vars = _ExtractEnvVars(decode_worker)
  prefill_env_vars = _ExtractEnvVars(prefill_worker) or decode_env_vars

  decode_replicas = decode_worker.get('nodeCount', 1)
  prefill_replicas = prefill_worker.get('replicas', decode_replicas)
  release_version = dynamo_sglang_benchmark_helper.DYNAMO_RELEASE_VERSION.value

  return {
      'deployment_name': dynamo_config.get(
          'deploymentName', 'dynamo-disagg1p1d'
      ),
      'namespace': dynamo_config.get('namespace', 'dynamo-cloud'),
      'compute_domain_name': compute_domain.get('name', 'a4x-domain'),
      'compute_domain_num_nodes': compute_domain.get('numNodes', 2),
      'compute_domain_channel': compute_domain.get(
          'resourceClaimTemplateName', 'a4x-channel'
      ),
      'frontend_replicas': dynamo_config.get('frontend', {}).get('replicas', 1),
      'worker_replicas': decode_replicas,
      'decode_replicas': decode_replicas,
      'prefill_replicas': prefill_replicas,
      'gpu_count': cluster.gpu_count or 4,
      'dynamo_model_id': _DYNAMO_SGLANG_FABRIC_MODEL_ID.value,
      'frontend_image': (
          dynamo_sglang_benchmark_helper.DYNAMO_FRONTEND_IMAGE.value
          or f'nvcr.io/nvidia/ai-dynamo/sglang-runtime:{release_version}'
      ),
      'workload_image': (
          dynamo_sglang_benchmark_helper.DYNAMO_WORKLOAD_IMAGE.value
          or f'nvcr.io/nvidia/ai-dynamo/sglang-runtime:{release_version}'
      ),
      'env_vars': decode_env_vars,
      'decode_env_vars': decode_env_vars,
      'prefill_env_vars': prefill_env_vars,
  }


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the software on the cluster (runs once)."""
  context = _BuildValuesContext(benchmark_spec.container_cluster)
  dynamo_sglang_benchmark_helper.Prepare(
      context, 'dynamo/sglang_fabric/dynamo_graph.yaml.j2'
  )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark workload."""
  context = _BuildValuesContext(benchmark_spec.container_cluster)
  logging.info('Triggering the Dynamo SGLang fabric benchmark workload...')
  return dynamo_sglang_benchmark_helper.Run(
      context,
      metadata_fn=lambda: {
          'profile': _DYNAMO_SGLANG_FABRIC_WORKLOAD_PROFILE.value,
          'model_id': _DYNAMO_SGLANG_FABRIC_MODEL_ID.value,
          'dynamo_release_version': (
              dynamo_sglang_benchmark_helper.DYNAMO_RELEASE_VERSION.value
          ),
      },
  )


def Cleanup(_: bm_spec.BenchmarkSpec) -> None:
  """No explicit cleanup: cluster teardown handles all created resources."""
  pass
