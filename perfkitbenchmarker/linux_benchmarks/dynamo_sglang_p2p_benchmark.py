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
"""Dynamo + SGLang disaggregated serving on standard-networking GPU hardware.

Targets GKE g4 (RTX PRO 6000) hardware, which uses peer-to-peer NCCL over
standard TCP/IP, not the NVLink/NVL72 fabric that dynamo_sglang_fabric
targets. See dynamo_sglang_fabric_benchmark.py for the NVLink-class variant.
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
# both this benchmark and dynamo_sglang_fabric_benchmark.py import directly,
# so they're registered exactly once regardless of module load order.
_DYNAMO_SGLANG_P2P_MODEL_ID = flags.DEFINE_string(
    'dynamo_sglang_p2p_model_id',
    'Qwen/Qwen2.5-32B-Instruct',
    'The Huggingface model ID to serve and benchmark against.',
)
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'dynamo_sglang_p2p'
BENCHMARK_CONFIG = """
dynamo_sglang_p2p:
  description: >
    Dynamo + SGLang disaggregated serving on standard-networking (no NVLink,
    peer-to-peer NCCL) GPU hardware, defaulting to GKE g4 (RTX PRO 6000).
  flags:
    cluster_template_file: cluster/gke/dynamo_sglang_p2p.yaml.j2
  container_cluster:
    cloud: GCP
    type: gcluster
    vm_count: 1
    vm_spec:
      GCP:
        zone: us-central1-a
        machine_type: g4-standard-96
        gpu_type: nvidia-rtx-pro-6000
        gpu_count: 4
"""


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
  default_values = data.ResourcePath('dynamo/dynamo_values_g4.yaml')
  values_file = (
      dynamo_sglang_benchmark_helper.DYNAMO_HELM_VALUES_OVERRIDE.value
      or default_values
  )

  with open(values_file, 'r') as f:
    values_yaml = yaml.safe_load(f)

  dynamo_config = values_yaml.get('dynamo', {})
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
      'frontend_replicas': dynamo_config.get('frontend', {}).get('replicas', 1),
      'worker_replicas': decode_replicas,
      'decode_replicas': decode_replicas,
      'prefill_replicas': prefill_replicas,
      'gpu_count': cluster.gpu_count or 4,
      'dynamo_model_id': _DYNAMO_SGLANG_P2P_MODEL_ID.value,
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
      context, 'dynamo/sglang_p2p/dynamo_graph.yaml.j2'
  )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark workload."""
  context = _BuildValuesContext(benchmark_spec.container_cluster)
  logging.info('Triggering the Dynamo SGLang p2p benchmark workload...')
  return dynamo_sglang_benchmark_helper.Run(
      context,
      metadata_fn=lambda: {
          'model_id': _DYNAMO_SGLANG_P2P_MODEL_ID.value,
          'dynamo_release_version': (
              dynamo_sglang_benchmark_helper.DYNAMO_RELEASE_VERSION.value
          ),
      },
  )


def Cleanup(_: bm_spec.BenchmarkSpec) -> None:
  """No explicit cleanup: cluster teardown handles all created resources."""
  pass
