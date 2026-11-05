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
r"""Serves GLM-5.3 with SGLang on 8x NVIDIA B200 GKE nodes.

Creates a GKE cluster with a small CPU default node pool and a node pool of
a4-highgpu-8g (8x B200) nodes, deploys one SGLang replica of GLM-5.3 (FP8) per
A4 node and measures it with sglang.bench_serving.

The ~750 GB checkpoint must be preprovisioned in GCS; pass its location with
--glm_sglang_model_uri, e.g. gs://<bucket>/kubernetes_glm_sglang/GLM-5.3.
"""

import json
import math
from typing import Any

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubernetes_commands


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kubernetes_glm_sglang'
BENCHMARK_CONFIG = """
kubernetes_glm_sglang:
  description: >
    Serves GLM-5.3 (FP8) with SGLang, one replica per 8x NVIDIA B200 node
    (GKE a4-highgpu-8g), and measures it with sglang.bench_serving.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-standard-8
    nodepools:
      gpupool:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: a4-highgpu-8g
            gpu_type: b200
            gpu_count: 8
            # a4-highgpu-8g bundles 32 Local SSDs; with
            # --gke_use_lssd_as_ephemeral_storage they back /var/lib/kubelet.
            num_local_ssds: 32
            boot_disk_type: hyperdisk-balanced
            boot_disk_size: 200
  flags:
    gke_gpu_driver_version: latest
"""

GPU_NODEPOOL = 'gpupool'
APP_NAME = 'glm-sglang'
SERVICE_PORT = 30000
MANIFEST_TEMPLATE = 'container/kubernetes_glm_sglang/glm_sglang.yaml.j2'
_SCRIPTS_DIR = 'container/kubernetes_glm_sglang'
# hostPath for the weight cache; /var/lib/kubelet is on local NVMe.
_NODE_CACHE_DIR = '/var/lib/kubelet/pkb-models'
# Image that copies the weights from the bucket, by cloud.
_FETCH_IMAGES = {
    'GCP': 'google/cloud-sdk:slim',
}
_BENCH_TIMEOUT = 4 * 3600
_SERVER_TIMEOUT = 2 * 3600  # seconds
# Fields that sglang.bench_serving copies from its command line into its
# results; the sample metadata records the ones that vary.
_BENCH_ARG_FIELDS = frozenset({
    'max_concurrency',
    'random_input_len',
    'random_output_len',
    'random_range_ratio',
    'request_rate',
    'sharegpt_output_len',
})

# Not using the preprovisioned data bucket, so that the model (~750 GB) can be
# placed in a zonal bucket in the same zone as the GPUs to avoid network costs.
_MODEL_URI = flags.DEFINE_string(
    'glm_sglang_model_uri',
    '',
    'Required. Download URI of the preprovisioned checkpoint, e.g.'
    ' gs://<bucket>/kubernetes_glm_sglang/GLM-5.3.',
)
_MODEL_ID = flags.DEFINE_string(
    'glm_sglang_model_id',
    'zai-org/GLM-5.3',
    'Name SGLang serves the model under. Its last path component names the'
    ' local copy on each node, e.g. /models/GLM-5.3.',
)
_SGLANG_IMAGE = flags.DEFINE_string(
    'glm_sglang_image',
    'lmsysorg/sglang:v0.5.18',
    'SGLang container image for the server and the benchmark client.',
)
_SERVING_PROFILE = flags.DEFINE_enum(
    'glm_sglang_serving_profile',
    'throughput',
    ['throughput', 'latency'],
    'SGLang launch flags: throughput (TP8 + DP-attention + DeepEP + MTP) or'
    ' latency (TP8 + a longer MTP draft).',
)
_HICACHE_SIZE_GB = flags.DEFINE_integer(
    'glm_sglang_hicache_size_gb',
    0,
    'Host DRAM in GB per GPU rank for SGLang HiCache; 0 disables it.',
)
_BENCH_NUM_PROMPTS = flags.DEFINE_integer(
    'glm_sglang_bench_num_prompts',
    256,
    'Number of prompts per sglang.bench_serving run.',
)
_BENCH_INPUT_LEN = flags.DEFINE_integer(
    'glm_sglang_bench_input_len',
    8192,
    'Input tokens per prompt of the random workload.',
)
_BENCH_OUTPUT_LEN = flags.DEFINE_integer(
    'glm_sglang_bench_output_len',
    1024,
    'Output tokens per prompt of the random workload.',
)
flag_util.DEFINE_integerlist(
    'glm_sglang_bench_max_concurrency',
    flag_util.IntegerList([64]),
    'Maximum concurrent requests; sglang.bench_serving runs once per value.',
)


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the benchmark config."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config: Any) -> None:
  """Fails before cluster creation if --glm_sglang_model_uri is unset.

  Args:
    benchmark_config: The decoded BenchmarkConfigSpec (unused).

  Raises:
    errors.Config.MissingOption: If --glm_sglang_model_uri is not set.
  """
  del benchmark_config
  if not _MODEL_URI.value:
    raise errors.Config.MissingOption(
        f'{BENCHMARK_NAME} needs --glm_sglang_model_uri, the bucket prefix of'
        f' a preprovisioned copy of the checkpoint.'
    )


def _ModelName() -> str:
  return _MODEL_ID.value.rstrip('/').split('/')[-1]


def _ModelDir() -> str:
  return f'/models/{_ModelName()}'


def _ReadScript(name: str) -> str:
  with open(data.ResourcePath(f'{_SCRIPTS_DIR}/{name}')) as script:
    return script.read()


def _ManifestContext(replicas: int, gpu_count: int) -> dict[str, Any]:
  """Returns the jinja context for MANIFEST_TEMPLATE."""
  return {
      'name': APP_NAME,
      'replicas': replicas,
      'nodepool': GPU_NODEPOOL,
      'sglang_image': _SGLANG_IMAGE.value,
      'fetch_image': _FETCH_IMAGES[FLAGS.cloud],
      'model_uri': _MODEL_URI.value,
      'model_dir': _ModelDir(),
      'served_model_name': _MODEL_ID.value,
      'service_port': SERVICE_PORT,
      'gpu_count': gpu_count,
      'serving_profile': _SERVING_PROFILE.value,
      'hicache_size_gb': _HICACHE_SIZE_GB.value,
      'node_cache_dir': _NODE_CACHE_DIR,
      'progress_deadline_seconds': _SERVER_TIMEOUT,
      'cpu_request': 100,
      'memory_request': '1000Gi',
      'shm_size': '64Gi',
      'fetch_weights_sh': _ReadScript('fetch_weights.sh'),
      'serve_sh': _ReadScript('serve.sh'),
  }


def _Replicas(benchmark_spec: bm_spec.BenchmarkSpec) -> int:
  return benchmark_spec.container_cluster.nodepools[GPU_NODEPOOL].num_nodes


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Deploys GLM-5.3 on SGLang and waits until every replica is ready."""
  cluster = benchmark_spec.container_cluster
  list(
      kubernetes_commands.ApplyManifest(
          MANIFEST_TEMPLATE,
          **_ManifestContext(
              _Replicas(benchmark_spec), cluster.gpu_count or 8
          ),
      )
  )
  kubernetes_commands.WaitForRollout(
      f'deployment/{APP_NAME}', timeout=_SERVER_TIMEOUT
  )


def _BenchCommand(max_concurrency: int, output_file: str) -> list[str]:
  return [
      'python3',
      '-u',
      '-m',
      'sglang.bench_serving',
      '--backend=sglang',
      # Through the Service, so every replica gets traffic.
      f'--base-url=http://{APP_NAME}:{SERVICE_PORT}',
      f'--model={_MODEL_ID.value}',
      f'--tokenizer={_ModelDir()}',
      # Random token ids; unlike "random", this needs no ShareGPT download
      # from Hugging Face.
      '--dataset-name=random-ids',
      f'--random-input-len={_BENCH_INPUT_LEN.value}',
      f'--random-output-len={_BENCH_OUTPUT_LEN.value}',
      '--random-range-ratio=1.0',
      f'--num-prompts={_BENCH_NUM_PROMPTS.value}',
      f'--max-concurrency={max_concurrency}',
      '--warmup-requests=4',
      f'--output-file={output_file}',
  ]


def ParseBenchServingOutput(stdout: str) -> dict[str, Any]:
  """Returns the last JSON line written by sglang.bench_serving."""
  lines = [line for line in stdout.strip().splitlines() if line.strip()]
  if not lines:
    raise errors.Benchmarks.RunError('Empty sglang.bench_serving output.')
  return json.loads(lines[-1])


def _MetricUnit(metric: str) -> str:
  if metric.endswith('_ms'):
    return 'ms'
  if metric == 'duration':
    return 's'
  # Rates, e.g. output_throughput or max_output_tokens_per_s.
  if 'throughput' in metric or metric.endswith('_per_s'):
    return 'requests/s' if 'request' in metric else 'tokens/s'
  return ''


def MakeSamples(
    results: dict[str, Any], metadata: dict[str, Any], num_gpus: int
) -> list[sample.Sample]:
  """Converts the finite numeric bench_serving metrics into samples."""
  samples = []
  for metric, value in results.items():
    # Infinity and NaN are not valid JSON for the sample publishers.
    if (
        metric in _BENCH_ARG_FIELDS
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
      continue
    samples.append(sample.Sample(metric, value, _MetricUnit(metric), metadata))
    if metric == 'output_throughput' and num_gpus:
      samples.append(
          sample.Sample(
              'output_throughput_per_gpu',
              value / num_gpus,
              'tokens/s',
              metadata,
          )
      )
  return samples


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs sglang.bench_serving once per max concurrency value."""
  cluster = benchmark_spec.container_cluster
  replicas = _Replicas(benchmark_spec)
  gpus_per_replica = cluster.gpu_count or 8
  base_metadata = {
      'model': _MODEL_ID.value,
      'model_uri': _MODEL_URI.value,
      'sglang_image': _SGLANG_IMAGE.value,
      'serving_profile': _SERVING_PROFILE.value,
      'replicas': replicas,
      'gpus_per_replica': gpus_per_replica,
      'gpu_machine_type': cluster.nodepools[GPU_NODEPOOL].machine_type,
      'hicache_size_gb': _HICACHE_SIZE_GB.value,
      'bench_dataset': 'random-ids',
      'bench_num_prompts': _BENCH_NUM_PROMPTS.value,
      'bench_input_len': _BENCH_INPUT_LEN.value,
      'bench_output_len': _BENCH_OUTPUT_LEN.value,
  }
  samples = []
  for max_concurrency in FLAGS.glm_sglang_bench_max_concurrency:
    output_file = f'/tmp/bench_serving_{max_concurrency}.jsonl'
    kubernetes_commands.ExecAndParseResult(
        pod_selector=APP_NAME,
        command=_BenchCommand(max_concurrency, output_file),
        parse_fn=lambda _: None,
        timeout=_BENCH_TIMEOUT,
    )
    results = kubernetes_commands.ExecAndParseResult(
        pod_selector=APP_NAME,
        command=['cat', output_file],
        parse_fn=ParseBenchServingOutput,
    )
    metadata = dict(base_metadata, bench_max_concurrency=max_concurrency)
    samples += MakeSamples(results, metadata, replicas * gpus_per_replica)
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Nothing to clean up: PKB deletes the cluster with the deployment."""
  del benchmark_spec
