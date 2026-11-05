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
"""Shared Dynamo + SGLang orchestration for fabric and P2P benchmarks.

Holds the flags and Prepare/Run logic common to both benchmarks, so a single
module (loaded independently of benchmark import order) owns their
definitions instead of one benchmark silently depending on the other.
"""

import json
import logging
from typing import Any, Callable

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

DYNAMO_HELM_VALUES_OVERRIDE = flags.DEFINE_string(
    'dynamo_helm_values_override',
    None,
    'Optional absolute path to a custom values.yaml for the Dynamo deployment.',
)
DYNAMO_STORAGE_BUCKET = flags.DEFINE_string(
    'dynamo_storage_bucket',
    None,
    'The GCS bucket where models and data are stored.',
)
DYNAMO_RELEASE_VERSION = flags.DEFINE_string(
    'dynamo_release_version',
    '0.7.0',
    'The Dynamo Platform release version to deploy.',
)
DYNAMO_HF_TOKEN = flags.DEFINE_string(
    'dynamo_hf_token',
    None,
    'The Huggingface token for downloading gated models.',
)
DYNAMO_WORKLOAD_IMAGE = flags.DEFINE_string(
    'dynamo_workload_image',
    None,
    'Container image for the Dynamo serving workload (workers). Defaults to '
    'nvcr.io/nvidia/ai-dynamo/sglang-runtime:<dynamo_release_version>.',
)
DYNAMO_FRONTEND_IMAGE = flags.DEFINE_string(
    'dynamo_frontend_image',
    None,
    'Container image for the Dynamo frontend. Defaults to '
    'nvcr.io/nvidia/ai-dynamo/sglang-runtime:<dynamo_release_version>.',
)
FLAGS = flags.FLAGS

_VALID_POD_PLACEMENTS = ('cross_node', 'colocated', 'unconstrained')
_DYNAMO_OPERATOR_DEPLOYMENT = (
    'deployment/dynamo-platform-dynamo-operator-controller-manager'
)
_KUBE_RBAC_PROXY_IMAGE = 'quay.io/brancz/kube-rbac-proxy:v0.15.0'


def ResolveModelId(
    model_flag: flags.FlagHolder[str], values_yaml: dict[str, Any]
) -> str:
  """Resolves the model ID, preferring explicit flag overrides over YAML."""
  flag_obj = FLAGS[model_flag.name]
  if flag_obj.present or model_flag.value != flag_obj.default:
    return model_flag.value
  return str(
      (values_yaml.get('workload') or {}).get('model') or model_flag.value
  )


def ApplyValuesOverrides(
    context: dict[str, Any],
    values_yaml: dict[str, Any],
    default_pod_placement: str = 'cross_node',
) -> dict[str, Any]:
  """Populates worker TP, placement, args, volumes, and benchmark settings."""
  gpu_count = context.get('gpu_count') or 4
  dynamo_cfg = values_yaml.get('dynamo') or {}
  decode_cfg = dynamo_cfg.get('decodeWorker') or {}
  prefill_cfg = dynamo_cfg.get('prefillWorker') or {}
  frontend_cfg = dynamo_cfg.get('frontend') or {}
  volumes_cfg = values_yaml.get('volumes') or {}
  bench_cfg = values_yaml.get('benchmark') or {}

  if 'nodeCount' in decode_cfg:
    context['decode_replicas'] = int(decode_cfg['nodeCount'])
    context['worker_replicas'] = context['decode_replicas']
  elif 'replicas' in decode_cfg:
    context['decode_replicas'] = int(decode_cfg['replicas'])
    context['worker_replicas'] = context['decode_replicas']
  if 'replicas' in prefill_cfg:
    context['prefill_replicas'] = int(prefill_cfg['replicas'])
  elif 'nodeCount' in decode_cfg or 'replicas' in decode_cfg:
    context['prefill_replicas'] = context['decode_replicas']

  context['decode_tp'] = int(decode_cfg.get('tp', gpu_count))
  context['prefill_tp'] = int(prefill_cfg.get('tp', gpu_count))

  placement = dynamo_cfg.get('podPlacement', default_pod_placement)
  if placement not in _VALID_POD_PLACEMENTS:
    raise errors.Config.InvalidValue(
        f'Invalid dynamo.podPlacement {placement!r}; expected one of'
        f' {_VALID_POD_PLACEMENTS}.'
    )
  context['pod_placement'] = placement
  context['enable_compute_domain'] = bool(
      dynamo_cfg.get('enableComputeDomain', True)
  )
  context['disaggregation_transfer_backend'] = dynamo_cfg.get(
      'disaggregationTransferBackend'
  )
  context['decode_extra_args'] = [
      str(a) for a in decode_cfg.get('extraArgs') or []
  ]
  context['prefill_extra_args'] = [
      str(a) for a in prefill_cfg.get('extraArgs') or []
  ]
  context['router_mode'] = frontend_cfg.get('routerMode')

  use_gcs = volumes_cfg.get('useGcs', True)
  mount_path = (volumes_cfg.get('gcsMounts') or {}).get(
      'mountPath', '/data/model'
  )
  context['model_path_prefix'] = (
      str(mount_path) if use_gcs and mount_path is not None else ''
  )
  context['benchmark'] = {
      'num_prompts': int(bench_cfg.get('numPrompts', 10240)),
      'input_len': int(bench_cfg.get('inputLen', 8192)),
      'range_ratio': float(bench_cfg.get('rangeRatio', 0.8)),
      'output_len': int(bench_cfg.get('outputLen', 1024)),
      'max_concurrency': int(bench_cfg.get('maxConcurrency', 2048)),
      'request_rate': (
          float(bench_cfg['requestRate'])
          if bench_cfg.get('requestRate') is not None
          else None
      ),
  }
  return context


def ValidatePlacement(
    context: dict[str, Any], gpus_per_node: int | None
) -> None:
  """Checks that worker TP and colocated placement fit within per-node GPUs.

  Args:
    context: The CR-template context, after ApplyValuesOverrides.
    gpus_per_node: The GPU count per cluster node, or None if unknown, in which
      case the check is skipped.

  Raises:
    errors.Config.InvalidValue: If a worker's TP exceeds gpus_per_node, or if
      placement is colocated and total worker GPUs exceed gpus_per_node.
  """
  if not gpus_per_node:
    if context['pod_placement'] == 'colocated':
      logging.warning(
          'GPU count per node is unknown; skipping the colocated placement '
          'GPU fit check.'
      )
    return
  for worker_key, tp_key in (
      ('decodeWorker', 'decode_tp'),
      ('prefillWorker', 'prefill_tp'),
  ):
    if context[tp_key] > gpus_per_node:
      raise errors.Config.InvalidValue(
          f'{worker_key}.tp={context[tp_key]} exceeds'
          f' gpus_per_node={gpus_per_node}.'
      )
  if context['pod_placement'] != 'colocated':
    return
  decode_replicas = context.get('decode_replicas', 1)
  prefill_replicas = context.get('prefill_replicas', 1)
  decode_gpus = context['decode_tp'] * decode_replicas
  prefill_gpus = context['prefill_tp'] * prefill_replicas
  needed = decode_gpus + prefill_gpus
  if needed > gpus_per_node:
    raise errors.Config.InvalidValue(
        'dynamo.podPlacement=colocated needs '
        f'{decode_gpus} + {prefill_gpus} = {needed} GPUs on one node '
        f'(decode {context["decode_tp"]}x{decode_replicas}, '
        f'prefill {context["prefill_tp"]}x{prefill_replicas}), '
        f'but nodes have {gpus_per_node}. Lower decodeWorker.tp / '
        'prefillWorker.tp or use another dynamo.podPlacement.'
    )


def BuildRunMetadata(context: dict[str, Any], profile: str) -> dict[str, Any]:
  """Builds sample metadata from the resolved Dynamo context."""
  metadata: dict[str, Any] = {
      'profile': profile,
      'model_id': context['dynamo_model_id'],
      'dynamo_release_version': DYNAMO_RELEASE_VERSION.value,
      'decode_replicas': context['decode_replicas'],
      'prefill_replicas': context['prefill_replicas'],
      'decode_tp': context['decode_tp'],
      'prefill_tp': context['prefill_tp'],
      'pod_placement': context['pod_placement'],
  }
  if context.get('router_mode') is not None:
    metadata['router_mode'] = context['router_mode']
  if context.get('disaggregation_transfer_backend') is not None:
    metadata['disaggregation_transfer_backend'] = context[
        'disaggregation_transfer_backend'
    ]
  bench = context.get('benchmark') or {}
  for key in (
      'num_prompts',
      'input_len',
      'output_len',
      'max_concurrency',
      'request_rate',
  ):
    val = bench.get(key) if key in bench else context.get(key)
    if val is not None:
      metadata[key] = val
  return metadata


def Prepare(context: dict[str, Any], manifest_path: str) -> None:
  """Prepares the Dynamo platform and workload on the cluster (runs once).

  Args:
    context: The CR-template context, as built by a benchmark's
      _BuildValuesContext.
    manifest_path: Path (relative to a PKB data directory) to the
      DynamoGraphDeployment .yaml.j2 manifest template.
  """
  _, platform_err, platform_rc = kubectl.RunKubectlCommand(
      ['get', _DYNAMO_OPERATOR_DEPLOYMENT, '-n', 'default'],
      raise_on_failure=False,
  )
  if platform_rc != 0:
    if 'notfound' not in platform_err.lower():
      raise errors.VmUtil.IssueCommandError(
          f'Failed to query {_DYNAMO_OPERATOR_DEPLOYMENT}: {platform_err}'
      )
    helm.InstallCharts([
        helm.HelmChart(
            release_name='dynamo-crds',
            chart_ref=(
                'https://helm.ngc.nvidia.com/nvidia/ai-dynamo/charts/'
                f'dynamo-crds-{DYNAMO_RELEASE_VERSION.value}.tgz'
            ),
            namespace='default',
            wait=True,
            atomic=True,
        ),
        helm.HelmChart(
            release_name='dynamo-platform',
            chart_ref=(
                'https://helm.ngc.nvidia.com/nvidia/ai-dynamo/charts/'
                f'dynamo-platform-{DYNAMO_RELEASE_VERSION.value}.tgz'
            ),
            namespace='default',
            set_values={
                'grove.enabled': 'true',
                'kai-scheduler.enabled': 'true',
            },
            # --wait is skipped: the chart references a dead gcr.io repository
            # for one of its images, so `helm --wait` never returns.
            wait=False,
            atomic=False,
        ),
    ])

  kubectl.RunKubectlCommand([
      'set',
      'image',
      _DYNAMO_OPERATOR_DEPLOYMENT,
      f'kube-rbac-proxy={_KUBE_RBAC_PROXY_IMAGE}',
      '-n',
      'default',
  ])
  kubectl.RunKubectlCommand([
      'wait',
      '--for=condition=available',
      'deployment',
      '--all',
      '-n',
      'default',
      '--timeout=300s',
  ])

  namespace = context['namespace']

  _, stderr, retcode = kubectl.RunKubectlCommand(
      ['create', 'namespace', namespace], raise_on_failure=False
  )
  if retcode != 0 and 'AlreadyExists' not in stderr:
    raise errors.VmUtil.IssueCommandError(
        f'Failed to create namespace {namespace}: {stderr}'
    )

  if DYNAMO_HF_TOKEN.value:
    logging.info('Creating HuggingFace token secret...')
    kubectl.RunKubectlCommand([
        'delete',
        'secret',
        'hf-token-secret',
        '-n',
        namespace,
        '--ignore-not-found',
    ])
    kubectl.RunKubectlCommand([
        'create',
        'secret',
        'generic',
        'hf-token-secret',
        f'--from-literal=HF_TOKEN={DYNAMO_HF_TOKEN.value}',
        '-n',
        namespace,
    ])

  list(kubernetes_commands.ApplyManifest(manifest_path, **context))

  kubernetes_commands.WaitForPodsReady(namespace, timeout=900)


def _ParseBenchServingOutput(stdout: str) -> Any:
  lines = [line for line in stdout.strip().splitlines() if line.strip()]
  if not lines:
    raise ValueError('Empty JSON output from sglang.bench_serving')
  return json.loads(lines[-1])


def _InferMetricUnit(metric_name: str) -> str:
  """Infers a human-readable unit string from a sglang.bench_serving metric key."""
  if metric_name.endswith('_ms'):
    return 'ms'
  if metric_name == 'duration' or metric_name.endswith('_s'):
    return 's'
  if 'throughput' in metric_name:
    return 'requests/s' if 'request' in metric_name else 'tokens/s'
  return ''


def Run(
    context: dict[str, Any],
    metadata_fn: Callable[[], dict[str, Any]] = dict,
) -> list[sample.Sample]:
  """Runs the sglang.bench_serving workload and parses the results.

  Args:
    context: The CR-template context, as built by a benchmark's
      _BuildValuesContext.
    metadata_fn: Called with no arguments to build the metadata dict attached to
      each returned sample.

  Returns:
    A list of samples parsed from the bench_serving JSON output.
  """
  namespace = context['namespace']
  prefix = str(context.get('model_path_prefix', '/data/model') or '').rstrip(
      '/'
  )
  model_arg = (
      f"{prefix}/{context['dynamo_model_id']}"
      if prefix
      else context['dynamo_model_id']
  )
  bench = context.get('benchmark') or {}
  num_prompts = bench.get('num_prompts', 10240)
  input_len = bench.get('input_len', 8192)
  range_ratio = bench.get('range_ratio', 0.8)
  output_len = bench.get('output_len', 1024)
  max_concurrency = bench.get('max_concurrency', 2048)
  request_rate = bench.get('request_rate')

  cmd = [
      'python3',
      '-u',
      '-m',
      'sglang.bench_serving',
      '--backend',
      'sglang-oai-chat',
      '--base-url',
      'http://localhost:8000',
      '--model',
      model_arg,
      '--tokenizer',
      model_arg,
      '--dataset-name',
      'random',
      '--num-prompts',
      str(num_prompts),
      '--random-input-len',
      str(input_len),
      '--random-range-ratio',
      str(range_ratio),
      '--random-output-len',
      str(output_len),
      '--max-concurrency',
      str(max_concurrency),
  ]
  if request_rate is not None:
    cmd.extend(['--request-rate', str(request_rate)])
  cmd.extend(['--output-file', '/tmp/output.json'])

  kubernetes_commands.ExecAndParseResult(
      pod_selector='frontend',
      command=cmd,
      parse_fn=lambda _: None,
      namespace=namespace,
      timeout=7200,
  )

  results = kubernetes_commands.ExecAndParseResult(
      pod_selector='frontend',
      command=['cat', '/tmp/output.json'],
      parse_fn=_ParseBenchServingOutput,
      namespace=namespace,
  )

  samples = []
  for key, value in results.items():
    if isinstance(value, (int, float)):
      samples.append(
          sample.Sample(key, value, _InferMetricUnit(key), metadata_fn())
      )
  return samples
