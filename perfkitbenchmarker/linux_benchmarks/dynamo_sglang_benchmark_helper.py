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


def Prepare(context: dict[str, Any], manifest_path: str) -> None:
  """Prepares the Dynamo platform and workload on the cluster (runs once).

  Args:
    context: The CR-template context, as built by a benchmark's
      _BuildValuesContext.
    manifest_path: Path (relative to a PKB data directory) to the
      DynamoGraphDeployment .yaml.j2 manifest template.
  """
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
          set_values={'grove.enabled': 'true', 'kai-scheduler.enabled': 'true'},
          # --wait is skipped: the chart references a dead gcr.io repository
          # for one of its images, so `helm --wait` never returns.
          wait=False,
          atomic=False,
      ),
  ])

  kubectl.RunKubectlCommand([
      'set',
      'image',
      'deployment/dynamo-platform-dynamo-operator-controller-manager',
      'kube-rbac-proxy=quay.io/brancz/kube-rbac-proxy:v0.15.0',
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

  kubernetes_commands.ExecAndParseResult(
      pod_selector='frontend',
      command=[
          'python3',
          '-u',
          '-m',
          'sglang.bench_serving',
          '--backend',
          'sglang-oai-chat',
          '--base-url',
          'http://localhost:8000',
          '--model',
          f"/data/model/{context['dynamo_model_id']}",
          '--tokenizer',
          f"/data/model/{context['dynamo_model_id']}",
          '--dataset-name',
          'random',
          '--num-prompts',
          '10240',
          '--random-input-len',
          '8192',
          '--random-range-ratio',
          '0.8',
          '--random-output-len',
          '1024',
          '--max-concurrency',
          '2048',
          '--output-file',
          '/tmp/output.json',
      ],
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
