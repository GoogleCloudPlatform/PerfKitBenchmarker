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
"""Benchmarks agent sandbox SandboxClaim provisioning latency.

Submits SandboxClaim custom resources at a target QPS against the agent
sandbox installed on the cluster (via benchmark_spec.agent_sandbox) and
reports claim-to-Ready latency percentiles, throughput, and peak concurrency.

The load generator runs INSIDE a pod on the cluster (Job: agent-sandbox-load-runner)
so the kubernetes Python client is not a PKB core dependency.
"""

import inspect
import json
import os
import tempfile
from typing import Any

from absl import flags
from absl import logging
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker.linux_benchmarks import agent_sandbox_loadgen
from perfkitbenchmarker.linux_benchmarks import agent_sandbox_metrics
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox

FLAGS = flags.FLAGS

_QPS = flags.DEFINE_float(
    'agent_sandbox_qps', 10.0,
    'Target SandboxClaim submission rate (claims per second).')
_DURATION = flags.DEFINE_float(
    'agent_sandbox_duration', 60.0,
    'Submission window in seconds. Combined with qps to derive total.')
_TOTAL = flags.DEFINE_integer(
    'agent_sandbox_total', None,
    'Total claims to submit. If set, overrides duration derivation.')
_MAX_CONCURRENT = flags.DEFINE_integer(
    'agent_sandbox_max_concurrent', 200, 'Maximum in-flight claims.')
_READY_TIMEOUT = flags.DEFINE_integer(
    'agent_sandbox_ready_timeout', 180, 'Per-claim ready timeout in seconds.')
_WORKLOAD_DURATION = flags.DEFINE_integer(
    'agent_sandbox_workload_duration', 0,
    'Seconds to run a CPU busy-loop inside each sandbox after it becomes '
    'Ready, before releasing it. 0 (default) releases immediately (pure '
    'provisioning benchmark). >0 holds the sandbox so peak_concurrency '
    'reflects simultaneously-alive sandboxes and exec/lifecycle metrics emit.')

BENCHMARK_NAME = 'agent_sandbox'
BENCHMARK_CONFIG = """
agent_sandbox:
  description: >
    Submit SandboxClaims at a target QPS and measure provisioning latency on
    the agent-sandbox stack.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: c4-standard-4
        zone: us-central1-a
      AWS:
        machine_type: m8i.xlarge
        zone: us-east-1a
    nodepools:
      sandbox:
        vm_count: 4
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
          AWS:
            machine_type: m8i.4xlarge
            zone: us-east-1a
        node_taints:
          - sandbox.gke.io/runtime=runsc:NoSchedule
  agent_sandbox:
    type: Kubernetes
"""

# K8s object names for the in-pod load runner.
_LOAD_RUNNER_CONFIGMAP = 'agent-sandbox-load-runner-script'
_LOAD_RUNNER_JOB = 'agent-sandbox-load-runner'
_LOAD_RUNNER_RBAC_MANIFEST = 'agent_sandbox/load_runner_rbac.yaml.j2'
_LOAD_RUNNER_JOB_MANIFEST = 'agent_sandbox/load_runner_job.yaml.j2'
_RESULTS_SENTINEL = '---RESULTS---'


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads the benchmark config and merges user overrides."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['container_cluster']['cloud'] = FLAGS.cloud
  return config


def _create_loadgen_configmap(namespace: str) -> None:
  """Creates or updates the load-runner-script ConfigMap from the loadgen source.

  Uses --dry-run=client -o yaml to render the ConfigMap (idempotent on
  re-runs), writes it to a temp file, and applies it.
  """
  script_path = inspect.getsourcefile(agent_sandbox_loadgen)
  yaml_out, _, _ = kubectl.RunKubectlCommand([
      'create',
      'configmap',
      _LOAD_RUNNER_CONFIGMAP,
      f'--from-file=load_runner.py={script_path}',
      '--namespace', namespace,
      '--dry-run=client',
      '-o', 'yaml',
  ])
  tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
  with tmpfile as tmp:
    tmp.write(yaml_out)
    tmp_path = tmp.name
  try:
    kubectl.RunKubectlCommand(['apply', '-f', tmp_path])
  finally:
    os.unlink(tmp_path)


def Prepare(benchmark_spec: Any) -> None:
  """Installs the controller, sandbox template, warm pool, and load runner RBAC.

  CRDs, RBAC, and gVisor are installed during provision. The controller stack
  is installed here so it can be re-applied against an existing cluster with
  `--run_stage=prepare` to iterate on controller settings without recreating
  the cluster.
  """
  sandbox = benchmark_spec.agent_sandbox
  if sandbox is None:
    raise errors.Benchmarks.PrepareException(
        'agent_sandbox must be configured for this benchmark')
  if stages.PROVISION not in FLAGS.run_stage:
    sandbox.RefreshSpecFromFlags()
  sandbox.InstallWorkload()

  namespace = sandbox.spec.namespace
  _create_loadgen_configmap(namespace)
  kubernetes_commands.ApplyManifest(
      _LOAD_RUNNER_RBAC_MANIFEST,
      namespace=namespace,
  )


def Run(benchmark_spec: Any) -> list[sample.Sample]:
  """Runs the load generator Job inside the cluster and returns latency samples."""
  sandbox = benchmark_spec.agent_sandbox
  spec = sandbox.spec
  namespace = spec.namespace

  shape = agent_sandbox_loadgen.resolve_run_shape(
      qps=_QPS.value,
      duration=_DURATION.value if _TOTAL.value is None else None,
      total=_TOTAL.value)

  # Delete any previous job so kubectl apply works cleanly.
  kubectl.RunKubectlCommand(
      ['delete', 'job', _LOAD_RUNNER_JOB, '--namespace', namespace,
       '--ignore-not-found'],
  )

  # Render and apply the Job manifest.
  kubernetes_commands.ApplyManifest(
      _LOAD_RUNNER_JOB_MANIFEST,
      namespace=namespace,
      template_name=k8s_agent_sandbox.SANDBOX_NAME,
      qps=shape.qps,
      total=shape.total,
      duration=shape.duration,
      max_concurrent=_MAX_CONCURRENT.value,
      workload_duration=_WORKLOAD_DURATION.value,
      ready_timeout=_READY_TIMEOUT.value,
  )

  # Derive a generous wait timeout: submission window + per-claim timeout +
  # workload duration + margin.
  wait_timeout = int(
      shape.duration + _READY_TIMEOUT.value + _WORKLOAD_DURATION.value + 120
  )

  # Wait for the Job to complete OR fail (whichever comes first).
  condition = kubernetes_commands.WaitForResourceForMultiConditions(
      f'job/{_LOAD_RUNNER_JOB}',
      conditions=['condition=Complete', 'condition=Failed'],
      namespace=namespace,
      timeout=wait_timeout,
  )

  # Fetch logs regardless; we need them for results or the error message.
  pod_out, _, _ = kubectl.RunKubectlCommand([
      'get', 'pods',
      '--namespace', namespace,
      '-l', f'job-name={_LOAD_RUNNER_JOB}',
      '-o', 'jsonpath={.items[0].metadata.name}',
  ])
  pod_name = pod_out.strip()

  logs_out, _, _ = kubectl.RunKubectlCommand(
      ['logs', pod_name, '--namespace', namespace],
  )

  if condition == 'condition=Failed':
    raise errors.Benchmarks.RunError(
        f'Load runner Job {_LOAD_RUNNER_JOB!r} failed.\n'
        f'Pod logs:\n{logs_out}'
    )

  # Split on the LAST ---RESULTS--- sentinel.
  parts = logs_out.split(_RESULTS_SENTINEL)
  if len(parts) < 2:
    raise errors.Benchmarks.RunError(
        f'No {_RESULTS_SENTINEL!r} sentinel found in pod logs.\n'
        f'Pod logs:\n{logs_out}'
    )
  jsonl_section = parts[-1].strip()

  records = []
  peak_concurrency = None
  for line in jsonl_section.splitlines():
    line = line.strip()
    if not line:
      continue
    obj = json.loads(line)
    if 'summary' in obj:
      peak_concurrency = obj['summary']['peak_concurrency']
    else:
      records.append(agent_sandbox_loadgen.ClaimRecord(**obj))

  if peak_concurrency is None:
    logging.warning('No summary line found; defaulting peak_concurrency to 0')
    peak_concurrency = 0

  metadata = {
      'target_qps': shape.qps,
      'duration': shape.duration,
      'total_claims': shape.total,
      'warmpool_replicas': spec.sandbox_warmpool.replicas,
      'runtime_class': spec.sandbox_template.runtime_class,
  }
  metadata.update(benchmark_spec.container_cluster.GetResourceMetadata())
  return agent_sandbox_metrics.build_samples(
      records, peak_concurrency, metadata)


def Cleanup(benchmark_spec: Any) -> None:
  """No-op: cluster teardown reclaims the agent sandbox stack."""
  del benchmark_spec
