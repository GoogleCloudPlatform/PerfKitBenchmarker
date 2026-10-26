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

The load generator runs INSIDE a pod on the cluster (Job:
agent-sandbox-load-runner)
so the kubernetes Python client is not a PKB core dependency.
"""

import json
from typing import Any, cast

from absl import flags
from absl import logging
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.kubernetes import agent_sandbox_metrics
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
from perfkitbenchmarker.resources.kubernetes import k8s_agent_sandbox

FLAGS = flags.FLAGS

_DEFAULT_MAX_CONCURRENT = 200
_DEFAULT_READY_TIMEOUT = 180


_TARGET_QPS = flags.DEFINE_float(
    'agent_sandbox_benchmark_target_qps',
    10.0,
    'Target SandboxClaim submission rate (claims per second).',
)
_TARGET_DURATION = flags.DEFINE_float(
    'agent_sandbox_benchmark_target_duration',
    60.0,
    'Target benchmark duration in seconds. Combined with qps to derive total.',
)
_TARGET_SANDBOXES_CREATED = flags.DEFINE_integer(
    'agent_sandbox_benchmark_target_sandboxes_created',
    None,
    'Target benchmark sandbox creations. If set, overrides duration'
    ' derivation.',
)
_WORKLOAD_DURATION = flags.DEFINE_integer(
    'agent_sandbox_workload_duration',
    20,
    'Seconds to run a CPU busy-loop inside each sandbox after it becomes Ready,'
    ' before releasing it. 20 (default) holds the sandbox so peak_concurrency'
    ' reflects simultaneously-alive sandboxes and exec/lifecycle metrics emit.',
)
_MAX_CONCURRENT = flags.DEFINE_integer(
    'agent_sandbox_max_concurrent',
    None,
    'Maximum concurrent in-flight SandboxClaims in the load runner. If unset,'
    ' dynamically scales above 200 when target QPS * workload_duration requires'
    ' higher concurrency.',
    lower_bound=1,
)
_READY_TIMEOUT = flags.DEFINE_integer(
    'agent_sandbox_ready_timeout',
    _DEFAULT_READY_TIMEOUT,
    'Per-claim Ready timeout in seconds for the load runner.',
    lower_bound=10,
)
_LOAD_RUNNER_PARALLELISM = flags.DEFINE_integer(
    'agent_sandbox_load_runner_parallelism',
    1,
    'Number of parallel load-runner Job pods to shard target QPS and total'
    ' claims across.',
    lower_bound=1,
)

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
      Azure:
        machine_type: Standard_D4s_v5
        zone: eastus2
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
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus2
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
  """Loads the benchmark config and merges user overrides.

  Args:
    user_config: User-supplied benchmark configuration overrides.

  Returns:
    Merged benchmark configuration dictionary.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['container_cluster']['cloud'] = FLAGS.cloud
  return config


def _CreateLoadgenConfigMap(namespace: str) -> None:
  """Creates or updates the load-runner-script ConfigMap from the loadgen source.

  Args:
    namespace: Kubernetes namespace in which to apply the ConfigMap.
  """
  script_path = data.ResourcePath('agent_sandbox/agent_sandbox_loadgen.py')
  with open(script_path) as f:
    script_content = f.read()
  configmap = {
      'apiVersion': 'v1',
      'kind': 'ConfigMap',
      'metadata': {
          'name': _LOAD_RUNNER_CONFIGMAP,
          'namespace': namespace,
      },
      'data': {
          'load_runner.py': script_content,
      },
  }
  kubernetes_commands.ApplyYaml([configmap], should_log_file=False)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Installs the controller, sandbox template, warm pool, and load runner RBAC.

  CRDs, RBAC, and gVisor are installed during provision. The controller stack
  is installed here so it can be re-applied against an existing cluster with
  `--run_stage=prepare` to iterate on controller settings without recreating
  the cluster.

  Args:
    benchmark_spec: The benchmark specification.

  Raises:
    errors.Benchmarks.PrepareException: If agent_sandbox is not configured.
  """
  sandbox = cast(
      k8s_agent_sandbox.K8sAgentSandbox | None, benchmark_spec.agent_sandbox
  )
  if sandbox is None:
    raise errors.Benchmarks.PrepareException(
        'agent_sandbox must be configured for this benchmark'
    )

  sandbox.InstallWorkload()

  namespace = sandbox.spec.namespace
  _CreateLoadgenConfigMap(namespace)
  kubernetes_commands.ApplyManifest(
      _LOAD_RUNNER_RBAC_MANIFEST,
      namespace=namespace,
  )


def _DeployLoadRunnerJob(
    namespace: str,
    shape: agent_sandbox_metrics.RunShape,
    sandbox: k8s_agent_sandbox.K8sAgentSandbox | None = None,
) -> None:
  """Deletes any previous Job and applies the configured load runner Job."""
  kubectl.RunKubectlCommand(
      [
          'delete',
          'job',
          _LOAD_RUNNER_JOB,
          '--namespace',
          namespace,
          '--ignore-not-found',
      ],
  )

  yaml_docs = kubernetes_commands.ConvertManifestToYamlDicts(
      _LOAD_RUNNER_JOB_MANIFEST,
      namespace=namespace,
  )

  parallelism = _LOAD_RUNNER_PARALLELISM.value or 1
  if parallelism > 1:
    yaml_docs[0]['spec']['parallelism'] = parallelism
    yaml_docs[0]['spec']['completions'] = parallelism

  pod_qps = (shape.qps / parallelism) if shape.qps is not None else None
  pod_total = (
      max(1, int(round(shape.total / parallelism)))
      if shape.total is not None
      else None
  )

  estimated_concurrency = int(
      (pod_qps or 10.0) * max(1, _WORKLOAD_DURATION.value) * 1.5
  )
  if _MAX_CONCURRENT.value:
    effective_max_concurrent = max(1, _MAX_CONCURRENT.value // parallelism)
  else:
    effective_max_concurrent = max(
        _DEFAULT_MAX_CONCURRENT, estimated_concurrency
    )
  ready_timeout = _READY_TIMEOUT.value or _DEFAULT_READY_TIMEOUT

  load_runner_args: list[str] = [
      '--namespace',
      namespace,
      '--max-concurrent',
      str(effective_max_concurrent),
      '--workload-duration',
      str(_WORKLOAD_DURATION.value),
      '--ready-timeout',
      str(ready_timeout),
      '--output',
      '/tmp/results/run.jsonl',
  ]
  if parallelism > 1:
    load_runner_args.extend(['--claim-prefix', '"claim-${HOSTNAME:-0}"'])
  if sandbox is not None:
    get_pools = getattr(sandbox, 'GetWarmpoolNames', None)
    if callable(get_pools):
      pool_names = get_pools()
      if isinstance(pool_names, (list, tuple)) and pool_names:
        load_runner_args.extend(['--template-name', ','.join(pool_names)])
  if pod_qps is not None:
    load_runner_args.extend(['--qps', str(pod_qps)])
  if pod_total is not None:
    load_runner_args.extend(['--total', str(pod_total)])
  if shape.duration is not None:
    load_runner_args.extend(['--duration', str(shape.duration)])

  script = (
      'set -euo pipefail\n'
      'mkdir -p /tmp/results\n'
      'pip install --no-cache-dir kubernetes\n'
      f'python /opt/load-runner/load_runner.py {" ".join(load_runner_args)}\n'
      "echo '---RESULTS---'\n"
      'cat /tmp/results/run.jsonl\n'
  )
  container = yaml_docs[0]['spec']['template']['spec']['containers'][0]
  container['args'] = [script]
  if effective_max_concurrent > 500:
    container['resources'] = {
        'requests': {'cpu': '1', 'memory': '2Gi'},
        'limits': {'cpu': '8', 'memory': '8Gi'},
    }

  kubernetes_commands.ApplyYaml(yaml_docs)


def _WaitForJobAndFetchLogs(
    benchmark_spec: bm_spec.BenchmarkSpec,
    namespace: str,
    shape: agent_sandbox_metrics.RunShape,
) -> str:
  """Waits for the load runner Job to finish and returns its pod logs."""
  # Derive a generous wait timeout: submission window + per-claim timeout +
  # workload duration + margin.
  ready_timeout = _READY_TIMEOUT.value or _DEFAULT_READY_TIMEOUT
  wait_timeout = int(
      shape.duration + ready_timeout + _WORKLOAD_DURATION.value + 120
  )

  # Wait for the Job to complete OR fail (whichever comes first).
  condition = kubernetes_commands.WaitForResourceForMultiConditions(
      f'job/{_LOAD_RUNNER_JOB}',
      conditions=['condition=Complete', 'condition=Failed'],
      namespace=namespace,
      timeout=wait_timeout,
  )

  pod_out, _, _ = kubectl.RunKubectlCommand(
      [
          'get',
          'pods',
          '--namespace',
          namespace,
          '-l',
          f'job-name={_LOAD_RUNNER_JOB}',
          '-o',
          'jsonpath={.items[*].metadata.name}',
      ],
      raise_on_failure=False,
  )
  pod_names = pod_out.split()
  logs_chunks = []
  for pod_name in pod_names:
    pod_log, _, _ = kubectl.RunKubectlCommand(
        ['logs', pod_name, '--namespace', namespace],
        raise_on_failure=condition != 'condition=Failed',
    )
    if pod_log:
      logs_chunks.append(pod_log)
  logs_out = '\n'.join(logs_chunks)

  if condition == 'condition=Failed':
    cluster = benchmark_spec.container_cluster
    assert cluster is not None
    event_poller: Any = getattr(cluster, 'event_poller', None)
    if event_poller:
      failure_events = event_poller.GetAndLogFailureEvents()
      event_poller.CheckForQuotaFailure(failure_events)
    raise errors.Benchmarks.RunError(
        f'Load runner Job {_LOAD_RUNNER_JOB!r} failed.\nPod logs:\n{logs_out}'
    )

  return logs_out


def _ParseJobOutput(
    logs_out: str,
) -> tuple[list[agent_sandbox_metrics.ClaimRecord], int]:
  """Parses JSONL ClaimRecords and peak_concurrency from pod logs."""
  parts = logs_out.split(_RESULTS_SENTINEL)
  if len(parts) < 2:
    raise errors.Benchmarks.RunError(
        f'No {_RESULTS_SENTINEL!r} sentinel found in pod logs.\n'
        f'Pod logs:\n{logs_out}'
    )

  records = []
  total_peak_concurrency = 0
  found_summary = False
  for section in parts[1:]:
    for line in section.strip().splitlines():
      line = line.strip()
      if not line:
        continue
      try:
        obj = json.loads(line)
      except json.JSONDecodeError:
        continue
      if 'summary' in obj:
        total_peak_concurrency += obj['summary']['peak_concurrency']
        found_summary = True
        break
      else:
        records.append(agent_sandbox_metrics.ClaimRecord(**obj))

  if not found_summary:
    logging.warning('No summary line found; defaulting peak_concurrency to 0')

  return records, total_peak_concurrency


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the load generator Job in-cluster and returns latency samples.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of PerfKitBenchmarker samples from the load generator run.
  """
  sandbox = cast(
      k8s_agent_sandbox.K8sAgentSandbox | None, benchmark_spec.agent_sandbox
  )
  assert sandbox is not None
  spec = sandbox.spec
  namespace = spec.namespace

  shape: agent_sandbox_metrics.RunShape = (
      agent_sandbox_metrics.resolve_run_shape(
          qps=_TARGET_QPS.value,
          duration=_TARGET_DURATION.value
          if _TARGET_SANDBOXES_CREATED.value is None
          else None,
          total=_TARGET_SANDBOXES_CREATED.value,
      )
  )

  scrape_cp_fn = getattr(sandbox, 'ScrapeControlPlaneMetrics', None)
  cp_pre = scrape_cp_fn() if callable(scrape_cp_fn) else {}
  if not isinstance(cp_pre, dict):
    cp_pre = {}

  _DeployLoadRunnerJob(namespace, shape, sandbox=sandbox)
  logs_out = _WaitForJobAndFetchLogs(benchmark_spec, namespace, shape)
  records, peak_concurrency = _ParseJobOutput(logs_out)

  cp_post = scrape_cp_fn() if callable(scrape_cp_fn) else {}
  if not isinstance(cp_post, dict):
    cp_post = {}

  metadata: dict[str, Any] = {
      'target_qps': shape.qps,
      'duration': shape.duration,
      'total_claims': shape.total,
      'workload_duration': _WORKLOAD_DURATION.value,
      'load_runner_parallelism': _LOAD_RUNNER_PARALLELISM.value or 1,
  }
  for phase, cp_data in (('pre', cp_pre), ('post', cp_post)):
    for k, v in cp_data.items():
      metadata[f'cp_{k}_{phase}'] = v
  if 'process_start_time' in cp_pre and 'process_start_time' in cp_post:
    metadata['cp_resized_mid_run'] = (
        cp_pre['process_start_time'] != cp_post['process_start_time']
    )
  metadata.update(sandbox.GetResourceMetadata())
  metadata.update(benchmark_spec.container_cluster.GetResourceMetadata())
  samples = agent_sandbox_metrics.build_samples(
      records, peak_concurrency, metadata
  )
  scrape_fn = getattr(sandbox, 'ScrapeControllerMetrics', None)
  if callable(scrape_fn):
    prom_text = scrape_fn()
    if isinstance(prom_text, str) and prom_text:
      samples.extend(
          agent_sandbox_metrics.parse_controller_prometheus_metrics(
              prom_text, metadata
          )
      )
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """No-op: cluster teardown reclaims the agent sandbox stack."""
  del benchmark_spec
