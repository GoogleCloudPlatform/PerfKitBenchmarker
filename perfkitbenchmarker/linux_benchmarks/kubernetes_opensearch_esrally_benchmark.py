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

"""Run opensearch-benchmark against OpenSearch on Kubernetes."""

import json
from typing import Any
import uuid

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'kubernetes_opensearch_esrally_swap_enabled',
    False,
    'Whether to enable swap and upgrade instances.',
)
flags.DEFINE_string(
    'kubernetes_opensearch_esrally_opensearch_version',
    '2.13.0',
    'OpenSearch image tag.',
)
flags.DEFINE_string(
    'kubernetes_opensearch_esrally_esrally_version',
    '1.7.0',
    'opensearch-benchmark pip package version.',
)
flags.DEFINE_string(
    'kubernetes_opensearch_esrally_esrally_track',
    'geonames',
    'opensearch-benchmark workload name.',
)
flags.DEFINE_string(
    'kubernetes_opensearch_esrally_esrally_challenge',
    'append-no-conflicts',
    'opensearch-benchmark challenge name.',
)
flags.DEFINE_integer(
    'kubernetes_opensearch_esrally_opensearch_port',
    9200,
    'HTTP port.',
)

BENCHMARK_NAME = 'kubernetes_opensearch_esrally'
BENCHMARK_CONFIG = """
kubernetes_opensearch_esrally:
  description: Run opensearch-benchmark on Kubernetes
  container_cluster:
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
          AWS:
            machine_type: m6i.xlarge
"""

_RACE_JSON_BEGIN = '=== PKB_RACE_JSON_BEGIN ==='
_RACE_JSON_END = '=== PKB_RACE_JSON_END ==='


def GetConfig(user_config):
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.kubernetes_opensearch_esrally_swap_enabled:
    server_np = config['container_cluster']['nodepools']['servers']
    server_np['swap_config'] = {
        'enabled': True,
        'swappiness': 100,
        'min_free_kbytes': 67584,
        'watermark_scale_factor': 1000,
        'boot_disk_iops': 160000,
        'boot_disk_throughput': 1000,
    }
    gcp_spec = server_np['vm_spec'].get('GCP')
    if gcp_spec:
      gcp_spec['machine_type'] = 'n4-highmem-32'
      gcp_spec['boot_disk_type'] = 'hyperdisk-balanced'
      gcp_spec['boot_disk_size'] = 500
    aws_spec = server_np['vm_spec'].get('AWS')
    if aws_spec:
      aws_spec['machine_type'] = 'r6i.8xlarge'
      aws_spec['boot_disk_type'] = 'gp3'
  return config


def Prepare(benchmark_spec: Any):
  """Prepare the OpenSearch cluster."""
  del benchmark_spec  # Unused.
  kubernetes_commands.ApplyManifest(
      'container/kubernetes_opensearch_esrally/opensearch.yaml.j2',
      opensearch_version=FLAGS.kubernetes_opensearch_esrally_opensearch_version,
      opensearch_port=FLAGS.kubernetes_opensearch_esrally_opensearch_port,
  )
  kubernetes_commands.WaitForRollout('statefulset/opensearch')


def Run(
    benchmark_spec: Any,
) -> list[sample.Sample]:
  """Run the benchmark."""
  del benchmark_spec  # Unused.
  kubectl.RunKubectlCommand(
      ['delete', 'job', 'esrally-pkb', '--ignore-not-found']
  )

  race_id = uuid.uuid4().hex
  created_resources = kubernetes_commands.ApplyManifest(
      'container/kubernetes_opensearch_esrally/esrally_job.yaml.j2',
      esrally_version=FLAGS.kubernetes_opensearch_esrally_esrally_version,
      target_hosts=(
          f'opensearch:{FLAGS.kubernetes_opensearch_esrally_opensearch_port}'
      ),
      esrally_track=FLAGS.kubernetes_opensearch_esrally_esrally_track,
      esrally_challenge=FLAGS.kubernetes_opensearch_esrally_esrally_challenge,
      race_id=race_id,
  )
  job_path = list(created_resources)[0]
  job_name = job_path.split('/')[1]

  condition = kubernetes_commands.WaitForResourceForMultiConditions(
      f'jobs/{job_name}',
      ['condition=Complete', 'condition=Failed'],
      timeout=3600,
  )

  if not condition:
    raise errors.Benchmarks.RunError(f'esrally Job {job_name} timed out.')
  if condition == 'condition=Failed':
    raise errors.Benchmarks.RunError(f'esrally Job {job_name} failed.')

  logs, _, _ = kubectl.RunKubectlCommand(
      ['logs', f'jobs/{job_name}'], raise_on_failure=True
  )
  return _ParseResults(logs)


def _ParseResults(logs):
  """Parses results from pod logs."""
  begin_idx = logs.find(_RACE_JSON_BEGIN)
  end_idx = logs.find(_RACE_JSON_END)
  if begin_idx == -1 or end_idx == -1:
    return []

  json_str = logs[begin_idx + len(_RACE_JSON_BEGIN) : end_idx].strip()
  try:
    race_json = json.loads(json_str)
  except json.JSONDecodeError:
    return []

  if 'results' not in race_json or 'op_metrics' not in race_json['results']:
    return []

  samples = []
  metadata = {
      'rally_version': race_json.get('rally-version'),
      'rally_track': race_json.get('track'),
      'rally_challenge': race_json.get('challenge'),
      'opensearch_version': (
          FLAGS.kubernetes_opensearch_esrally_opensearch_version
      ),
      'swap_enabled': FLAGS.kubernetes_opensearch_esrally_swap_enabled,
  }

  for op in race_json['results']['op_metrics']:
    task = op.get('task', '').replace('-', '_')
    meta = metadata.copy()
    meta['esrally_task'] = task

    if 'throughput' in op:
      samples.append(
          sample.Sample(
              f'{task}_throughput',
              op['throughput'].get('mean', 0.0),
              op['throughput'].get('unit', ''),
              meta,
          )
      )
    if 'latency' in op:
      samples.append(
          sample.Sample(
              f'{task}_latency_mean',
              op['latency'].get('mean', 0.0),
              op['latency'].get('unit', ''),
              meta,
          )
      )
      if '90_0' in op['latency']:
        samples.append(
            sample.Sample(
                f'{task}_latency_p90',
                op['latency']['90_0'],
                op['latency'].get('unit', ''),
                meta,
            )
        )

  return samples


def Cleanup(benchmark_spec: Any):
  """Cleanup esrally on the target vm."""
  del benchmark_spec  # Unused.


def Teardown(benchmark_spec: Any):
  """Teardown the benchmark."""
  del benchmark_spec  # Unused.
