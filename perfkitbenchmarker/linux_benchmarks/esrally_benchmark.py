# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs the ElasticSearch Rally benchmark https://github.com/elastic/rally."""

import json
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

BENCHMARK_NAME = 'esrally'
BENCHMARK_CONFIG = """
esrally:
  description: Run esrally
  vm_groups:
    servers:
      vm_spec:
        GCP:
          machine_type: n2-standard-8
          zone: us-central1-b
      disk_spec:
        # Standardize with 700 MB/s bandwidth to minimize I/O impact.
        GCP:
          disk_size: 500
          disk_type: hyperdisk-balanced
          provisioned_iops: 12000
          provisioned_throughput: 700
          mount_point: /scratch
      # For single node, specify --track-param=number_of_replicas:0
      vm_count: 3
    clients:
      vm_spec:
        GCP:
          machine_type: n2-standard-16
          zone: us-central1-b
      disk_spec: *default_50_gb
      vm_count: 1
  flags:
    openjdk_version: 17
    iostat: true
"""

SET_RALLY_HOME = 'export RALLY_HOME=/scratch/'
SET_ENV = 'source esrally_venv/bin/activate'
SET_JAVA = 'export JAVA_HOME=/usr/'
SET_ALL = f'{SET_RALLY_HOME} && {SET_ENV} && {SET_JAVA}'
HTTP_PORT = 39200
TLS_PORT = 39300
FLAGS = flags.FLAGS

# By default run the elastic/logs track which is only supported on version 7.9+
# https://github.com/elastic/rally-tracks/tree/master/elastic/logs#elasticsearch-compatibility
ESRALLY_TRACK = flags.DEFINE_string(
    'esrally_track',
    'elastic/logs',
    'Track to run.',
)
ESRALLY_CHALLENGE = flags.DEFINE_string(
    'esrally_challenge',
    'logging-indexing',
    'Challenge to run within specified track.',
)
ESRALLY_DISTRIBUTION_VERSION = flags.DEFINE_string(
    'esrally_distribution_version',
    '7.17.0',
    'Elasticsearch distribution version to use.',
)
ESRALLY_TRACK_PARAMS = flags.DEFINE_list(
    'esrally_track_params',
    [],
    'List of additional --track-params to set.',
)


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


@vm_util.Retry(max_retries=10, poll_interval=10)
def _CheckClusterStatus(vm):
  vm.RemoteCommand(f'curl http://{vm.internal_ip}:{HTTP_PORT}/_cat/health\\?v')


def _InstallEsrally(vm, master_nodes, seed_hosts, is_load_generator=False):
  """Install esrally on the target vm.

  Args:
    vm: The VM to install esrally on.
    master_nodes: comma separated string of the cluster's vm names.
    seed_hosts: comma separated string of the cluster's internal IPs.
    is_load_generator: Whether the vm is a load generator.
  """
  vm.Install('openjdk')
  vm.InstallPackages('python3-pip python3-venv')
  vm.RemoteCommand('python3 -m venv esrally_venv')
  vm.RemoteCommand(f'{SET_ENV} && pip3 install esrally')
  if is_load_generator:
    return
  install_cmd = (
      f'{SET_ALL} && '
      'esrally install --quiet '
      f'--distribution-version={ESRALLY_DISTRIBUTION_VERSION.value} '
      f'--node-name={vm.name} '
      f'--network-host={vm.internal_ip} --http-port={HTTP_PORT} '
      f'--master-nodes={master_nodes} --seed-hosts={seed_hosts}'
  )
  stdout, _ = vm.RemoteCommand(install_cmd)
  installation_id = json.loads(stdout)['installation-id']
  vm.esrally_installation_id = installation_id


def Prepare(benchmark_spec):
  """Installs dependencies and setups up esrally cluster.

  Args:
    benchmark_spec: The benchmark specification.
  """
  client_vm = benchmark_spec.vm_groups['clients'][0]
  server_vms = benchmark_spec.vm_groups['servers']
  master_nodes = ','.join(vm.name for vm in server_vms)
  seed_hosts = ','.join(f'{vm.internal_ip}:{TLS_PORT}' for vm in server_vms)

  # Install Esrally on nodes
  _InstallEsrally(client_vm, master_nodes, seed_hosts, is_load_generator=True)
  background_tasks.RunThreaded(
      lambda vm: _InstallEsrally(vm, master_nodes, seed_hosts),
      server_vms,
  )


def Run(benchmark_spec):
  """Runs esrally on the target vm.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    Benchmarks.RunError: If no results are found.
  """
  client_vm = benchmark_spec.vm_groups['clients'][0]
  server_vms = benchmark_spec.vm_groups['servers']

  # Start Esrally on nodes.
  race_id, _ = client_vm.RemoteCommand('echo -n $(uuidgen)')
  for vm in server_vms:
    installation_id = vm.esrally_installation_id
    start_cmd = (
        f'{SET_ALL} && '
        f'esrally start --installation-id={installation_id} --race-id={race_id}'
    )
    vm.RemoteCommand(start_cmd)
  _CheckClusterStatus(server_vms[0])

  target_hosts = ','.join(f'{vm.internal_ip}:{HTTP_PORT}' for vm in server_vms)
  extra_track_params = ','.join(
      f'{param}' for param in ESRALLY_TRACK_PARAMS.value
  )
  run_cmd = (
      f'{SET_ALL} && '
      'esrally race '
      f'--pipeline=benchmark-only --target-host={target_hosts} '
      f'--track={ESRALLY_TRACK.value} --challenge={ESRALLY_CHALLENGE.value} '
      f'--race-id={race_id} --track-params={extra_track_params}'
  )
  client_vm.RemoteCommand(run_cmd)

  samples = []
  stdout, _ = client_vm.RemoteCommand(
      f'cat {client_vm.GetScratchDir()}/.rally/benchmarks/races/{race_id}/race.json'
  )
  metrics = json.loads(stdout)
  metadata = {
      'rally_version': metrics['rally-version'],
      'rally_track': metrics['track'],
      'rally_challenge': metrics['challenge'],
      'esrally_track_params': ESRALLY_TRACK_PARAMS.value or 'default',
      'es_distribution_version': ESRALLY_DISTRIBUTION_VERSION.value,
  }
  # TODO(user): Capture important latency statistics as well.
  for op_metric in metrics['results']['op_metrics']:
    task = op_metric['task']
    mean_throughput = op_metric['throughput']['mean']
    unit = op_metric['throughput']['unit']
    samples.append(
        sample.Sample(
            f'{task}-throughput'.replace('-', '_'),
            mean_throughput,
            unit,
            metadata.copy(),
        )
    )
  client_vm.PullFile(
      vm_util.GetTempDir(), f'{client_vm.GetScratchDir()}/.rally/logs/rally.log'
  )

  # Stop Esrally on nodes.
  for vm in server_vms:
    installation_id = vm.esrally_installation_id
    stop_cmd = f'{SET_ALL} && esrally stop --installation-id={installation_id}'
    vm.RemoteCommand(stop_cmd, ignore_failure=True)
  return samples


def Cleanup(_):
  """Cleanup esrally on the target vm."""
  return
