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

"""Run memtier_benchmark against a KVrocks cluster."""

import logging
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import kvrocks_server
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import prometheus

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'kvrocks_memtier'
BENCHMARK_CONFIG = """
kvrocks_memtier:
  description: >
      Run memtier_benchmark against KVrocks.
  vm_groups:
    servers:
      vm_spec:
        GCP:
          machine_type: c3-standard-44-lssd
      vm_count: 3
      os_type: rocky-9
      disk_spec:
        GCP:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
        AWS:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
        Azure:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
    clients:
      vm_spec:
        GCP:
          machine_type: n2-standard-64
      vm_count: 1
      os_type: rocky-9
      disk_spec:
        GCP:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
        AWS:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
        Azure:
          disk_type: local
          num_striped_disks: 8
          mount_point: /scratch
"""


_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(_):
  """Verifies that benchmark setup is correct."""
  pass


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install and set up KVrocks and client tools."""
  server_vms = bm_spec.vm_groups['servers']
  client_vms = bm_spec.vm_groups['clients']
  vms = server_vms + client_vms

  background_tasks.RunThreaded(lambda vm: vm.Install('kvrocks_server'), vms)
  background_tasks.RunThreaded(lambda vm: vm.Install('memtier'), client_vms)

  # The nc provided by nmap-ncat on Rocky9 does not output "succeeded"
  # which is expected by memtier._CheckRedisReachable.
  # This wrapper script mimics the expected behavior.
  def _InstallNcWrapper(vm):
    vm.InstallPackages('nmap-ncat')
    nc_wrapper = """
#!/bin/bash
/usr/bin/nc "$@"
if [[ $? -eq 0 ]]; then
  echo "succeeded"
fi
"""
    vm.RemoteCommand(f"echo '{nc_wrapper}' | sudo tee /usr/local/bin/nc")
    vm.RemoteCommand('sudo chmod +x /usr/local/bin/nc')

  background_tasks.RunThreaded(_InstallNcWrapper, client_vms)

  def _InstallAndStartPrometheus(vm):
    vm.Install('prometheus')
    prometheus.ConfigureAndStart(vm, server_vms)

  background_tasks.RunThreaded(_InstallAndStartPrometheus, client_vms)

  # Skipping preconditioning for now.
  # TODO(user): Add preconditioning.
  kvrocks_server.StartCluster(server_vms)

  def _StartExporter(vm):
    kvrocks_server.StartExporter(vm)

  # The prometheus server on the client is configured to scrape exporters
  # from the server VMs.
  background_tasks.RunThreaded(_StartExporter, server_vms)

  prometheus.WaitUntilHealthy(client_vms[0])

  # Check cluster status
  master_vm = server_vms[0]

  try:
    cluster_info, _ = kvrocks_server.RunRedisCommand(
        master_vm, '-c cluster info'
    )
    logging.info('KVrocks cluster info: %s', cluster_info)
    cluster_nodes, _ = kvrocks_server.RunRedisCommand(
        master_vm, '-c cluster nodes'
    )
    logging.info('KVrocks cluster nodes: %s', cluster_nodes)
  except errors.VirtualMachine.RemoteCommandError as e:
    logging.warning('KVrocks cluster not ready or redis commands failed: %s', e)


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run the memtier benchmark and collect results."""
  client_vms = bm_spec.vm_groups['clients']
  server_vms = bm_spec.vm_groups['servers']
  master_vm = server_vms[0]
  ip = master_vm.internal_ip
  ports = list(kvrocks_server.INSTANCE_TO_PORT_MAP.values())

  logging.info('Starting memtier runs...')
  try:
    results = memtier.RunOverAllThreadsPipelinesAndClients(
        client_vms, ip, [ports[0]], kvrocks_server.GetPassword()
    )
  except errors.VirtualMachine.RemoteCommandError as e:
    logging.exception('Memtier run failed: %s', e)
    results = []
  logging.info('Memtier runs finished. Results: %s', results)

  metadata = kvrocks_server.GetMetadata()
  for res in results:
    res.metadata.update(metadata)

  # Query prometheus stats from the first client
  client_vm = client_vms[0]

  slowlog_total = prometheus.RunQuery(client_vm, 'sum(kvrocks_slowlog_total)')
  commands_total = prometheus.RunQuery(
      client_vm, 'sum(kvrocks_commands_total{cmd="hmset"})'
  )
  slowlog_ratio = prometheus.RunQuery(
      client_vm,
      'sum(kvrocks_slowlog_total) / sum(kvrocks_commands_total{cmd="hmset"})',
  )

  logging.info('Slowlog total: %s', slowlog_total)
  logging.info('Commands total: %s', commands_total)
  logging.info('Slowlog ratio: %s', slowlog_ratio)

  if slowlog_total is not None:
    results.append(
        sample.Sample('KVRocks Slowlog Total', slowlog_total, 'count', metadata)
    )

  if commands_total is not None:
    results.append(
        sample.Sample(
            'KVRocks Commands Total HMSET',
            commands_total,
            'count',
            metadata,
        )
    )

  if slowlog_ratio is not None:
    results.append(
        sample.Sample('KVRocks Slowlog Ratio', slowlog_ratio, 'ratio', metadata)
    )

  return results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  """Cleanup resources."""
  del bm_spec
