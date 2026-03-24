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
"""Runs Neper RFS benchmark."""

import logging
import threading
import time
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import neper
from perfkitbenchmarker.linux_packages import rfs

FLAGS = flags.FLAGS

# --- Benchmark Flags ---
flags.DEFINE_integer(
    'neper_num_flows', 10000, 'Total connections created by Neper.'
)
flags.DEFINE_integer(
    'neper_payload_size', 1024, 'Size of request/response payload in bytes.'
)
flags.DEFINE_integer(
    'neper_numa_node', -1, 'NUMA node for pinning. If -1, pinning is disabled.'
)
# Note: RFS global and per-queue flags are now in linux_packages/rfs.py
flags.DEFINE_integer(
    'neper_test_length', 30, 'Duration of the traffic run in seconds.'
)

BENCHMARK_NAME = 'neper'
BENCHMARK_CONFIG = """
neper:
  description: Run Neper RFS benchmark.
  vm_groups:
    vm_1:
      vm_spec: *default_dual_core
    vm_2:
      vm_spec: *default_dual_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def GetPrimaryNIC(vm):
  """Finds the primary network interface."""
  stdout, _ = vm.RemoteCommand(
      "ip route show | grep default | awk '{print $5}'"
  )
  return stdout.strip()


def PrepareVM(vm):
  """Prepares a single VM for the Neper benchmark."""
  vm.Install('neper')

  primary_iface = GetPrimaryNIC(vm)

  # NUMA Validation
  numa_node_file = f'/sys/class/net/{primary_iface}/device/numa_node'
  stdout, _ = vm.RemoteCommand(f'cat {numa_node_file}', ignore_failure=True)
  if stdout:
    try:
      actual_numa = int(stdout.strip())
      if FLAGS.neper_numa_node != -1 and FLAGS.neper_numa_node != actual_numa:
        logging.warning(
            'NUMA node mismatch on %s: expected %s, got %s',
            vm.name,
            FLAGS.neper_numa_node,
            actual_numa,
        )
    except ValueError:
      logging.warning('Could not determine NUMA node for %s', primary_iface)

  # Use standardized RFS configuration
  rfs.Configure(vm)

  # Memory Safety Check:
  # Neper consumes significant memory for socket buffers and tracking flows.
  # We estimate ~128KB of kernel/user memory per flow.
  # We warn if estimated usage exceeds 80% of total system RAM to avoid OOM.
  stdout, _ = vm.RemoteCommand("grep MemTotal /proc/meminfo | awk '{print $2}'")
  total_mem_kb = int(stdout.strip())
  estimated_mem_kb = FLAGS.neper_num_flows * 128
  if estimated_mem_kb > 0.8 * total_mem_kb:
    logging.warning(
        'Estimated memory usage on %s exceeds 80%% of total RAM.', vm.name
    )

  # CPU Count Check
  stdout, _ = vm.RemoteCommand('nproc')
  if int(stdout.strip()) == 1:
    logging.warning(
        'RFS on %s requires at least two cores to provide any steering'
        ' benefit.',
        vm.name,
    )


def Prepare(benchmark_spec):
  """Prepares the VMs for the Neper benchmark."""
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(PrepareVM, vms)


def GetProcStat(vm):
  """Reads and parses /proc/stat."""
  stdout, _ = vm.RemoteCommand("grep '^cpu ' /proc/stat")
  parts = stdout.split()
  # User(1), System(3), Idle(4), IOWait(5), SoftIRQ(7)
  columns = [int(p) for p in parts[1:]]
  total = sum(columns)
  return {
      'softirq': columns[6],
      'total': total,
      'active': (
          total - columns[3] - columns[4]
      ),  # Active = Total - Idle - IOWait
  }


def Run(benchmark_spec):
  """Runs the Neper benchmark."""
  client_vm, server_vm = benchmark_spec.vms[:2]

  # Neper default ports
  server_vm.AllowPort(12866)
  server_vm.AllowPort(12867)

  pinning = (
      f'numactl --cpunodebind={FLAGS.neper_numa_node}'
      if FLAGS.neper_numa_node >= 0
      else ''
  )

  # Start Server:
  # Increase file descriptor limits for high flow counts, pin to NUMA node if
  # requested, and set flow/payload/latency/duration parameters.
  server_args = [
      'ulimit -n 65535 &&',
      pinning,
      neper.GetPath(),
      '-T $(nproc)',
      f'-F {FLAGS.neper_num_flows}',
      f'-Q {FLAGS.neper_payload_size}',
      f'-R {FLAGS.neper_payload_size}',
      '-p 50,90,99',
      f'-l {FLAGS.neper_test_length}',
  ]
  server_cmd = ' '.join(filter(None, server_args))

  def StartServer():
    server_vm.RemoteCommand(server_cmd)

  server_thread = threading.Thread(target=StartServer)
  server_thread.daemon = True
  server_thread.start()

  # Neper consumes significant memory for socket buffers and tracking flows.
  # We wait a few seconds for flow initialization to complete.
  time.sleep(10)

  # Snapshot T0: capture CPU and RFS stats before traffic begins.
  client_t0 = GetProcStat(client_vm)
  server_t0 = GetProcStat(server_vm)
  client_rfs_t0 = rfs.GetSoftnetStat(client_vm)
  server_rfs_t0 = rfs.GetSoftnetStat(server_vm)

  # Run Client: similar parameters to server, but with -c and target host.
  client_args = [
      'ulimit -n 65535 &&',
      pinning,
      neper.GetPath(),
      '-c',
      f'-H {server_vm.internal_ip}',
      '-T $(nproc)',
      f'-F {FLAGS.neper_num_flows}',
      f'-Q {FLAGS.neper_payload_size}',
      f'-R {FLAGS.neper_payload_size}',
      '-p 50,90,99',
      f'-l {FLAGS.neper_test_length}',
  ]
  client_cmd = ' '.join(filter(None, client_args))
  stdout, _ = client_vm.RemoteCommand(client_cmd)

  # Snapshot Tend: capture CPU and RFS stats after traffic finishes.
  client_tend = GetProcStat(client_vm)
  server_tend = GetProcStat(server_vm)
  client_rfs_tend = rfs.GetSoftnetStat(client_vm)
  server_rfs_tend = rfs.GetSoftnetStat(server_vm)

  # Calculate Deltas: Compute utilization percentages based on the snapshots.
  samples = []
  metadata = {
      'numa_node': FLAGS.neper_numa_node,
      'num_flows': FLAGS.neper_num_flows,
      'payload_size': FLAGS.neper_payload_size,
  }
  metadata.update(rfs.GetMetadata())

  def AddCpuSamples(t0, tend, prefix):
    """Calculates CPU and SoftIRQ utilization and appends to samples."""
    delta_total = tend['total'] - t0['total']
    delta_active = tend['active'] - t0['active']
    delta_softirq = tend['softirq'] - t0['softirq']
    if delta_total > 0:
      cpu_util = (delta_active / delta_total) * 100
      softirq_util = (delta_softirq / delta_total) * 100
      samples.append(
          sample.Sample(f'{prefix}_cpu_utilization', cpu_util, '%', metadata)
      )
      samples.append(
          sample.Sample(
              f'{prefix}_softirq_utilization', softirq_util, '%', metadata
          )
      )

  AddCpuSamples(client_t0, client_tend, 'client')
  AddCpuSamples(server_t0, server_tend, 'server')

  # Add RFS verification samples
  samples.append(
      sample.Sample(
          'client_rps_flow_steer_delta',
          client_rfs_tend - client_rfs_t0,
          'count',
          metadata,
      )
  )
  samples.append(
      sample.Sample(
          'server_rps_flow_steer_delta',
          server_rfs_tend - server_rfs_t0,
          'count',
          metadata,
      )
  )

  # Parse Neper Output
  for line in stdout.splitlines():
    if '=' in line:
      key, value = line.split('=')
      if key in ['throughput', 'latency_p50', 'latency_p99', 'latency_max']:
        unit = 'Transactions/s' if key == 'throughput' else 's'
        samples.append(sample.Sample(key, float(value), unit, metadata))

  return samples


def CleanupVM(vm):
  """Cleans up a single VM."""
  vm.RemoteCommand('sudo killall tcp_rr', ignore_failure=True)
  rfs.Restore(vm)


def Cleanup(benchmark_spec):
  """Cleans up the VMs after the Neper benchmark."""
  vms = benchmark_spec.vms
  background_tasks.RunThreaded(CleanupVM, vms)
