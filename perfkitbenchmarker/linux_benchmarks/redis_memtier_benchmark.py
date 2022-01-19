# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run memtier_benchmark against Redis.

memtier_benchmark is a load generator created by RedisLabs to benchmark
Redis.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""

import datetime
import logging
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server

# location for top command output
_TOP_OUTPUT = 'top.txt'
# location for top script
_TOP_SCRIPT = 'top_script.sh'

FLAGS = flags.FLAGS
flags.DEFINE_string('redis_memtier_client_machine_type', None,
                    'If provided, overrides the memtier client machine type.')
flags.DEFINE_string('redis_memtier_server_machine_type', None,
                    'If provided, overrides the redis server machine type.')
REDIS_MEMTIER_SIMULATE_DISK = flags.DEFINE_bool(
    'redis_memtier_simulate_disk', False, 'If true, simulate usage of '
    'disks on the server by filling any scratch disks, if present. ')
REDIS_MEMTIER_MEASURE_CPU = flags.DEFINE_bool(
    'redis_memtier_measure_cpu', False, 'If true, measure cpu usage on the '
    'server via top tool. Defaults to False.')
BENCHMARK_NAME = 'redis_memtier'
BENCHMARK_CONFIG = """
redis_memtier:
  description: >
      Run memtier_benchmark against Redis.
      Specify the number of client VMs with --redis_clients.
  vm_groups:
    servers:
      vm_spec: *default_dual_core
      vm_count: 1
      disk_spec: *default_50_gb
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def CheckPrerequisites(_):
  """Verifies that benchmark setup is correct."""
  if len(redis_server.GetRedisPorts()) > 1 and (
      len(FLAGS.memtier_pipeline) > 1 or
      len(FLAGS.memtier_threads) > 1 or
      len(FLAGS.memtier_clients) > 1):
    raise errors.Setup.InvalidFlagConfigurationError(
        'There can only be 1 setting for pipeline, threads and clients if '
        'there are multiple redis endpoints. Consider splitting up the '
        'benchmarking.')


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['vm_groups']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.redis_memtier_client_machine_type)
  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['vm_groups']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = (
          FLAGS.redis_memtier_server_machine_type)
  if not REDIS_MEMTIER_SIMULATE_DISK.value:
    config['vm_groups']['servers'].pop('disk_spec')
  return config


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on one VM and memtier_benchmark on another."""
  server_count = len(bm_spec.vm_groups['servers'])
  if server_count != 1:
    raise errors.Benchmarks.PrepareException(
        f'Expected servers vm count to be 1, got {server_count}')
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]

  # Install memtier
  vm_util.RunThreaded(lambda client: client.Install('memtier'),
                      client_vms + [server_vm])

  # Install redis on the 1st machine.
  server_vm.Install('redis_server')
  redis_server.Start(server_vm)

  if REDIS_MEMTIER_SIMULATE_DISK.value:
    server_vm.InstallPackages('fio')
    for disk in server_vm.scratch_disks:
      cmd = ('sudo fio --name=global --direct=1 --ioengine=libaio --numjobs=1 '
             '--refill_buffers --scramble_buffers=1 --allow_mounted_write=1 '
             '--blocksize=128k --rw=write --iodepth=64 --size=100% '
             f'--name=wipc --filename={disk.device_path}')
      logging.info('Start filling %s. This may take up to 30min...',
                   disk.device_path)
      server_vm.RemoteCommand(cmd)

  # Load the redis server with preexisting data.
  for port in redis_server.GetRedisPorts():
    memtier.Load(server_vm, 'localhost', str(port))

  bm_spec.redis_endpoint_ip = bm_spec.vm_groups['servers'][0].internal_ip
  vm_util.SetupSimulatedMaintenance(server_vm)


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  client_vms = bm_spec.vm_groups['clients']
  # Don't reference vm_groups['server'] directly, because this is reused by
  # kubernetes_redis_memtier_benchmark, which doesn't have one.
  measure_cpu_on_server_vm = (
      REDIS_MEMTIER_MEASURE_CPU.value and 'servers' in bm_spec.vm_groups)

  ports = [str(port) for port in redis_server.GetRedisPorts()]
  def DistributeClientsToPorts(port):
    client_index = int(port) % len(ports) % len(client_vms)
    vm = client_vms[client_index]
    return memtier.RunOverAllThreadsPipelinesAndClients(
        vm, bm_spec.redis_endpoint_ip, port)

  benchmark_metadata = {}

  # if testing performance due to a live migration, simulate live migration.
  # actual live migration timestamps is not reported by PKB.
  if FLAGS.simulate_maintenance:
    vm_util.StartSimulatedMaintenance()
    simulate_maintenance_time = datetime.datetime.now() + datetime.timedelta(
        seconds=FLAGS.simulate_maintenance_delay)
    benchmark_metadata[
        'simulate_maintenance_time'] = str(simulate_maintenance_time)
    benchmark_metadata[
        'simulate_maintenance_delay'] = FLAGS.simulate_maintenance_delay
  if measure_cpu_on_server_vm:
    server_vm = bm_spec.vm_groups['servers'][0]
    top_cmd = f'top -b -d 1 -n {memtier.MEMTIER_RUN_DURATION.value} > {_TOP_OUTPUT} &'
    server_vm.RemoteCommand(
        f'echo "{top_cmd}" > {_TOP_SCRIPT}')
    server_vm.RemoteCommand(f'bash {_TOP_SCRIPT}')

  raw_results = vm_util.RunThreaded(DistributeClientsToPorts, ports)
  redis_metadata = redis_server.GetMetadata()

  top_results = []
  if measure_cpu_on_server_vm:
    top_results = _GetTopResults(server_vm)

  results = []

  for server_result in raw_results:
    for result_sample in server_result:
      result_sample.metadata.update(redis_metadata)
      result_sample.metadata.update(benchmark_metadata)
      results.append(result_sample)

  return results + top_results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  del bm_spec


def _GetTopResults(server_vm) -> List[sample.Sample]:
  """Gat and parse CPU output from top command."""
  if not REDIS_MEMTIER_MEASURE_CPU.value:
    return []
  cpu_usage, _ = server_vm.RemoteCommand(f'grep Cpu {_TOP_OUTPUT}')

  samples = []
  row_index = 0
  for row in cpu_usage.splitlines():
    line = row.strip()
    columns = line.split(',')
    idle_value, _ = columns[3].strip().split(' ')
    samples.append(
        sample.Sample('CPU Idle time', idle_value, '%Cpu(s)',
                      {'time_series_sec': row_index,
                       'cpu_idle_percent': idle_value,}))
    row_index += 1
  return samples
