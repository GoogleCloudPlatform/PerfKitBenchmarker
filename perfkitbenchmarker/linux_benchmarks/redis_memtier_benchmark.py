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

from typing import Any, Dict, List
import logging

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server

# location for top command output
_TOP_OUTPUT = 'top.txt'
# location for top script
_TOP_SCRIPT = 'top_script.sh'

FLAGS = flags.FLAGS
CLIENT_OS_TYPE = flags.DEFINE_string(
    'redis_memtier_client_os_type',
    None,
    'If provided, overrides the memtier client os type.',
)
SERVER_OS_TYPE = flags.DEFINE_string(
    'redis_memtier_server_os_type',
    None,
    'If provided, overrides the redis server os type.',
)
flags.DEFINE_string(
    'redis_memtier_client_machine_type',
    None,
    'If provided, overrides the memtier client machine type.',
)
flags.DEFINE_string(
    'redis_memtier_server_machine_type',
    None,
    'If provided, overrides the redis server machine type.',
)
REDIS_MEMTIER_MEASURE_CPU = flags.DEFINE_bool(
    'redis_memtier_measure_cpu',
    False,
    'If true, measure cpu usage on the server via top tool. Defaults to False.',
)
BENCHMARK_NAME = 'redis_memtier'
BENCHMARK_CONFIG = """
redis_memtier:
  description: >
      Run memtier_benchmark against Redis.
  flags:
    memtier_protocol: redis
    create_and_boot_post_task_delay: 5
    memtier_data_size: 1024
    memtier_pipeline: 1
    placement_group_style: none
    redis_aof: False
    sar: True
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
  if FLAGS.redis_server_cluster_mode and not FLAGS.memtier_cluster_mode:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--redis_server_cluster_mode must be used with --memtier_cluster_mode'
    )
  if len(redis_server.GetRedisPorts()) >= 0 and (
      len(FLAGS.memtier_pipeline) > 1
      or len(FLAGS.memtier_threads) > 1
      or len(FLAGS.memtier_clients) > 1
  ):
    raise errors.Setup.InvalidFlagConfigurationError(
        'There can only be 1 setting for pipeline, threads and clients if '
        'there are multiple redis endpoints. Consider splitting up the '
        'benchmarking.'
    )


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if CLIENT_OS_TYPE.value:
    config['vm_groups']['clients']['os_type'] = CLIENT_OS_TYPE.value

  if SERVER_OS_TYPE.value:
    config['vm_groups']['servers']['os_type'] = SERVER_OS_TYPE.value

  if FLAGS.redis_memtier_client_machine_type:
    vm_spec = config['vm_groups']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_client_machine_type
  if FLAGS.redis_memtier_server_machine_type:
    vm_spec = config['vm_groups']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.redis_memtier_server_machine_type
  if not redis_server.REDIS_AOF.value:
    config['vm_groups']['servers']['disk_count'] = 0
  return config


def PrepareSystem(bm_spec: _BenchmarkSpec) -> None:
  """Set system-wide parameters."""
  server_vms = bm_spec.vm_groups['servers']  # for cluster mode
  background_tasks.RunThreaded(redis_server.PrepareSystem, server_vms)


def InstallPackages(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on the servers and memtier on the clients."""
  client_vms = bm_spec.vm_groups['clients']
  server_vms = bm_spec.vm_groups['servers']
  background_tasks.RunThreaded(
      lambda client: client.Install('memtier'), client_vms
  )
  background_tasks.RunThreaded(
      lambda server: server.Install('redis_server'), server_vms
  )


def StartServices(bm_spec: _BenchmarkSpec) -> None:
  """Start Redis servers."""
  server_count = len(bm_spec.vm_groups['servers'])
  if server_count != 1 and not redis_server.CLUSTER_MODE.value:
    raise errors.Benchmarks.PrepareException(
        f'Expected servers vm count to be 1, got {server_count}'
    )
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]
  all_server_vms = bm_spec.vm_groups['servers']  # for cluster mode

  # Set io_threads from command-line flag if specified
  if redis_server.IO_THREADS.value:
    # Use the first value if it's a sweep
    io_threads_list = [int(io) for io in redis_server.IO_THREADS.value]
    if io_threads_list:
      redis_server.CURRENT_IO_THREADS = io_threads_list[0]
      logging.info(f'Setting io_threads to {redis_server.CURRENT_IO_THREADS} from flag')

  for vm in all_server_vms:
    redis_server.Start(vm)

  if redis_server.CLUSTER_MODE.value:
    redis_server.StartCluster(all_server_vms)

  # Load the redis server with preexisting data.
  # Run 4 at a time with only the first client VM to reduce memory
  # fragmentation and avoid overloading the server
  bm_spec.redis_endpoint_ip = bm_spec.vm_groups['servers'][0].internal_ip
  ports = redis_server.GetRedisPorts(server_vm)
  ports_group_of_four = [ports[i : i + 4] for i in range(0, len(ports), 4)]
  assert bm_spec.redis_endpoint_ip
  for ports_group in ports_group_of_four:
    # pylint: disable=g-long-lambda
    background_tasks.RunThreaded(
        lambda port: memtier.Load(
            [client_vms[0]], bm_spec.redis_endpoint_ip, port
        ),
        ports_group,
        10,
    )


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  client_vms = bm_spec.vm_groups['clients']
  # IMPORTANT: Don't reference vm_groups['servers'] directly, because this is
  # reused by kubernetes_redis_memtier_benchmark, which doesn't define it.
  server_vm: virtual_machine.BaseVirtualMachine | None = None
  if 'servers' in bm_spec.vm_groups:
    server_vm = bm_spec.vm_groups['servers'][0]
  measure_cpu_on_server_vm = server_vm and REDIS_MEMTIER_MEASURE_CPU.value

  benchmark_metadata = {}

  if measure_cpu_on_server_vm:
    top_cmd = (
        f'top -b -d 1 -n {memtier.MEMTIER_RUN_DURATION.value} > {_TOP_OUTPUT} &'
    )
    server_vm.RemoteCommand(f'echo "{top_cmd}" > {_TOP_SCRIPT}')
    server_vm.RemoteCommand(f'bash {_TOP_SCRIPT}')

  assert bm_spec.redis_endpoint_ip

  # Sweep the number of IO threads to find the optimal value.
  io_threads_to_sweep = (
      [int(io_threads) for io_threads in redis_server.IO_THREADS.value]
      if redis_server.IO_THREADS.value
      else [None]
  )

  maximum_total_ops_throughput = 0.0
  result_with_maximum_total_ops_throughput = None
  all_results = []
  top_results = []

  try:
    for io_threads in io_threads_to_sweep:
      # Override the io_threads and restart the server if io_threads is not None
      if io_threads is not None:
        _UpdateIOThreadsForRedisServer(bm_spec.vm_groups['servers'], io_threads)

      raw_results = memtier.RunOverAllThreadsPipelinesAndClients(
          client_vms,
          bm_spec.redis_endpoint_ip,
          redis_server.GetRedisPorts(server_vm),
      )
      redis_metadata = redis_server.GetMetadata(server_vm)

      total_ops_throughput = 0.0
      for server_result in raw_results:
        if server_result.metric == 'Total Ops Throughput':
          total_ops_throughput = server_result.value
        server_result.metadata.update(redis_metadata)
        server_result.metadata.update(benchmark_metadata)

      if total_ops_throughput > maximum_total_ops_throughput:
        maximum_total_ops_throughput = total_ops_throughput
        result_with_maximum_total_ops_throughput = raw_results

      # Add the results with the current io_threads value
      if io_threads is not None:
        for server_result in raw_results:
          # Cannot assign to metric directly because it is an immutable
          # named tuple
          server_result = server_result._replace(
              metric=f'{server_result.metric} (io_threads={io_threads})'
          )
          all_results.append(server_result)

    if measure_cpu_on_server_vm:
      top_results = _GetTopResults(server_vm)
  finally:
    if redis_server.REDIS_AOF.value and redis_server.REDIS_AOF_VERIFY.value:
      for server_vm in bm_spec.vm_groups['servers']:
        redis_server.VerifyRedisAof(server_vm)

  return result_with_maximum_total_ops_throughput + all_results + top_results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  del bm_spec


def _UpdateIOThreadsForRedisServer(
    server_vms: List[virtual_machine.BaseVirtualMachine], io_threads: int
) -> None:
  for server_vm in server_vms:
    redis_server.Stop(server_vm)

  for server_vm in server_vms:
    redis_server.CURRENT_IO_THREADS = io_threads
    redis_server.Start(server_vm)

  if redis_server.CLUSTER_MODE.value:
    redis_server.StartCluster(server_vms)


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
        sample.Sample(
            'CPU Idle time',
            idle_value,
            '%Cpu(s)',
            {
                'time_series_sec': row_index,
                'cpu_idle_percent': idle_value,
            },
        )
    )
    row_index += 1
  return samples
