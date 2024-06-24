# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run memtier_benchmark against KeyDB (https://docs.keydb.dev/).

memtier_benchmark (https://github.com/RedisLabs/memtier_benchmark) is a
load generator created by RedisLabs to benchmark Redis. For the list of flags
supported by Memtier, see
https://github.com/RedisLabs/memtier_benchmark/blob/master/memtier_benchmark.cpp
"""

from typing import Any, Dict, List, Optional

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import keydb_server
from perfkitbenchmarker.linux_packages import memtier

FLAGS = flags.FLAGS
BENCHMARK_NAME = 'keydb_memtier'
BENCHMARK_CONFIG = """
keydb_memtier:
  description: >
      Run memtier_benchmark against Keydb.
  flags:
    memtier_protocol: redis
    memtier_key_pattern: P:P
    memtier_run_duration: 1200
    memtier_expiry_range: 600-600
    create_and_boot_post_task_delay: 5
    memtier_data_size: 1024
    memtier_key_maximum: 20000000
    memtier_clients: 50
    memtier_threads: 32
    memtier_pipeline: 1
    placement_group_style: none
  vm_groups:
    servers:
      vm_spec:
        GCP:
          machine_type: n2-standard-8
          zone: us-central1-b
        AWS:
          machine_type: m7g.2xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_D8as_v5
          zone: eastus2
      vm_count: 1
    clients:
      vm_spec:
        GCP:
          machine_type: n2-standard-32
          zone: us-central1-b
        AWS:
          machine_type: m7g.8xlarge
          zone: us-east-1a
        Azure:
          machine_type: Standard_D32as_v5
          zone: eastus2
      vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec


def CheckPrerequisites(_) -> None:
  """Verifies that benchmark setup is correct."""
  return


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def Prepare(bm_spec: _BenchmarkSpec) -> None:
  """Install KeyDB on one VM and memtier_benchmark on others."""
  server_count = len(bm_spec.vm_groups['servers'])
  if server_count != 1:
    raise errors.Benchmarks.PrepareException(
        f'Expected servers vm count to be 1, got {server_count}'
    )
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]

  # Install memtier
  background_tasks.RunThreaded(
      lambda client: client.Install('memtier'), client_vms
  )

  # Install KeyDB on the 1st machine.
  server_vm.Install('keydb_server')
  keydb_server.Start(server_vm)

  # Transfer SSL cert files to client(s)
  if FLAGS.memtier_tls:
    for client_vm in client_vms:
      for filename in ['keydb.crt', 'keydb.key', 'ca.crt']:
        server_vm.MoveHostFile(
            client_vm,
            f'{keydb_server.GetKeydbDir()}/tests/tls/{filename}',
            remote_path='',
        )

  # Load the KeyDB server with preexisting data.
  bm_spec.keydb_endpoint_ip = str(bm_spec.vm_groups['servers'][0].internal_ip)
  # Hack in SSL args into the endpoint IP
  if FLAGS.memtier_tls:
    bm_spec.keydb_endpoint_ip = (
        f'{bm_spec.keydb_endpoint_ip} '
        '--cert=keydb.crt '
        '--key=keydb.key '
        '--cacert=ca.crt'
    )
  memtier.Load(
      [client_vms[0]], bm_spec.keydb_endpoint_ip, keydb_server.DEFAULT_PORT
  )


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against KeyDB."""
  client_vms = bm_spec.vm_groups['clients']
  server_vm: Optional[virtual_machine.BaseVirtualMachine] = None
  if 'servers' in bm_spec.vm_groups:
    server_vm = bm_spec.vm_groups['servers'][0]

  benchmark_metadata = {}

  raw_results = memtier.RunOverAllThreadsPipelinesAndClients(
      client_vms,
      bm_spec.keydb_endpoint_ip,
      [keydb_server.DEFAULT_PORT],
  )
  keydb_metadata = keydb_server.GetMetadata(server_vm)

  for server_result in raw_results:
    server_result.metadata.update(keydb_metadata)
    server_result.metadata.update(benchmark_metadata)

  return raw_results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  del bm_spec
