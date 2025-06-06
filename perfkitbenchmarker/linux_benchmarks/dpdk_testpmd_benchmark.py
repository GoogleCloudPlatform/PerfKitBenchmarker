# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs DPDK Testpmd benchmarks for high-performance networking.

DPDK Testpmd Benchmark currently contains dpdk-testpmd (Poll-Mode Driver), which
measures packets-per-second (PPS) using DPDK. DPDK bypasses the kernel
networking stack, allowing for much higher PPS.

Benchmark Documentation:
http://doc.dpdk.org/guides-22.11/testpmd_app_ug/run_app.html#testpmd-command-line-options
"""

import re
from typing import Any, Mapping, Tuple

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import dpdk


BENCHMARK_NAME = 'dpdk_testpmd'
BENCHMARK_CONFIG = """
dpdk_testpmd:
  description: Runs dpdk testpmd benchmarks
  vm_groups:
    vm_1:
      vm_spec: *default_dual_core
    vm_2:
      vm_spec: *default_dual_core
  flags:
    placement_group_style: closest_supported
    gce_subnet_name: default,dpdk0
    gce_nic_types: GVNIC,GVNIC
    gce_nic_queue_counts: default,default
    gce_network_type: custom
    ip_addresses: INTERNAL
"""

FLAGS = flags.FLAGS

_DPDK_TEST_LENGTH = flags.DEFINE_integer(
    'dpdk_test_length',
    60,
    'Number of seconds to run test. Default value is 60s.',
    lower_bound=0,
)
_DPDK_FORWARD_MODE = flags.DEFINE_list(
    'dpdk_forward_mode',
    ['txonly', 'rxonly'],
    'Forwarding mode (i.e. io, flowgen, txonly, rxonly) for both VMs.',
)
_DPDK_TXPKTS = flags.DEFINE_integer(
    'dpdk_txpkts',
    64,
    (
        'Set TX segment sizes or total packet length. Valid for tx-only and'
        ' flowgen forwarding modes.'
    ),
    lower_bound=64,
)
_DPDK_TXQ = flags.DEFINE_integer(
    'dpdk_txq',
    2,
    (
        'The number of TX queues per port to N, where 1 <= N <= 65535. The'
        ' default value is 2.'
    ),
    lower_bound=1,
)
_DPDK_RXQ = flags.DEFINE_integer(
    'dpdk_rxq',
    2,
    (
        'The number of RX queues per port to N, where 1 <= N <= 65535. The'
        ' default value is 2.'
    ),
    lower_bound=1,
)
_DPDK_NB_CORES = flags.DEFINE_integer(
    'dpdk_nb_cores',
    2,
    (
        'The number of forwarding cores, where 1 <= N <= “number of cores” or'
        ' RTE_MAX_LCORE from the configuration file. The default value is 2.'
    ),
    lower_bound=1,
)
_DPDK_TXONLY_MULTI_FLOW = flags.DEFINE_bool(
    'dpdk_txonly_multi_flow', False, 'Generate multiple flows in txonly mode.'
)
_DPDK_BURST = flags.DEFINE_integer(
    'dpdk_burst',
    1,
    'The number of packets per burst. Default value is 1.',
    lower_bound=1,
    upper_bound=512,
)
flags.register_validator(
    'dpdk_forward_mode', lambda fwd_mode_: len(fwd_mode_) == 2
)


def GetConfig(user_config: Mapping[Any, Any]) -> Mapping[Any, Any]:
  """Merge BENCHMARK_CONFIG with user_config to create benchmark_spec.

  Args:
    user_config: user-defined configs (through FLAGS.benchmark_config_file or
      FLAGS.config_override).

  Returns:
    The resulting configs that come from merging user-defined configs with
    BENCHMARK_CONFIG.
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares both VM's to run DPDK.

  This include Hugepage allocation, dpdk installation, and binding the secondary
  NIC to a DPDK-compatible driver.

  Args:
    benchmark_spec: The benchmark specification.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]
  background_tasks.RunThreaded(
      lambda vm_: vm_.Install('dpdk'), [client_vm, server_vm]
  )


def _CreateRemoteCommand(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
    cmd: str,
    duration: int,
) -> Tuple[str, str]:
  interactive_cmd = (
      f"(sleep {duration} && echo 'stop' && cat) | (echo 'start' && cat) | "
  )
  return vm.RobustRemoteCommand(interactive_cmd + cmd, ignore_failure=True)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs DPDK benchmarks.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

  Raises:
    RunError: A run-stage error raised by an individual benchmark.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]

  metadata = {
      'dpdk_burst': _DPDK_BURST.value,
      'dpdk_num_forwarding_cores': _DPDK_NB_CORES.value,
      'dpdk_forward_mode': _DPDK_FORWARD_MODE.value,
      'dpdk_txpkts': _DPDK_TXPKTS.value,
      'dpdk_txq': _DPDK_TXQ.value,
      'dpdk_rxq': _DPDK_RXQ.value,
      'dpdk_test_length': _DPDK_TEST_LENGTH.value,
  }

  dpdk_cmd = 'sudo dpdk-testpmd'

  client_cmd = (
      dpdk_cmd
      + f' -a {client_vm.secondary_nic_bus_info}'
      f' -l 0-{_DPDK_NB_CORES.value} --'
      f' --forward-mode={_DPDK_FORWARD_MODE.value[0]}'
      f' --tx-ip={client_vm.internal_ips[1]},{server_vm.internal_ips[1]}'
      f' --txpkts={_DPDK_TXPKTS.value}'
      f' --txq={_DPDK_TXQ.value}'
      f' --rxq={_DPDK_RXQ.value}'
      f' --nb-cores={_DPDK_NB_CORES.value}'
      f' --burst={_DPDK_BURST.value}'
      ' -i'
  )
  if client_vm.CLOUD == 'AWS':
    client_cmd += f' --eth-peer=0,{server_vm.secondary_mac_addr}'

  if _DPDK_TXONLY_MULTI_FLOW.value:
    client_cmd += ' --txonly-multi-flow'

  server_cmd = (
      dpdk_cmd
      + f' -a {server_vm.secondary_nic_bus_info}'
      f' -l 0-{_DPDK_NB_CORES.value} --'
      f' --txq={_DPDK_TXQ.value}'
      f' --rxq={_DPDK_RXQ.value}'
      f' --nb-cores={_DPDK_NB_CORES.value}'
      f' --forward-mode={_DPDK_FORWARD_MODE.value[1]}'
      f' --burst={_DPDK_BURST.value}'
      ' -i'
  )

  remote_stdout_stderrs = background_tasks.RunParallelThreads(
      [
          (
              lambda vm_: _CreateRemoteCommand(
                  vm_,
                  server_cmd,
                  _DPDK_TEST_LENGTH.value
                  + 10,  # Ensure server stops after client.
              ),
              [server_vm],
              {},
          ),
          (
              lambda vm_: _CreateRemoteCommand(
                  vm_, client_cmd, _DPDK_TEST_LENGTH.value
              ),
              [client_vm],
              {},
          ),
      ],
      max_concurrency=2,
      post_task_delay=5  # Ensure server starts before client.
  )

  output_samples = []
  server_stdout = remote_stdout_stderrs[0][0]
  client_stdout = remote_stdout_stderrs[1][0]

  tx_packet_last_match = re.findall(
      r'(TX-packets):[\s]+([0-9]+)[\s]+(TX-dropped):[\s]+([0-9]+)[\s]+',
      client_stdout,
  )[-1]
  rx_packet_last_match = re.findall(
      r'(RX-packets):[\s]+([0-9]+)[\s]+(RX-dropped):[\s]+([0-9]+)[\s]+',
      server_stdout,
  )[-1]

  for match in [
      tx_packet_last_match,
      rx_packet_last_match,
  ]:
    if not match:
      raise errors.Benchmarks.RunError('Failed to parse output.')
    (
        packets_label,
        packets_val,
        dropped_label,
        dropped_val,
    ) = match
    output_samples.extend([
        sample.Sample(packets_label, int(packets_val), 'packets', metadata),
        sample.Sample(
            packets_label + '-per-second',
            int(packets_val) // _DPDK_TEST_LENGTH.value,
            'packets/s',
            metadata,
        ),
        sample.Sample(dropped_label, int(dropped_val), 'dropped', metadata),
    ])
  tx_pps = None
  rx_pps = None
  for output_sample in output_samples:
    if output_sample.metric == 'TX-packets-per-second':
      tx_pps = output_sample.value
    elif output_sample.metric == 'RX-packets-per-second':
      rx_pps = output_sample.value
  if tx_pps is None or rx_pps is None:
    raise errors.Benchmarks.RunError('TX or RX packets-per-second not found.')
  output_samples.extend([
      sample.Sample(
          'packet-loss-per-second', tx_pps - rx_pps, 'packets/s', metadata
      ),
      sample.Sample(
          'packet-loss-rate', (tx_pps - rx_pps) / tx_pps, 'rate', metadata
      ),
  ])

  return output_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  client_vm, server_vm = benchmark_spec.vms[:2]
  dpdk_git_repo_dir = _GetDirFromRepo(dpdk.DPDK_GIT_REPO)
  dpdk_driver_git_repo_dir = _GetDirFromRepo(dpdk.DPDK_GCP_DRIVER_GIT_REPO)
  background_tasks.RunThreaded(
      lambda vm_: vm_.RemoteCommand(f'rm -rf {dpdk_git_repo_dir}'),
      [client_vm, server_vm],
  )
  background_tasks.RunThreaded(
      lambda vm_: vm_.RemoteCommand(f'rm -rf {dpdk_driver_git_repo_dir}'),
      [client_vm, server_vm],
  )


def _GetDirFromRepo(repo: str) -> str:
  return repo.split('/')[-1].strip('.git')
