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
"""Runs DPDK Pktgen benchmarks for high-performance networking.

DPDK Pktgen Benchmark is a more feature-rich DPDK benchmark than dpdk_testpmd.
DPDK bypasses the kernel networking stack, allowing for much higher PPS.

Benchmark Documentation:
https://pktgen-dpdk.readthedocs.io/en/latest/getting_started.html
https://toonk.io/building-a-high-performance-linux-based-traffic-generator-with-dpdk/index.html
"""

import copy
from typing import Any, Mapping

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import dpdk_pktgen


BENCHMARK_NAME = 'dpdk_pktgen'
BENCHMARK_CONFIG = """
dpdk_pktgen:
  description: Runs dpdk testpmd benchmarks
  vm_groups:
    vm_1:
      vm_spec: *default_dual_core
    vm_2:
      vm_spec: *default_dual_core
  flags:
    gce_subnet_name: default,dpdk0
    gce_nic_types: GVNIC,GVNIC
    gce_nic_queue_counts: default,default
    gce_network_type: custom
    ip_addresses: INTERNAL
"""

FLAGS = flags.FLAGS

_DPDK_PKTGEN_DURATION = flags.DEFINE_integer(
    'dpdk_pktgen_duration', 60, 'Run duration in seconds.'
)
_DPDK_PKTGEN_PACKET_LOSS_THRESHOLDS = flags.DEFINE_multi_float(
    'dpdk_pktgen_packet_loss_threshold_rates',
    [0],
    'Packet loss thresholds to record samples for.',
    lower_bound=0,
)
_DPDK_PKTGEN_NUM_FLOWS = flags.DEFINE_integer(
    'dpdk_pktgen_num_flows',
    200,
    'Number of flows to use by taking a range of source ports.',
    lower_bound=0,
)
_DPDK_PKTGEN_TX_RX_LCORES_LIST = flags.DEFINE_list(
    'dpdk_pktgen_tx_rx_lcores_list',
    [
        '[8:1],[1:8]',
        '[8:1-2],[1-2:8]',
        '[8:1-4],[1-4:8]',
        '[8:1-7],[1-7:8]',
    ],
    'A list of strings designating logical cores assigned to TX and RX. Each'
    ' string is in the form'
    '"[<sender_rx>:<sender_tx>],[<receiver_rx>:<receiver_tx>]".',
)
_DPDK_PKTGEN_NUM_MEMORY_CHANNELS_PER_NUMA = flags.DEFINE_integer(
    'dpdk_pktgen_num_memory_channels_per_numa',
    1,
    'Number of memory channels per NUMA node.',
)
_DPDK_PKTGEN_TX_BURST = flags.DEFINE_integer(
    'dpdk_pktgen_tx_burst', 1, 'The TX burst size.'
)
_DPDK_PKTGEN_RX_BURST = flags.DEFINE_integer(
    'dpdk_pktgen_rx_burst', 1, 'The RX burst size.'
)
_DPDK_PKTGEN_TXD = flags.DEFINE_integer(
    'dpdk_pktgen_txd', 8192, 'The size of the TX descriptor ring size.'
)
_DPDK_PKTGEN_RXD = flags.DEFINE_integer(
    'dpdk_pktgen_rxd', 8192, 'The size of the RX descriptor ring size.'
)

# DPDK Pktgen maximum logical cores
_MAX_LCORES = 128
_START_RATE = 100000000
# Percent difference in PPS between consecutive iterations to terminate binary
# search.
_PPS_BINARY_SEARCH_THRESHOLD = 0.01
_STDOUT_LOG_FILE = 'pktgen_stdout.log'


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
  """Prepares both VM's to run DPDK Pktgen.

  Args:
    benchmark_spec: The benchmark specification.
  """
  sender_vm, receiver_vm = benchmark_spec.vms[:2]
  background_tasks.RunThreaded(
      lambda vm: vm.Install('dpdk_pktgen'),
      [sender_vm, receiver_vm],
  )
  background_tasks.RunThreaded(
      lambda vm: PrepareVM(
          vm,
          sender_vm.internal_ips[1],
          sender_vm.secondary_mac_addr,
          receiver_vm.internal_ips[1],
          receiver_vm.secondary_mac_addr,
      ),
      [sender_vm, receiver_vm],
  )
  receiver_vm.RemoteCommand(
      'sudo sed -i "s/start all//g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  # Ensure receiver runs longer than sender. Receiver starts 1 second before
  # sender, so make receiver duration 2 seconds longer.
  receiver_vm.RemoteCommand(
      f'sudo sed -i "s/<DURATION>/{_DPDK_PKTGEN_DURATION.value+2}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  sender_vm.RemoteCommand(
      f'sudo sed -i "s/<DURATION>/{_DPDK_PKTGEN_DURATION.value}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )


def PrepareVM(
    vm: linux_virtual_machine.BaseLinuxVirtualMachine,
    sender_vm_ip: str,
    sender_vm_mac: str,
    receiver_vm_ip: str,
    receiver_vm_mac: str,
) -> None:
  """Prepares a VM to run DPDK Pktgen."""
  vm.PushDataFile(
      'dpdk_pktgen/pktgen.pkt',
      f'{dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt',
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<SRC_IP>/{sender_vm_ip}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<SRC_MAC>/{sender_vm_mac}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  # Updating src port numbers for multiple flows.
  vm.RemoteCommand(
      'sudo sed -i "s/1234 1234 1234 0/1234 1234'
      f' {1233+_DPDK_PKTGEN_NUM_FLOWS.value} 1/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<DST_IP>/{receiver_vm_ip}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<DST_MAC>/{receiver_vm_mac}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  # Updating dst port numbers for multiple flows.
  vm.RemoteCommand(
      'sudo sed -i "s/5678 5678 5678 0/5678 5678'
      f' {5677+_DPDK_PKTGEN_NUM_FLOWS.value} 1/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  # Update burst sizes.
  vm.RemoteCommand(
      f'sudo sed -i "s/<TX_BURST>/{_DPDK_PKTGEN_TX_BURST.value}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<RX_BURST>/{_DPDK_PKTGEN_RX_BURST.value}/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )


def IssueCommand(vm: linux_virtual_machine.BaseLinuxVirtualMachine, cmd: str):
  # Running Pktgen requires a terminal.
  vm.RemoteCommand(cmd, login_shell=True, disable_tty_lock=True)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs DPDK benchmarks across multiple core configurations.

  This function iterates through various combinations of Tx and Rx core settings
  to find the optimal configuration that maximizes the packet rate for different
  packet loss thresholds.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects containing the performance results for the
    best-performing core configuration for each packet loss threshold.

  Raises:
    RunError: A run-stage error raised by an individual benchmark.
  """
  sender_vm, receiver_vm = benchmark_spec.vms[:2]
  samples = []

  num_lcores = min(sender_vm.NumCpusForBenchmark(), _MAX_LCORES)
  num_numa, _ = sender_vm.RemoteCommand(
      "lscpu | grep 'NUMA node(s)' | awk '{print $3}'"
  )
  num_memory_channels = (
      int(num_numa) * _DPDK_PKTGEN_NUM_MEMORY_CHANNELS_PER_NUMA.value
  )

  # Base metadata applicable to all runs
  base_metadata = {
      'dpdk_pktgen_tx_burst': _DPDK_PKTGEN_TX_BURST.value,
      'dpdk_pktgen_rx_burst': _DPDK_PKTGEN_RX_BURST.value,
      'dpdk_pktgen_lcores': num_lcores,
      'dpdk_pktgen_num_memory_channels': num_memory_channels,
      'dpdk_pktgen_duration': _DPDK_PKTGEN_DURATION.value,
      'dpdk_pktgen_num_flows': _DPDK_PKTGEN_NUM_FLOWS.value,
      'dpdk_pktgen_txd': _DPDK_PKTGEN_TXD.value,
      'dpdk_pktgen_rxd': _DPDK_PKTGEN_RXD.value,
      'dpdk_pktgen_mbuf_cache_sizes': (
          dpdk_pktgen.DPDK_PKTGEN_MBUF_CACHE_SIZE.value
      ),
      'dpdk_pktgen_mbufs_per_port_multiplier': (
          dpdk_pktgen.DPDK_PKTGEN_MBUFS_PER_PORT_MULTIPLIER.value
      ),
  }
  pktgen_env_var = ''
  # Incorrect llq_policy default:
  # https://github.com/amzn/amzn-drivers/issues/331
  aws_eal_arg = ''
  if sender_vm.CLOUD == 'AWS':
    pktgen_env_var = ' LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64'
    aws_eal_arg = f' -a "{receiver_vm.secondary_nic_bus_info},llq_policy=1"'

  def _FindMaxRateFromConfig(
      tx_cmd: str, rx_cmd: str, packet_loss_threshold: float, start_rate: float
  ) -> tuple[int | None, int | None, int | None, float]:
    """Runs a binary search to find the max PPS for a given configuration."""
    valid_total_sender_tx_pkts = None
    valid_total_sender_rx_pkts = None
    valid_total_receiver_rx_pkts = None
    valid_packet_loss_rate = 1

    prev_pps, curr_pps = -float('inf'), 0
    curr_rate = None
    lb, ub = 0, start_rate * 2
    prev_rate = start_rate

    while (
        (abs(curr_pps - prev_pps) / (curr_pps + 1))
        > _PPS_BINARY_SEARCH_THRESHOLD
    ) or (valid_total_receiver_rx_pkts is None):
      curr_rate = (lb + ub) / 2
      sender_vm.RemoteCommand(
          f'sudo sed -i "s/pps        = {prev_rate};/pps        ='
          f' {curr_rate};/g"'
          f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/app/pktgen.c'
      )
      sender_vm.RemoteCommand(
          f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} && make'
      )
      background_tasks.RunParallelThreads(
          [
              (IssueCommand, [receiver_vm, rx_cmd], {}),
              (IssueCommand, [sender_vm, tx_cmd], {}),
          ],
          post_task_delay=1,  # Ensure receiver starts before sender.
          max_concurrency=2,
      )

      # Parse ANSI codes from pktgen output to get packet counts.
      stdout_rx_parser = (
          f'cat {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/{_STDOUT_LOG_FILE} |'
          r' grep -oP "\[7;22H\s*\K[0-9]+" | tail -1'
      )
      stdout_tx_parser = (
          f'cat {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/{_STDOUT_LOG_FILE} |'
          r' grep -oP "\[8;22H\s*\K[0-9]+" | tail -1'
      )
      total_sender_tx_pkts, _ = sender_vm.RemoteCommand(stdout_tx_parser)
      total_sender_rx_pkts, _ = sender_vm.RemoteCommand(stdout_rx_parser)
      total_receiver_rx_pkts, _ = receiver_vm.RemoteCommand(stdout_rx_parser)

      if not all([
          total_sender_tx_pkts,
          total_sender_rx_pkts,
          total_receiver_rx_pkts,
      ]):
        # Failed run, treat as 100% loss and narrow the search space.
        ub = curr_rate
        prev_rate = curr_rate
        continue

      packet_loss_rate = (
          int(total_sender_tx_pkts)
          + int(total_sender_rx_pkts)
          - int(total_receiver_rx_pkts)
      ) / int(total_sender_tx_pkts)
      if packet_loss_rate > packet_loss_threshold:
        ub = curr_rate
      else:
        # This is a valid run, save the results.
        valid_total_sender_tx_pkts = total_sender_tx_pkts
        valid_total_sender_rx_pkts = total_sender_rx_pkts
        valid_total_receiver_rx_pkts = total_receiver_rx_pkts
        valid_packet_loss_rate = packet_loss_rate
        lb = curr_rate
      prev_pps, curr_pps = (
          curr_pps,
          int(total_receiver_rx_pkts) // _DPDK_PKTGEN_DURATION.value,
      )
      prev_rate = curr_rate

    # Reset PPS target in app/pktgen.c so sed command can work on next
    # function invocation.
    if curr_rate:
      sender_vm.RemoteCommand(
          f'sudo sed -i "s/pps        = {curr_rate};/pps        ='
          f' {start_rate};/g"'
          f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/app/pktgen.c'
      )
      sender_vm.RemoteCommand(
          f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} && make'
      )
    return (
        valid_total_sender_tx_pkts,
        valid_total_sender_rx_pkts,
        valid_total_receiver_rx_pkts,
        valid_packet_loss_rate,
    )

  prev_rate = _START_RATE
  for packet_loss_threshold in _DPDK_PKTGEN_PACKET_LOSS_THRESHOLDS.value:
    best_run_results = {}
    max_receiver_pkts = -1

    # Iterate over all combinations of Tx and Rx lcore configurations
    for tx_rx_lcores in _DPDK_PKTGEN_TX_RX_LCORES_LIST.value:
      tx_cores, rx_cores = tx_rx_lcores.split(',')

      # Build pktgen commands for the current core configuration
      tx_cmd = (
          f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} &&'
          f' sudo{pktgen_env_var} ./usr/local/bin/pktgen -l 0-{num_lcores-1}'
          f' -n {num_memory_channels}{aws_eal_arg} --'
          f' --txd={_DPDK_PKTGEN_TXD.value}'
          f' --rxd={_DPDK_PKTGEN_RXD.value} -m "{tx_cores}.0"'
          ' -f pktgen.pkt'
          f' > {_STDOUT_LOG_FILE}'
      )
      rx_cmd = (
          f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} &&'
          f' sudo{pktgen_env_var} ./usr/local/bin/pktgen -l 0-{num_lcores-1}'
          f' -n {num_memory_channels}{aws_eal_arg} --'
          f' --txd={_DPDK_PKTGEN_TXD.value}'
          f' --rxd={_DPDK_PKTGEN_RXD.value} -m "{rx_cores}.0"'
          ' -f pktgen.pkt'
          f' > {_STDOUT_LOG_FILE}'
      )
      # Find the max rate for this specific core configuration
      s_tx, s_rx, r_rx, loss = _FindMaxRateFromConfig(
          tx_cmd, rx_cmd, packet_loss_threshold, prev_rate
      )
      if r_rx and int(r_rx) > max_receiver_pkts:
        max_receiver_pkts = int(r_rx)
        # Store the metadata and results of this better-performing run
        current_metadata = copy.deepcopy(base_metadata)
        current_metadata.update({
            'dpdk_pktgen_packet_loss_threshold': packet_loss_threshold,
            'dpdk_pktgen_tx_cores': tx_cores,
            'dpdk_pktgen_rx_cores': rx_cores,
        })
        best_run_results = {
            'valid_total_sender_tx_pkts': s_tx,
            'valid_total_sender_rx_pkts': s_rx,
            'valid_total_receiver_rx_pkts': r_rx,
            'valid_packet_loss_rate': loss,
            'metadata': current_metadata,
        }

    # After checking all core combinations, if a valid run was found,
    # create samples for the best performing one.
    if best_run_results:
      duration = _DPDK_PKTGEN_DURATION.value
      s_tx_pkts = int(best_run_results['valid_total_sender_tx_pkts'])
      s_rx_pkts = int(best_run_results['valid_total_sender_rx_pkts'])
      r_rx_pkts = int(best_run_results['valid_total_receiver_rx_pkts'])
      meta = best_run_results['metadata']

      samples.extend([
          sample.Sample('Total sender tx packets', s_tx_pkts, 'packets', meta),
          sample.Sample(
              'Total sender tx pps', s_tx_pkts // duration, 'packets/s', meta
          ),
          sample.Sample('Total sender rx packets', s_rx_pkts, 'packets', meta),
          sample.Sample(
              'Total sender rx pps', s_rx_pkts // duration, 'packets/s', meta
          ),
          sample.Sample(
              'Total receiver rx packets', r_rx_pkts, 'packets', meta
          ),
          sample.Sample(
              'Total receiver rx pps', r_rx_pkts // duration, 'packets/s', meta
          ),
          sample.Sample(
              'packet loss rate',
              best_run_results['valid_packet_loss_rate'],
              'rate (1=100%)',
              meta,
          ),
      ])

  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec
