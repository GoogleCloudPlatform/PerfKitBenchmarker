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
    placement_group_style: closest_supported
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
_DPDK_PKTGEN_TX_CORES = flags.DEFINE_string(
    'dpdk_pktgen_tx_cores', '[1-7]', 'Cores assigned to TX.'
)
_DPDK_PKTGEN_RX_CORES = flags.DEFINE_string(
    'dpdk_pktgen_rx_cores', '[1-7]', 'Cores assigned to RX.'
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
    'dpdk_pktgen_txd', 2048, 'The size of the TX descriptor ring size.'
)
_DPDK_PKTGEN_RXD = flags.DEFINE_integer(
    'dpdk_pktgen_rxd', 2048, 'The size of the RX descriptor ring size.'
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
  """Runs DPDK benchmarks.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects with the performance results.

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
  metadata = {
      'dpdk_pktgen_tx_burst': _DPDK_PKTGEN_TX_BURST.value,
      'dpdk_pktgen_rx_burst': _DPDK_PKTGEN_RX_BURST.value,
      'dpdk_pktgen_lcores': num_lcores,
      'dpdk_pktgen_num_memory_channels': num_memory_channels,
      'dpdk_pktgen_duration': _DPDK_PKTGEN_DURATION.value,
      'dpdk_pktgen_num_flows': _DPDK_PKTGEN_NUM_FLOWS.value,
      'dpdk_pktgen_tx_cores': _DPDK_PKTGEN_TX_CORES.value,
      'dpdk_pktgen_rx_cores': _DPDK_PKTGEN_RX_CORES.value,
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

  tx_cmd = (
      f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} &&'
      f' sudo{pktgen_env_var} ./usr/local/bin/pktgen -l 0-{num_lcores-1} -n'
      f' {num_memory_channels}{aws_eal_arg} --'
      f' --txd={_DPDK_PKTGEN_TXD.value} --rxd={_DPDK_PKTGEN_RXD.value} -m'
      f' "{_DPDK_PKTGEN_TX_CORES.value}.0" -f pktgen.pkt >'
      f' {_STDOUT_LOG_FILE}'
  )
  rx_cmd = (
      f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} &&'
      f' sudo{pktgen_env_var} ./usr/local/bin/pktgen -l 0-{num_lcores-1} -n'
      f' {num_memory_channels}{aws_eal_arg} --'
      f' --txd={_DPDK_PKTGEN_TXD.value} --rxd={_DPDK_PKTGEN_RXD.value} -m'
      f' "{_DPDK_PKTGEN_RX_CORES.value}.0" -f pktgen.pkt >'
      f' {_STDOUT_LOG_FILE}'
  )

  prev_rate = _START_RATE
  for packet_loss_threshold in _DPDK_PKTGEN_PACKET_LOSS_THRESHOLDS.value:
    metadata = metadata.copy()
    metadata['dpdk_pktgen_packet_loss_threshold'] = packet_loss_threshold
    valid_total_sender_tx_pkts = None
    valid_total_sender_rx_pkts = None
    valid_total_receiver_rx_pkts = None
    valid_packet_loss_rate = 1
    # Binary search for max PPS under packet loss rate thresholds.
    prev_pps, curr_pps = -float('inf'), 0
    lb, ub = 0, _START_RATE * 2

    while (
        (abs(curr_pps - prev_pps) / (curr_pps + 1))
        > _PPS_BINARY_SEARCH_THRESHOLD
    ) or (not valid_total_receiver_rx_pkts):
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

      # Parse ANSI codes.
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

      packet_loss_rate = (
          int(total_sender_tx_pkts)
          + int(total_sender_rx_pkts)
          - int(total_receiver_rx_pkts)
      ) / int(total_sender_tx_pkts)
      if packet_loss_rate > packet_loss_threshold:
        ub = curr_rate
      else:
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

    samples.extend([
        sample.Sample(
            'Total sender tx packets',
            int(valid_total_sender_tx_pkts),
            'packets',
            metadata,
        ),
        sample.Sample(
            'Total sender tx pps',
            int(valid_total_sender_tx_pkts) // _DPDK_PKTGEN_DURATION.value,
            'packets/s',
            metadata,
        ),
        sample.Sample(
            'Total sender rx packets',
            int(valid_total_sender_rx_pkts),
            'packets',
            metadata,
        ),
        sample.Sample(
            'Total sender rx pps',
            int(valid_total_sender_rx_pkts) // _DPDK_PKTGEN_DURATION.value,
            'packets/s',
            metadata,
        ),
        sample.Sample(
            'Total receiver rx packets',
            int(valid_total_receiver_rx_pkts),
            'packets',
            metadata,
        ),
        sample.Sample(
            'Total receiver rx pps',
            int(valid_total_receiver_rx_pkts) // _DPDK_PKTGEN_DURATION.value,
            'packets/s',
            metadata,
        ),
        sample.Sample(
            'packet loss rate',
            valid_packet_loss_rate,
            'rate (1=100%)',
            metadata,
        ),
    ])

  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec
