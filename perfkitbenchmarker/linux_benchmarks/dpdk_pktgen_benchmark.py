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

import re
from typing import Any, List, Mapping

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
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
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
    [0, 0.00001, 0.0001],
    'Packet loss thresholds to record samples for.',
    lower_bound=0,
)
_DPDK_PKTGEN_NUM_FLOWS = flags.DEFINE_integer(
    'dpdk_pktgen_num_flows',
    1,
    'Number of flows to use by taking a range of source ports.',
    lower_bound=0,
)

# DPDK Pktgen maximum logical cores
_MAX_LCORES = 128
# Starting packet transmission rate as a percentage.
_START_RATE = 0.001
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
      lambda vm: PrepareVM(
          vm,
          sender_vm.internal_ips[1],
          receiver_vm.internal_ips[1],
      ),
      [sender_vm, receiver_vm],
  )
  receiver_vm.RemoteCommand(
      'sudo sed -i "s/set all rate <RATE>//g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
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
    receiver_vm_ip: str,
) -> None:
  """Prepares a VM to run DPDK Pktgen."""
  vm.Install('dpdk_pktgen')
  vm.PushDataFile(
      'dpdk_pktgen/pktgen.pkt',
      f'{dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt',
  )
  vm.RemoteCommand(
      f'sudo sed -i "s/<SRC_IP>/{sender_vm_ip}/g"'
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
  # Updating dst port numbers for multiple flows.
  vm.RemoteCommand(
      'sudo sed -i "s/5678 5678 5678 0/5678 5678'
      f' {5677+_DPDK_PKTGEN_NUM_FLOWS.value} 1/g"'
      f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
  )


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
  num_memory_channels_stdout, _ = sender_vm.RemoteCommand(
      "lscpu | grep 'NUMA node(s)' | awk '{print $3}'"
  )
  num_memory_channels = int(num_memory_channels_stdout)
  metadata = {
      'dpdk_pkgen_burst': 1,
      'dpdk_pktgen_lcores': num_lcores,
      'dpdk_pktgen_num_memory_channels': num_memory_channels,
      'dpdk_pktgen_duration': _DPDK_PKTGEN_DURATION.value,
      'dpdk_pktgen_num_flows': _DPDK_PKTGEN_NUM_FLOWS.value,
  }
  cmd = (
      f'cd {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR} && sudo'
      f' ./usr/local/bin/pktgen -l 0-{num_lcores-1} -n {num_memory_channels} --'
      f' -m "[1-7].0" -f pktgen.pkt > {_STDOUT_LOG_FILE}'
  )

  prev_rate = '<RATE>'
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
        abs(curr_pps - prev_pps) / (curr_pps + 1)
    ) > _PPS_BINARY_SEARCH_THRESHOLD:
      curr_rate = (lb + ub) / 2
      sender_vm.RemoteCommand(
          f'sudo sed -i "s/{prev_rate}/{curr_rate}/g"'
          f' {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/pktgen.pkt'
      )

      # Running Pktgen requires a terminal.
      background_tasks.RunThreaded(
          lambda vm: vm.RemoteCommand(
              cmd, login_shell=True, disable_tty_lock=True
          ),
          [receiver_vm, sender_vm],
          post_task_delay=1,  # Ensure receiver starts before sender.
      )
      total_sender_tx_pkts, total_sender_rx_pkts, total_receiver_rx_pkts = (
          _ParseStdout(sender_vm, receiver_vm)
      )

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


def _ParseStdout(
    sender_vm: linux_virtual_machine.BaseLinuxVirtualMachine,
    receiver_vm: linux_virtual_machine.BaseLinuxVirtualMachine,
) -> List[str]:
  """Parse stdout log file to obtain packets sent and received.

  Args:
    sender_vm: The sender VM.
    receiver_vm: The receiver VM.

  Returns:
    A list of strings representing the total sender tx packets, total sender rx
    packets, and total receiver rx packets.
  """
  # Filter out ANSI escape codes from stdout.
  stdout_parser = (
      f'cat {dpdk_pktgen.DPDK_PKTGEN_GIT_REPO_DIR}/{_STDOUT_LOG_FILE} |'
      r' sed "s/\x1B\[[0-9;]*[a-zA-Z]//g"'
  )
  receiver_stdout, _ = receiver_vm.RemoteCommand(stdout_parser)
  sender_stdout, _ = sender_vm.RemoteCommand(stdout_parser)
  total_sender_rx_pkts, total_sender_tx_pkts = re.findall(
      r'Powered by DPDK.*', sender_stdout
  )[-1].split()[30:32]
  total_receiver_rx_pkts = re.findall(r'Powered by DPDK.*', receiver_stdout)[
      -1
  ].split()[30]

  return [total_sender_tx_pkts, total_sender_rx_pkts, total_receiver_rx_pkts]


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleanup benchmarks on the target vm.

  Args:
    benchmark_spec: The benchmark specification.
  """
  del benchmark_spec
