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
"""SCP files between VMs."""

import logging
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_integer('scp_file_size_gb', 1,
                     'Size of the file to be created for SCP transfer in GB.')

BENCHMARK_NAME = 'scp'
BENCHMARK_CONFIG = """
scp:
  description: Runs SCP benchmark.
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    vm_2:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Returns the configuration of a benchmark."""
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def GenerateSSHKey(vm):
  """Generates a new SSH key pair on the VM."""
  vm.RemoteCommand('ssh-keygen -t rsa -N "" -f ~/.ssh/scp_benchmark_key')
  public_key, _ = vm.RemoteCommand('cat ~/.ssh/scp_benchmark_key.pub')
  return public_key.strip()


def GenerateAndExchangeSSHKeys(vm1, vm2):
  """Exchanges SSH keys between two VMs."""
  public_key1 = GenerateSSHKey(vm1)
  public_key2 = GenerateSSHKey(vm2)
  vm1.RemoteCommand(f'echo "{public_key2}" >> ~/.ssh/authorized_keys')
  vm2.RemoteCommand(f'echo "{public_key1}" >> ~/.ssh/authorized_keys')


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec):
  """Prepares the VMs and files to be transferred."""
  logging.info('Preparing SCP benchmark')

  vms = benchmark_spec.vms
  if len(vms) != 2:
    raise ValueError(
        f'scp benchmark requires exactly two machines, found {len(vms)}'
    )

  # Backup current states
  for vm in vms:
    vm.RemoteCommand('sysctl -a > /tmp/original_sysctl.conf')

  # Generate and exchange SSH keys
  GenerateAndExchangeSSHKeys(vms[0], vms[1])
  
  # Create a payload file on both VM
  try:
    background_tasks.RunThreaded(PrepareFileForTransfer, vms)
  except Exception as e:
    logging.error('Error during file preparation: %s', str(e))


def PrepareFileForTransfer(vm):
  """Creates a test file of specified size on the VM."""
  logging.info('Preparing the test file')

  # Create directory with appropriate permissions
  vm.RemoteCommand('sudo mkdir -p /data_for_transfer && sudo chmod 777 /data_for_transfer')
  # Create a random file of specified size
  file_size_gb = FLAGS.scp_file_size_gb
  block_size = '50M'          # 50MiB per block
  count = file_size_gb * 20   # 1GB = 1000MiB = 20 blocks
  vm.RemoteCommand(
    f'dd if=/dev/urandom of=/data_for_transfer/payload.img bs={block_size} count={count} status=progress'
  )


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the benchmark and returns a dict of performance data."""
  logging.info('Running SCP benchmark')

  vms = benchmark_spec.vms
  vm1, vm2 = vms[0], vms[1]
  results = []

  def TransferFile(vm_pair):
    source_vm, dest_vm = vm_pair
    # clear any existing received file from previous runs
    dest_vm.RemoteCommand('rm -f /data_for_transfer/received.img')

    scp_cmd = (f'scp -o StrictHostKeyChecking=no -i ~/.ssh/scp_benchmark_key '
               f'/data_for_transfer/payload.img '
               f'{dest_vm.user_name}@{dest_vm.internal_ip}:/data_for_transfer/received.img')

    start_time = time.time()
    source_vm.RemoteCommand(scp_cmd)
    end_time = time.time()
    return end_time - start_time

  # Run TransferFile on both VMs
  thread_params = [
    (((vm1, vm2),), {}),  # VM1 to VM2
    (((vm2, vm1),), {})   # VM2 to VM1
  ]
  transfer_times = background_tasks.RunThreaded(TransferFile, thread_params)
  transfer_time_1, transfer_time_2 = transfer_times
  
  metadata = {
      'file_size_gb': FLAGS.scp_file_size_gb,
      'vm1_zone': vm1.zone,
      'vm2_zone': vm2.zone,
      'vm1_machine_type': vm1.machine_type,
      'vm2_machine_type': vm2.machine_type,
  }

  # Report individual transfer times
  results.append(sample.Sample(
      'SCP_Transfer_Time_VM1_to_VM2', transfer_time_1, 'seconds', metadata))
  results.append(sample.Sample(
      'SCP_Transfer_Time_VM2_to_VM1', transfer_time_2, 'seconds', metadata))

  return results


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up after SCP benchmark completes."""
  vms = benchmark_spec.vms
  
  for vm in vms:
    vm.RemoteCommand('rm -rf /data_for_transfer')
    vm.RemoteCommand('rm -f ~/.ssh/scp_benchmark_key ~/.ssh/scp_benchmark_key.pub')
    vm.RemoteCommand("sed -i '/scp_benchmark_key/d' ~/.ssh/authorized_keys")
    # restore original state in case
    vm.RemoteCommand('sudo sysctl -p /tmp/original_sysctl.conf')
    vm.RemoteCommand('rm /tmp/original_sysctl.conf')

  del benchmark_spec