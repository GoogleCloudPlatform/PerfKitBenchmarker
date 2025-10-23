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
"""Records the time required to boot a cluster of VMs.

This benchmark collects several provisioning time metrics, some of which are
conditional based on the type of VMs that the benchmark is being run on.

The metrics that are recorded are captured along the following timeline
and under the following conditions:

create_start_time recorded: this is the time that all metrics are measured
  against. Every metric mentioned below involves capturing a timestamp and
  calculating the difference between it and create_start_time.
create command invoked: cloud-specific VM instance create command is invoked
  immediately following the recording of create_start_time. Some clouds
  invoke a synchronous command, where the command waits until the instance
  is created before returning to PKB. Meanwhile, other clouds invoke
  their create command asynchronously, where the command returns to PKB
  immediately and instance creation is verified through a separate process.

The metrics and steps below apply only to asynchronous creates:
- Metric: time-to-create-async-return
  create_async_return_time recorded:
    The timestamp is captured immediately after an asynchronous create command
    returns to PKB.
- VM describe polling process:
    After the asynchronous create returns, the instance is polled via the use
    of a cloud-specific 'describe' command. The command runs in 1 second
    intervals and has its output parsed to see when the VM enters the
    cloud-specific 'running' state.
- Metric: time-to-running
  is_running_time recorded:
    This timestamp is captured once the polling process above determines that
    the VM is running.
Network reachability polling process:
  PKB uses a retryable WaitForSSH function that invokes a command to determine
  whether or not the VM is ready to respond to SSH commands.
Metric: time-to-ssh-internal
  ssh_internal_time recorded:
    This timestamp is captured once the VM responds to the network
    reachability polling command via its internal IP address.
Metric: time-to-ssh-external
  ssh_external_time recorded:
    This timestamp is captured once the VM responds to the network
    reachability polling command via its public IP address.
Metric: cluster-boot-time
  bootable_time recorded:
    This timestamp is captured once all times are captured. The maximum
    vm.bootable_time in a cluster of VMs is reported as the cluster boot time.
"""

import datetime
import logging
import os
import shlex
import signal
import socket
import subprocess
import time
from typing import List, Tuple

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import log_collector
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import linux_boot
import pytz

BENCHMARK_NAME = 'cluster_boot'
BENCHMARK_CONFIG = """
cluster_boot:
  description: >
      Create a cluster, record all times to boot.
      Specify the cluster size with --num_vms.
  vm_groups:
    default:
      vm_spec:
        AliCloud:
          machine_type: ecs.g5.large
          zone: us-east-1a
        AWS:
          machine_type: m5.large
          zone: us-east-1
        Azure:
          machine_type: Standard_D2s_v3
          zone: eastus
          boot_disk_type: StandardSSD_LRS
        # Boot disk can be overridden (e.g. --gce_boot_disk_type=pd-ssd)
        GCP:
          machine_type: n1-standard-2
          zone: us-central1-a
        IBMCloud:
          machine_type: cx2-2x4
          zone: us-south-1
        Kubernetes:
          image: null
        OpenStack:
          machine_type: t1.small
          zone: nova
      vm_count: null
  flags:
    # We don't want boot time samples to be affected from retrying, so don't
    # retry VM creation failures.
    retry_on_rate_limited: False
    retry_gce_subnetwork_not_ready: False
    # In case tcpdump is launched, always cleanup
    always_call_cleanup: True
"""

_BOOT_TIME_REBOOT = flags.DEFINE_boolean(
    'cluster_boot_time_reboot',
    False,
    'Whether to reboot the VMs during the cluster boot benchmark to measure '
    'reboot performance.',
)
_BOOT_TEST_PORT_LISTENING = flags.DEFINE_boolean(
    'cluster_boot_test_port_listening',
    False,
    'Test the time it takes to successfully connect to the port that is used '
    'to run the remote command.',
)
_LINUX_BOOT_METRICS = flags.DEFINE_boolean(
    'cluster_boot_linux_boot_metrics',
    False,
    'Collect detailed linux boot metrics.',
)
_CALLBACK_INTERNAL = flags.DEFINE_string(
    'cluster_boot_callback_internal_ip',
    '',
    'Internal ip address to use for collecting first egress/ingress packet, '
    'requires installation of tcpdump on the runner and tcpdump port is '
    'reachable from the VMs.',
)
_CALLBACK_EXTERNAL = flags.DEFINE_string(
    'cluster_boot_callback_external_ip',
    '',
    'External ip address to use for collecting first egress/ingress packet, '
    'requires installation of tcpdump on the runner and tcpdump port is '
    'reachable from the VMs.',
)
_TCPDUMP_PORT = flags.DEFINE_integer(
    'cluster_boot_tcpdump_port',
    0,
    'Port the runner uses to test VM network connectivity. By default, pick '
    'a random unused port.',
)
_POST_BOOT_LATENCY_TEST_COMMAND = flags.DEFINE_string(
    'cluster_boot_post_boot_latency_test_command',
    None,
    'Single command to run after a VM has booted that will have its latency '
    'tested and published as a sample.'
)
FLAGS = flags.FLAGS


def GetCallbackIPs() -> Tuple[str, str]:
  """Get internal/external IP of runner."""
  return (_CALLBACK_INTERNAL.value, _CALLBACK_EXTERNAL.value)


def CollectNetworkSamples() -> bool:
  """Whether or not network samples should be collected."""
  return bool(_CALLBACK_EXTERNAL.value or _CALLBACK_INTERNAL.value)


def PrepareStartupScript() -> Tuple[str, int | None, str]:
  """Prepare startup script which will be ran as part of VM booting process."""
  port = _TCPDUMP_PORT.value
  if CollectNetworkSamples():
    if not port:
      # If not specified, find a free port and close so it can be used by
      # tcpdump.
      sock = socket.socket()
      sock.bind(('', 0))
      port = sock.getsockname()[1]
      sock.close()
    tcpdump_output_path = vm_util.PrependTempDir(linux_boot.TCPDUMP_OUTPUT)
    tcpdump_output = open(tcpdump_output_path, 'w')
    tcpdump_cmd = subprocess.Popen(
        shlex.split(f'tcpdump -tt -n -l tcp dst port {port}'),
        stdout=tcpdump_output,
        stderr=tcpdump_output,
    )
    pid = tcpdump_cmd.pid
    logging.info('Starting tcpdump process %s', pid)
  else:
    pid = None
    tcpdump_output_path = linux_boot.TCPDUMP_OUTPUT

  startup_script = linux_boot.PrepareBootScriptVM(
      ' '.join(GetCallbackIPs()), port
  )
  startup_script_path = vm_util.PrependTempDir(linux_boot.BOOT_STARTUP_SCRIPT)
  with open(startup_script_path, 'w') as f:
    f.write(startup_script)

  return startup_script_path, pid, tcpdump_output_path


def GetConfig(user_config):
  benchmark_config = configs.LoadConfig(
      BENCHMARK_CONFIG, user_config, BENCHMARK_NAME
  )
  if _LINUX_BOOT_METRICS.value or CollectNetworkSamples():
    startup_script_path, pid, output_path = PrepareStartupScript()
    benchmark_config['flags']['boot_startup_script'] = startup_script_path
    benchmark_config['temporary'] = {
        'tcpdump_pid': pid,
        'tcpdump_output_path': output_path,
    }
  return benchmark_config


def Prepare(unused_benchmark_spec):
  pass


def GetTimeToBoot(vms):
  """Creates Samples for the boot time of a list of VMs.

  The time to create async return is the time difference from before the VM is
  created to when the asynchronous create call returns.

  The time to running is the time difference from before the VM is created to
  when the VM is in the 'running' state as determined by the response to a
  'describe' command.

  The boot time is the time difference from before the VM is created to when
  the VM is responsive to SSH commands.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing each of the provisioning metrics listed above,
    along with an overall cluster boot time.
  """
  if not vms:
    return []

  # Time that metrics are measured against.
  min_create_start_time = min(vm.create_start_time for vm in vms)

  # Vars used to store max values for whole-cluster boot metrics.
  max_create_delay_sec = 0
  max_boot_time_sec = 0
  max_port_listening_time_sec = 0
  max_rdp_port_listening_time_sec = 0

  samples = []
  os_types = set()
  for i, vm in enumerate(vms):
    assert vm.create_start_time
    assert vm.bootable_time
    assert vm.bootable_time >= vm.create_start_time

    os_types.add(vm.OS_TYPE)
    create_delay_sec = vm.create_start_time - min_create_start_time
    max_create_delay_sec = max(max_create_delay_sec, create_delay_sec)
    metadata = {
        'machine_instance': i,
        'num_vms': len(vms),
        'os_type': vm.OS_TYPE,
        'create_delay_sec': '%0.1f' % create_delay_sec,
    }

    # TIME TO CREATE ASYNC RETURN
    if vm.create_return_time:
      time_to_create_sec = vm.create_return_time - min_create_start_time
      samples.append(
          sample.Sample(
              'Time to Create Async Return',
              time_to_create_sec,
              'seconds',
              metadata,
          )
      )

    # TIME TO RUNNING
    if vm.is_running_time:
      time_to_running_sec = vm.is_running_time - vm.create_start_time
      samples.append(
          sample.Sample(
              'Time to Running', time_to_running_sec, 'seconds', metadata
          )
      )

    # TIME TO SSH
    boot_time_sec = vm.bootable_time - min_create_start_time
    if isinstance(vm, linux_virtual_machine.BaseLinuxMixin):
      # TODO(pclay): Remove when Windows refactor below is complete.
      if vm.ssh_external_time:
        samples.append(
            sample.Sample(
                'Time to SSH - External',
                vm.ssh_external_time - min_create_start_time,
                'seconds',
                metadata,
            )
        )
      if vm.ssh_internal_time:
        samples.append(
            sample.Sample(
                'Time to SSH - Internal',
                vm.ssh_internal_time - min_create_start_time,
                'seconds',
                metadata,
            )
        )

    # TIME TO PORT LISTENING
    max_boot_time_sec = max(max_boot_time_sec, boot_time_sec)
    samples.append(
        sample.Sample('Boot Time', boot_time_sec, 'seconds', metadata)
    )
    if _BOOT_TEST_PORT_LISTENING.value:
      assert vm.port_listening_time
      assert vm.port_listening_time >= vm.create_start_time
      port_listening_time_sec = vm.port_listening_time - min_create_start_time
      max_port_listening_time_sec = max(
          max_port_listening_time_sec, port_listening_time_sec
      )
      samples.append(
          sample.Sample(
              'Port Listening Time',
              port_listening_time_sec,
              'seconds',
              metadata,
          )
      )

    # TIME TO RDP LISTENING
    # TODO(pclay): refactor so Windows specifics aren't in linux_benchmarks
    if FLAGS.cluster_boot_test_rdp_port_listening:
      assert vm.rdp_port_listening_time
      assert vm.rdp_port_listening_time >= vm.create_start_time
      rdp_port_listening_time_sec = (
          vm.rdp_port_listening_time - min_create_start_time
      )
      max_rdp_port_listening_time_sec = max(
          max_rdp_port_listening_time_sec, rdp_port_listening_time_sec
      )
      samples.append(
          sample.Sample(
              'RDP Port Listening Time',
              rdp_port_listening_time_sec,
              'seconds',
              metadata,
          )
      )
    # Host Create Latency
    if FLAGS.dedicated_hosts:
      assert vm.host
      assert vm.host.create_start_time
      assert vm.host.create_start_time < vm.create_start_time
      host_create_latency_sec = (
          vm.create_start_time - vm.host.create_start_time
      )
      samples.append(
          sample.Sample(
              'Host Create Latency',
              host_create_latency_sec,
              'seconds',
              metadata,
          )
      )

  # Add a total cluster boot sample as the maximum boot time.
  metadata = {
      'num_vms': len(vms),
      'os_type': ','.join(sorted(os_types)),
      'max_create_delay_sec': '%0.1f' % max_create_delay_sec,
  }
  samples.append(
      sample.Sample('Cluster Boot Time', max_boot_time_sec, 'seconds', metadata)
  )
  if _BOOT_TEST_PORT_LISTENING.value:
    samples.append(
        sample.Sample(
            'Cluster Port Listening Time',
            max_port_listening_time_sec,
            'seconds',
            metadata,
        )
    )
  if FLAGS.cluster_boot_test_rdp_port_listening:
    samples.append(
        sample.Sample(
            'Cluster RDP Port Listening Time',
            max_rdp_port_listening_time_sec,
            'seconds',
            metadata,
        )
    )
  if max_create_delay_sec > 1:
    logging.warning(
        'The maximum delay between starting VM creations is %0.1fs.',
        max_create_delay_sec,
    )

  return samples


def _MeasureReboot(vms):
  """Measures the time to reboot the cluster of VMs.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the reboot times and an overall cluster reboot
    time.
  """
  before_reboot_timestamp = time.time()
  reboot_times = background_tasks.RunThreaded(lambda vm: vm.Reboot(), vms)
  cluster_reboot_time = time.time() - before_reboot_timestamp
  return _GetVmOperationDataSamples(
      reboot_times, cluster_reboot_time, 'Reboot', vms
  )


def MeasureDelete(
    vms: List[virtual_machine.BaseVirtualMachine],
) -> List[sample.Sample]:
  """Measures the time to delete the cluster of VMs.

  Args:
    vms: List of BaseVirtualMachine subclasses.

  Returns:
    List of Samples containing the delete times and an overall cluster delete
    time.
  """
  # Only measure VMs that have a delete time.
  vms = [vm for vm in vms if vm.delete_start_time and vm.delete_end_time]
  if not vms:
    return []
  # Collect a delete time from each VM.
  delete_times = [vm.delete_end_time - vm.delete_start_time for vm in vms]
  # Get the cluster delete time.
  min_delete_start_time = min([vm.delete_start_time for vm in vms])
  max_delete_end_time = max([vm.delete_end_time for vm in vms])
  cluster_delete_time = max_delete_end_time - min_delete_start_time
  # Record the delete metrics as samples.
  return _GetVmOperationDataSamples(
      delete_times, cluster_delete_time, 'Delete', vms
  )


def _GetVmOperationDataSamples(
    operation_times: List[int],
    cluster_time: float,
    operation: str,
    vms: List[virtual_machine.BaseVirtualMachine],
) -> List[sample.Sample]:
  """Append samples from given data.

  Args:
    operation_times: The list of times for each vms.
    cluster_time: The cluster time for the benchmark.
    operation: The benchmark operation being run, capitalized with no spaces.
    vms: list of virtual machines.

  Returns:
    List of samples constructed from data.
  """
  samples = []
  metadata_list = []
  for i, vm in enumerate(vms):
    metadata = {
        'machine_instance': i,
        'num_vms': len(vms),
        'os_type': vm.OS_TYPE,
    }
    metadata_list.append(metadata)
  for operation_time, metadata in zip(operation_times, metadata_list):
    samples.append(
        sample.Sample(f'{operation} Time', operation_time, 'seconds', metadata)
    )
  os_types = {vm.OS_TYPE for vm in vms}
  metadata = {'num_vms': len(vms), 'os_type': ','.join(sorted(os_types))}
  samples.append(
      sample.Sample(
          f'Cluster {operation} Time', cluster_time, 'seconds', metadata
      )
  )
  return samples


def _RunPostBootLatencyTest(
    test_cmd: str, test_vm: virtual_machine.BaseVirtualMachine
) -> list[sample.Sample] | None:
  """Runs a test command on a VM and returns a sample of its latency.

  Args:
    test_cmd: the command to run
    test_vm: the VM to run the command on

  Returns:
    A sample of the latency of the command, or None if the command fails.
  """
  try:
    before_test_time = time.time()
    _, stderr, retcode = test_vm.RemoteCommandWithReturnCode(
        test_cmd, ignore_failure=True
    )
    after_test_time = time.time()
    if retcode != 0:
      logging.warning(
          'The test command returned a non-zero exit code: %s', stderr
      )
      return [sample.Sample(
          'Post Boot Command Failed',
          1,
          'count',
          {
              'test_command': test_cmd,
              'test_command_stderr': stderr[:1000],
          },
      )]
    else:
      return [
          sample.Sample(
              'Post Boot Command Latency',
              after_test_time - before_test_time,
              'seconds',
              {'test_command': test_cmd},
          ),
          sample.Sample(
              'Create to Post Boot Command',
              after_test_time - test_vm.create_start_time,
              'seconds',
              {'test_command': test_cmd},
          ),
      ]
  except errors.VirtualMachine.RemoteCommandError as e:
    logging.warning(
        'Unable to establish connection with VM; the test command was not'
        ' run: %s',
        e,
    )


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    An empty list (all boot samples will be added later).
  """
  samples = []
  if _POST_BOOT_LATENCY_TEST_COMMAND.value and benchmark_spec.vms:
    test_vm = benchmark_spec.vms[0]
    post_boot_samples = _RunPostBootLatencyTest(
        _POST_BOOT_LATENCY_TEST_COMMAND.value, test_vm)
    if post_boot_samples:
      samples += post_boot_samples
  if _LINUX_BOOT_METRICS.value or CollectNetworkSamples():
    for vm in benchmark_spec.vms:
      samples.extend(
          linux_boot.CollectBootSamples(
              vm,
              vm.bootable_time - vm.create_start_time,
              GetCallbackIPs(),
              datetime.datetime.fromtimestamp(
                  vm.create_start_time, pytz.timezone('UTC')
              ),
              include_networking_samples=CollectNetworkSamples(),
          )
      )
  if _BOOT_TIME_REBOOT.value:
    samples.extend(_MeasureReboot(benchmark_spec.vms))

  return samples


def Cleanup(benchmark_spec):
  """Kill tcpdump process and upload and/or delete tcpdump output file.

  Args:
    benchmark_spec: The benchmark specification.
  """
  if CollectNetworkSamples():
    pid = benchmark_spec.config.temporary['tcpdump_pid']
    logging.info('Terminating tcpdump process %s', pid)
    try:
      os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
      logging.warning('tcpdump process %s ended prematurely.', pid)
    try:
      tcpdump_path = benchmark_spec.config.temporary['tcpdump_output_path']
      log_collector.CollectVMLogs(FLAGS.run_uri, tcpdump_path)
      os.remove(tcpdump_path)
    except FileNotFoundError:
      logging.warning(
          'tcpdump output file %s does not exist', linux_boot.TCPDUMP_OUTPUT
      )
