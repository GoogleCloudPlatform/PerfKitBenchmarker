# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run large scale boot benchmark for virtual machines.

This benchmark measures the boot time for virtual machines. It is different from
the cluster_boot benchmark because this one scales better and is capable of
measuring boot time for a large number of machines.

The way it works is as follows:
1) benchmark spins up a variable number of launcher server VM(s) (num_vms flag).
2) launcher server VM(s) start up a server that listens for curl requests.
3) launcher server VM(s) record the system time as start time
4) launcher server VM(s) run a script to create N VMs per server.
5) VMs curl the launcher server as soon as they start up.
6) once launcher server VM(s) get a curl request, it use separate process to
   confirm connection.
7) launcher server VM(s) records the system time as end time for this VM.
8) launcher server VM(s) report the measurements
9) total provisioning time is that of the slowest VM.
10) VMs have startup scripts to shut themselves down after TIMEOUT seconds.
"""
import logging
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import windows_virtual_machine

from perfkitbenchmarker.providers.gcp import util as gcp_util


BENCHMARK_NAME = 'large_scale_boot'
BENCHMARK_CONFIG = """
large_scale_boot:
  description: >
      Create a cluster of launcher servers,
      where each launcher server launches FLAGS.boots_per_launcher machines.
  vm_groups:
    servers:
      vm_spec:
        GCP:
          machine_type: n1-standard-2
          zone: us-central1-a
          boot_disk_type: pd-ssd
      vm_count: 1
      os_type: debian9
    clients:
      vm_spec:
        GCP:
          machine_type: n1-standard-2
          boot_disk_type: pd-ssd
      os_type: debian9
      vm_count: 1
"""

FLAGS = flags.FLAGS
flags.DEFINE_integer('boots_per_launcher', 1, 'Number of VMs to boot per '
                     'launcher server VM. Defaults to 1.')
flags.register_validator('boots_per_launcher',
                         lambda value: 1 <= value <= 1000,
                         message='The number of VMs booted by each launcher '
                         'should be between 1 and 1000.')
flags.DEFINE_string('boot_os_type', 'debian9', 'OS to boot on the VMs. '
                    'Defaults to debian9. OS on launcher server VM is set '
                    'using os_type flag.')
flags.DEFINE_string('boot_machine_type', 'n1-standard-2', 'Machine type to boot'
                    'on the VMs. Defaults to n1-standard-2. Set machine type '
                    'on launcher server VM with launcher_machine_type flag.')
flags.DEFINE_string('launcher_machine_type', 'n1-standard-16', 'Machine type '
                    'to launcher the VMs. Defaults to n1-standard-16. Set '
                    'machine type on boot VMs with boot_machine_type flag.')
flags.DEFINE_boolean('vms_contact_launcher', True, 'Whether launched vms '
                     'attempt to contact the launcher before launcher attempts '
                     'to connect to them. Default to True.')

# remote tmp directory used for this benchmark.
_REMOTE_DIR = vm_util.VM_TMP_DIR
# boot script to use on the launcher server vms.
_BOOT_SCRIPT = 'boot_script.sh'
# local boot template to build boot script.
_BOOT_TEMPLATE = 'large_scale_boot/boot_script.sh.jinja2'
# Remote boot script path
_BOOT_PATH = posixpath.join(_REMOTE_DIR, _BOOT_SCRIPT)
# python listener server to run on launcher server vms.
_LISTENER_SERVER = 'large_scale_boot/listener_server.py'
# log for python listener server.
_LISTENER_SERVER_LOG = 'http.log'
# clean up script to use on the launcher server vms.
_CLEAN_UP_SCRIPT = 'clean_up.sh'
# local clean up template to build the clean up script
_CLEAN_UP_TEMPLATE = 'large_scale_boot/clean_up_script.jinja2'
# Remote clean up script path
_CLEAN_UP_SCRIPT_PATH = posixpath.join(_REMOTE_DIR, _CLEAN_UP_SCRIPT)
# port where listener server listens for incoming booted vms.
_PORT = 8000
# file to record the start time of the boots using system time in nanoseconds.
_START_TIME_FILE = 'start_time'
# start time file path
_START_TIME_FILE_PATH = posixpath.join(_REMOTE_DIR, _START_TIME_FILE)
# file to record the end time of the boots using system time in naneseconds.
_RESULTS_FILE = 'results'
# results file path
_RESULTS_FILE_PATH = posixpath.join(_REMOTE_DIR, _RESULTS_FILE)
# Seconds to wait for vms to boot.
_TIMEOUT_SECONDS = 60 * 10
# Seconds to deplay between polling for launcher server task complete.
_POLLING_DELAY = 3
# Naming pattern for booted vms.
_BOOT_VM_NAME_PREFIX = 'booter-{launcher_name}'
# sha256sum for preprovisioned service account credentials.
# If not using service account credentials from preprovisioned data bucket,
# use --gcp_service_account_key_file flag to specify the same credentials.
BENCHMARK_DATA = {
    'large-scale-boot-381ea7fa0a7d.json':
        '22cd2412f38f5b6f1615ae565cd74073deff3f30829769ec66eebb5cf9672329',
}
# default linux ssh port
_SSH_PORT = linux_virtual_machine.DEFAULT_SSH_PORT
# default windows rdp port
_RDP_PORT = windows_virtual_machine.RDP_PORT


def _GetServerStartCommand(client_port, launcher_vm):
  # command to start the listener server
  vms_name_pattern = '{name_pattern}-VM_ID.{zone}.c.{project}.internal'.format(
      name_pattern=_BOOT_VM_NAME_PREFIX.format(
          launcher_name=launcher_vm.name),
      zone=launcher_vm.zone,
      project=FLAGS.project)
  return (
      'python3 {server_path} {port} {results_path} {client_port} {use_server} '
      '{vms_name_pattern} {vms_count} > {server_log} 2>&1 &'.format(
          server_path=posixpath.join(
              _REMOTE_DIR, _LISTENER_SERVER.split('/')[-1]),
          port=_PORT,
          results_path=_RESULTS_FILE_PATH,
          client_port=client_port,
          use_server=FLAGS.vms_contact_launcher,
          vms_name_pattern=vms_name_pattern,
          vms_count=FLAGS.boots_per_launcher,
          server_log=_LISTENER_SERVER_LOG))


def _IsLinux():
  """Returns whether the boot vms are Linux VMs."""
  return FLAGS.boot_os_type in os_types.LINUX_OS_TYPES


class InsufficientBootsError(Exception):
  """Error thrown if there are insufficient boots during wait."""


def CheckPrerequisites(_):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  data.ResourcePath(_BOOT_TEMPLATE)
  data.ResourcePath(_LISTENER_SERVER)
  data.ResourcePath(_CLEAN_UP_TEMPLATE)
  if FLAGS.cloud != 'GCP':
    raise errors.Benchmarks.PrepareException(
        'Booting VMs on non-GCP clouds is not yet supported.')


def GetConfig(user_config):
  """Load and updates the benchmark config with user flags.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  launcher_config = config['vm_groups']['servers']
  launcher_config['vm_count'] = FLAGS.num_vms
  launcher_config['vm_spec'][FLAGS.cloud]['machine_type'] = (
      FLAGS.launcher_machine_type)
  booter_template = config['vm_groups']['clients']
  booter_template['os_type'] = FLAGS.boot_os_type
  booter_template['vm_spec'][FLAGS.cloud]['machine_type'] = (
      FLAGS.boot_machine_type)
  if FLAGS.machine_type:
    raise errors.Setup.InvalidConfigurationError(
        'Do not set machine type flag as it will override both launcher and '
        'booter machine types. Use launcher_machine_type and boot_machine_type'
        'instead.')
  if booter_template['vm_count'] != 1:
    raise errors.Setup.InvalidConfigurationError(
        'Booter_template is a configuration template VM. '
        'Booter count should be set by number of launchers (FLAGS.num_vms) and '
        'booters per launcher (FLAGS.boots_per_launcher).')
  return config


def _BuildContext(launcher_vm, booter_template_vm):
  """Returns the context variables for Jinja2 template during rendering."""
  return {
      'os_type': 'linux' if _IsLinux() else 'windows',
      'contact_launcher': FLAGS.vms_contact_launcher,
      'cloud': FLAGS.cloud,
      'start_time_file': _START_TIME_FILE_PATH,
      'vm_count': FLAGS.boots_per_launcher,
      'launcher_vm_name': launcher_vm.name,
      'boot_vm_name_prefix': _BOOT_VM_NAME_PREFIX.format(
          launcher_name=launcher_vm.name),
      'project': FLAGS.project,
      'image_family': booter_template_vm.image_family,
      'image_project': booter_template_vm.image_project,
      'boot_disk_size': booter_template_vm.boot_disk_size,
      'boot_machine_type': booter_template_vm.machine_type,
      'server_ip': launcher_vm.internal_ip,
      'server_port': _PORT,
      'timeout': _TIMEOUT_SECONDS,
      'zone': launcher_vm.zone,
      'gcloud_path': FLAGS.gcloud_path,
  }


def _Install(launcher_vm, booter_template_vm):
  """Installs benchmark scripts and packages on the launcher vm."""
  # Render boot script on launcher server VM(s)
  context = _BuildContext(launcher_vm, booter_template_vm)
  launcher_vm.RenderTemplate(data.ResourcePath(_BOOT_TEMPLATE), _BOOT_PATH,
                             context)

  # Installs and start listener server on launcher VM(s).
  launcher_vm.InstallPackages('netcat')
  launcher_vm.PushDataFile(_LISTENER_SERVER, _REMOTE_DIR)
  client_port = _SSH_PORT if _IsLinux() else _RDP_PORT
  launcher_vm.RemoteCommand(_GetServerStartCommand(client_port, launcher_vm))
  # Render clean up script on launcher server VM(s).
  launcher_vm.RenderTemplate(data.ResourcePath(_CLEAN_UP_TEMPLATE),
                             _CLEAN_UP_SCRIPT_PATH, context)


def Prepare(benchmark_spec):
  """Prepare the launcher server vm(s).

  Prepare the launcher server vm(s) by:
  1) Build the script that each launcher server will use to kick off boot.
  2) Start a listening server to wait for booting vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True
  launcher_vms = benchmark_spec.vm_groups['servers']
  booter_template_vm = benchmark_spec.vm_groups['clients'][0]
  # Setup account/IAM credentials/permissions on launcher servers.
  if FLAGS.cloud == 'GCP':
    for vm in launcher_vms:
      gcp_util.AuthenticateServiceAccount(vm, benchmark=BENCHMARK_NAME)

  # fail early if launched VMs exceeds more than 50 per vcpu.
  # High CPU usage can negatively impact measured boot times.
  if FLAGS.boots_per_launcher > (launcher_vms[0].num_cpus * 50):
    raise errors.Setup.InvalidConfigurationError(
        'Each launcher server VM is launching too many VMs. '
        'Increase launcher server VM size or decrease boots_per_launcher. '
        'For a VM with {} CPUs, launch at most {} VMs.'.format(
            launcher_vms[0].num_cpus, launcher_vms[0].num_cpus * 50))

  vm_util.RunThreaded(
      lambda vm: _Install(vm, booter_template_vm), launcher_vms)


def _GetExpectedBoots():
  """Return the number of expected boots."""
  return int(FLAGS.num_vms) * int(FLAGS.boots_per_launcher)


@vm_util.Retry(poll_interval=_POLLING_DELAY, timeout=_TIMEOUT_SECONDS,
               retryable_exceptions=(InsufficientBootsError))
def _WaitForResponses(launcher_vms):
  """Wait for all results or server shutdown or TIMEOUT_SECONDS."""
  # if any listener server exited, stop waiting.
  def _LauncherError(vm):
    error, _ = vm.RemoteCommand('grep ERROR ' + _LISTENER_SERVER_LOG,
                                ignore_failure=True)
    return error
  error_str = vm_util.RunThreaded(_LauncherError, launcher_vms)
  if any(error_str):
    raise errors.Benchmarks.RunError(
        'Some listening server errored out: %s' % error_str)

  def _BootCountInLauncher(vm):
    stdout, _ = vm.RemoteCommand('wc -l ' + _RESULTS_FILE_PATH)
    return int(stdout.split()[0])
  boots = vm_util.RunThreaded(_BootCountInLauncher, launcher_vms)
  for vm, boot_count in zip(launcher_vms, boots):
    logging.info('Launcher %s reported %d/%d',
                 vm.internal_ip, boot_count, FLAGS.boots_per_launcher)
  reporting_vms_count = sum(boots)
  if reporting_vms_count != _GetExpectedBoots():
    raise InsufficientBootsError(
        'Launcher vms reported %d total boots. Expecting %d.' %
        (reporting_vms_count, _GetExpectedBoots()))


def _ParseResult(launcher_vms):
  """Parse the results on the launcher VMs and send it back.

  Boot time is the boot duration of the slowest machine.

  Args:
    launcher_vms: Launcher server VMs.

  Returns:
    A list of benchmark samples.
  """
  vm_count = 0
  slowest_time = -1
  get_starttime_cmd = 'cat {startime}'.format(startime=_START_TIME_FILE_PATH)
  get_results_cmd = 'cat {results}'.format(results=_RESULTS_FILE_PATH)
  samples = []
  common_metadata = {
      'cloud': FLAGS.cloud,
      'num_launchers': FLAGS.num_vms,
      'expected_boots_per_launcher': FLAGS.boots_per_launcher,
      'boot_os_type': FLAGS.boot_os_type,
      'boot_machine_type': FLAGS.boot_machine_type,
      'launcher_machine_type': FLAGS.launcher_machine_type,
      'vms_contact_launcher': FLAGS.vms_contact_launcher,
  }
  for vm in launcher_vms:
    start_time_str, _ = vm.RemoteCommand(get_starttime_cmd)
    start_time = int(start_time_str)
    results, _ = vm.RemoteCommand(get_results_cmd)
    cur_launcher_success = 0
    cur_launcher_closed_incoming = 0
    durations = []
    for line in results.splitlines():
      state, _, duration = line.split(':')
      end_time = int(duration)
      if state == 'Pass':
        duration_in_ns = end_time - start_time
        durations.append(duration_in_ns)
        slowest_time = max(slowest_time, duration_in_ns)
        cur_launcher_success += 1
      elif state == 'Fail':
        # outgoing port was open but incoming port was closed.
        cur_launcher_closed_incoming += 1

    vm_count += cur_launcher_success
    current_metadata = {
        'zone': vm.zone,
        'launcher_successes': cur_launcher_success,
        'launcher_boot_durations_ns': durations,
        'launcher_closed_incoming': cur_launcher_closed_incoming,
    }
    current_metadata.update(common_metadata)
    samples.append(sample.Sample('Launcher Boot Details', -1,
                                 '', current_metadata))

  samples.append(sample.Sample('Cluster Max Boot Time', slowest_time,
                               'nanoseconds', common_metadata))
  samples.append(sample.Sample('Cluster Expected Boots', _GetExpectedBoots(),
                               '', common_metadata))
  samples.append(sample.Sample('Cluster Success Boots', vm_count,
                               '', common_metadata))
  return samples


def Run(benchmark_spec):
  """Kick off gartner boot script on launcher server vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of benchmark samples.
  """
  launcher_vms = benchmark_spec.vm_groups['servers']
  vm_util.RunThreaded(
      lambda vm: vm.RemoteCommand('bash {} 2>&1 | tee log'.format(_BOOT_PATH)),
      launcher_vms)
  try:
    _WaitForResponses(launcher_vms)
  except InsufficientBootsError:
    # On really large-scale boots, some failures are expected.
    logging.info('Some VMs failed to boot.')
  return _ParseResult(launcher_vms)


def Cleanup(benchmark_spec):
  """Clean up.

  Launcher VMs and booter template VM are deleted by pkb resource management.
  Boot VMs are self-destructing, but we will make a second attempt at destroying
  them anyway for good hygene.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  launcher_vms = benchmark_spec.vm_groups['servers']
  command = 'bash {} 2>&1 | tee clean_up_log'.format(_CLEAN_UP_SCRIPT_PATH)
  vm_util.RunThreaded(
      lambda vm: vm.RemoteCommand(command),
      launcher_vms)
