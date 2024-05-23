"""Module containing tools for benchmarking fastboot performance."""

import calendar
import dataclasses
import datetime
import logging
import os
import re
import time
from typing import Callable, List, Optional, Tuple

from absl import flags
import jinja2
from perfkitbenchmarker import data
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

DATA_DIR = 'linux_boot'
BOOT_STARTUP_SCRIPT = 'startup.sh'
BOOT_STARTUP_SCRIPT_TEMPLATE = 'startup.sh.j2'
BOOT_SCRIPT_OUTPUT = '/tmp/boot.output'
TCPDUMP_OUTPUT = 'tcpdump.output'
TCPDUMP_PID_FILE = 'tcpdump.pid'
_MICROSECONDS_PER_SECOND = 1000000
FLAGS = flags.FLAGS


def PrepareBootScriptVM(aux_vm_ips: str, aux_vm_port: int) -> str:
  script_path = data.ResourcePath(
      os.path.join(DATA_DIR, BOOT_STARTUP_SCRIPT_TEMPLATE)
  )
  with open(script_path) as fp:
    template_str = fp.read()

  environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
  template = environment.from_string(template_str)
  return template.render(port=aux_vm_port, ips=aux_vm_ips)


@vm_util.Retry(log_errors=False, poll_interval=1, timeout=300)
def GetStartupScriptOutput(
    vm: virtual_machine.BaseVirtualMachine, output_file: str
) -> str:
  """return fastboot script log."""
  return vm.RemoteCommand(f'cat {output_file}')[0]


def DatetimeToUTCSeconds(date: datetime.datetime) -> float:
  """Converts a datetime object to seconds since the epoch in UTC.

  Args:
    date: A datetime to convert.

  Returns:
    The number of seconds since the epoch, in UTC, represented by the input
    datetime.
  """
  if date.tzinfo is None:
    seconds = calendar.timegm(date.utctimetuple())
  else:
    seconds = int(date.replace(microsecond=0).timestamp())
  return seconds + date.microsecond / _MICROSECONDS_PER_SECOND


def CollectBootSamples(
    vm: virtual_machine.VirtualMachine,
    runner_ip: Tuple[str, str],
    create_time: datetime.datetime,
    include_networking_samples: bool = False,
) -> List[sample.Sample]:
  """Collect boot samples.

  Args:
    vm: The boot vm.
    runner_ip: Runner ip addresses to collect VM-to-VM metrics.
    create_time: VM creation time.
    include_networking_samples: Boolean, whether to include samples such as time
      to first egress/ingress packet.

  Returns:
    A list of sample.Sample objects.
  """
  boot_output = GetStartupScriptOutput(vm, BOOT_SCRIPT_OUTPUT).split('\n')
  boot_samples = ScrapeConsoleLogLines(
      boot_output, create_time, CONSOLE_FIRST_START_MATCHERS
  )
  create_time_utc_seconds = DatetimeToUTCSeconds(create_time)
  guest_samples = CollectGuestSamples(vm, create_time_utc_seconds)
  kernel_offset = GetKernelStartTimestamp(vm) - create_time_utc_seconds
  kernel_samples = CollectKernelSamples(vm, kernel_offset)

  samples = boot_samples + guest_samples + kernel_samples

  if include_networking_samples:
    samples.extend(CollectVmToVmSamples(vm, runner_ip, create_time))

  return samples


def UtcTimestampToDatetime(timestamp_str: str) -> datetime.datetime:
  return datetime.datetime.fromtimestamp(
      float(timestamp_str), datetime.timezone.utc
  )


_DMESG_TIME_PREFIX = r'\[\s*(\d*.\d*)\]\s*'


def ParseDMesgOutput(dmesg_output: str, suffix: str) -> Optional[float]:
  """Takes a string from dmesg formatted output, searches for the suffix.

  Returns the timestamp from square brackets associated with it.

  Example: [    0.016311] SRAT: PXM 0 -> APIC 0x00 -> Node 0
           and 'SRAT' should return 0.016311.

  Args:
    dmesg_output: the full set of dmesg output.
    suffix: the regex after the timestamp to search for

  Returns:
    None if function not found in input, Float if found.
  """
  val = _DMESG_TIME_PREFIX + suffix
  regex = re.compile(val)
  for result in regex.finditer(dmesg_output):
    segment = result.group(0)
    timestamp_string = segment.split(']')[0][1:]
    return float(timestamp_string.strip())
  return None


@dataclasses.dataclass
class RegexMatcher:
  metric: str
  pattern: re.Pattern[str]
  parser: Callable[[str], datetime.datetime]


CONSOLE_FIRST_START_MATCHERS = [
    RegexMatcher(
        'startup_script_run',
        re.compile(r'Startup script running ([.0-9]+)'),
        UtcTimestampToDatetime,
    ),
    RegexMatcher(
        'linux_booting',
        re.compile(r'Kernel start time ([.0-9]+)'),
        UtcTimestampToDatetime,
    ),
]


def ScrapeConsoleLogLines(
    log_lines: List[str],
    start_time: datetime.datetime,
    matchers: List[RegexMatcher],
) -> List[sample.Sample]:
  """Extract timings from the guest console log lines.

  Args:
    log_lines: an iterable containing the log lines
    start_time: the datetime_tz to measure relative to
    matchers: A list of RegexMatcher for scraping output

  Returns:
    List of sample.Sample objects, containing various boot metrics parsed
    from guest logs.
  """
  times = {}
  samples = []

  for line in log_lines:
    for regex_matcher in matchers:
      m = regex_matcher.pattern.search(line)
      if m is None:
        continue
      try:
        times[regex_matcher.metric] = regex_matcher.parser(m.group(1))
      except ValueError:
        logging.error('Unable to process value %s', m.group(1))
        continue

  for key, val in times.items():
    samples.append(
        sample.Sample(key, (val - start_time).total_seconds(), 'second', {})
    )

  return samples


def CollectGuestSamples(
    vm: virtual_machine.VirtualMachine, metric_start_time: float
) -> List[sample.Sample]:
  """Collect guest metrics.

  All metrics published are normalized against VM creation timestamp, including:
  - kernel_start: Time guest kernel starts since vm create.
  - user_start: Time userspace starts since vm create.
  - guest_boot_complete: Time systemd-analyze finishes since vm create.
  - Various boot services reported from systemd-analyze critical-chain.

  Args:
    vm: The vm to extract metrics from.
    metric_start_time: The start time to measure from, in utc seconds.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  kernel_start = GetKernelStartTimestamp(vm) - metric_start_time
  drift = GetServerTimeDrift(vm)
  logging.info('Server time drift: %d', drift)

  # preventative measure dealing with startups stuck on user data fetch
  vm.RemoteCommand('sudo pkill -f wait_for_user_data.sh', ignore_failure=True)

  # user_start and total_time are normalized against guest kernel start time,
  # calculated from /proc/uptime.
  # When publishing, all metrics are normalized against vm creation time, thus
  # need to add kernel_start.
  user_start, total_time = WaitForReady(vm, 300)

  samples.append(
      sample.Sample(
          'guest_boot_complete',
          kernel_start + total_time,
          'second',
          {},
      )
  )

  samples.append(sample.Sample('kernel_start', kernel_start, 'second', {}))

  samples.append(
      sample.Sample('user_start', kernel_start + user_start, 'second', {})
  )

  for metric_name, parser in SYSTEMD_CRITICAL_CHAIN_METRICS:
    if callable(parser):
      metric_val = parser(vm)
    else:
      metric_val = GetSystemDCriticalChainMetric(vm, parser)
    if metric_val is not None:
      samples.append(
          sample.Sample(
              metric_name,
              metric_val + kernel_start + user_start,
              'second',
              {},
          )
      )
  return samples


def CollectKernelSamples(
    vm: virtual_machine.VirtualMachine, offset: float
) -> List[sample.Sample]:
  """Collect kernel metrics.

  Args:
    vm: the VM to extract the metrics from.
    offset: any clock offset to add.

  Returns:
    A list of sample.Sample objects.
  """
  dmesg = vm.RemoteCommand('sudo dmesg')[0]
  samples = []

  for metric, regex in DMESG_METRICS:
    val = ParseDMesgOutput(dmesg, regex)
    if val:
      samples.append(sample.Sample(metric, val + offset, 'second', {}))

  return samples


def GetServerTimeDrift(vm: virtual_machine.VirtualMachine) -> Optional[float]:
  start_time = time.process_time()
  out, _ = vm.RemoteCommand('date +%s.%N')
  end_time = time.process_time()
  latency = (end_time - start_time) / 2
  server_time = float(out)

  return server_time - (start_time + latency)


def GetKernelStartTimestamp(vm: virtual_machine.VirtualMachine) -> float:
  """Get when guest kernel starts."""
  out, _ = vm.RemoteCommand('cat /proc/uptime & (date +%s.%N)')
  lines = out.splitlines()
  if ' ' in lines[0]:
    uptime = lines[0]
    now = lines[1]
  else:
    uptime = lines[1]
    now = lines[0]

  uptime_epoc = float(now) - float(uptime.split(' ')[0])
  logging.info('Kernel start: %d', uptime_epoc)
  return uptime_epoc


def _ParseSeconds(formatted_time: str) -> float:
  """Returns the number of seconds, given a formatted time string.

  Expects a string similar to the following formats:
    1min 3.02s
    203ms

  Args:
    formatted_time: time formatted string.  See format example above.

  Raises:
    RuntimeError: For unrecognized date format.

  Returns:
    Floating point number of seconds represented by the string.
  """
  formatted_time = formatted_time.strip()
  secs = 0.0
  for part in formatted_time.split(' '):
    if part.endswith('h'):
      secs += float(part[0 : len(part) - 1]) * 3600
    elif part.endswith('min'):
      secs += float(part[0 : len(part) - 3]) * 60
    elif part.endswith('ms'):
      secs += float(part[0 : len(part) - 2]) / 1000
    elif part.endswith('us'):
      secs += float(part[0 : len(part) - 2]) / 1000 / 1000
    elif part.endswith('s'):
      secs += float(part[0 : len(part) - 1])
    else:
      raise RuntimeError('Unrecognized date format: ' + formatted_time)
  return secs


PATTERN_KERNEL_TIME = re.compile(r' \+ ([^\+]*?)s \(kernel\)')
ALT_PATTERN_KERNEL_TIME = re.compile(r' in (.*?)s \(kernel\)')
PATTERN_INITRD_TIME = re.compile(r' \+ ([^\+]*?)s \(initrd\)')
PATTERN_USER_TIME = re.compile(r' \+ ([^\+]*?)s \(userspace\)')


def ParseUserTotalTimes(system_d_string: str) -> Tuple[float, float]:
  """Takes a string from systemd-analyze command and parses it.

  Args:
    system_d_string: the string output from systemd-analyze string output. i.e.
      "Startup finished in 2.2s (kernel) + 1min 12.5s (userspace) = 1min
      14.774s" "Startup finished in 448ms (firmware) + 1.913s (loader) + 1.182s
      (kernel) + 52.438s (initrd) + 30.413s (userspace) = 1min 26.397s"

  Returns:
    A tuple of user_start and total_time, both in seconds.
  """
  kernel_time = -1
  user_time = -1
  # how long takes for initrd, which isn't reported in some OSes.
  initrd_time = 0

  for match in PATTERN_KERNEL_TIME.finditer(system_d_string):
    kernel_time = _ParseSeconds(match.group()[3:-9])
    break
  else:
    for match in ALT_PATTERN_KERNEL_TIME.finditer(system_d_string):
      kernel_time = _ParseSeconds(match.group()[4:-9])
      break

  for match in PATTERN_INITRD_TIME.finditer(system_d_string):
    initrd_time = _ParseSeconds(match.group()[3:-9])
    break

  for match in PATTERN_USER_TIME.finditer(system_d_string):
    user_time = _ParseSeconds(match.group()[3:-12])
    break

  # some OS reports firmware and loader time, but these are not counted
  # in /proc/uptime, since we uses /proc/uptime as kernel_start, these should
  # not be counted.
  user_start = kernel_time + initrd_time
  total_time = user_start + user_time
  logging.info(
      'systemd: %s, user_start : %s sec, total_time: %s sec',
      system_d_string,
      user_start,
      total_time,
  )
  return user_start, total_time


PATTERN_SYSTEMD_LINE1 = re.compile(r'\+(.*?)s')
PATTERN_SYSTEMD_LINE2 = re.compile(r'@(.*?)s')


def ParseSystemDCriticalChainOutput(systemd_output: str) -> Optional[float]:
  """Returns critical chain output seconds.

  Given the output from systemd-analyze critical-chain <target> will
  return the number if seconds it took for that service to launch relative
  to boot time.
  Sample 1 (first line with @):
    graphical.target @2.741s
      multi-user.target @2.739s
        cron.service @2.666s
  Sample 2 (first line with +):
    systemd-sysctl.service +103ms
    systemd-modules-load.service @671ms +150ms
      systemd-journald.socket @530ms
        -.mount @300ms
          -.slice @300ms

  Args:
    systemd_output: the output from systemd-analyze critical-chain command.

  Returns:
    number of seconds.
  """
  logging.info('Parsing System D Input: %s', systemd_output)
  lines = systemd_output.split('\n')
  if len(lines) < 5:
    logging.warning(
        'Invalid format. Critical chain command: %s',
        systemd_output,
    )
    return None
  first_line = lines[3]
  second_line = lines[4]
  timing = 0.0
  found_some = False

  try:
    if '+' in first_line:
      for match in PATTERN_SYSTEMD_LINE1.finditer(first_line):
        timing = _ParseSeconds(match.group()[1:])
        found_some = True
        break
    else:
      second_line = first_line

    for match in PATTERN_SYSTEMD_LINE2.finditer(second_line):
      timing += _ParseSeconds(match.group()[1:])
      found_some = True
      break

    if not found_some:
      logging.warning(
          'Invalid format. Critical chain command: %s',
          systemd_output,
      )
      return None

    return timing
  except RuntimeError:  # pylint: disable=broad-except
    logging.exception('SystemD output could not be parsed.')
    return None


def ParseSystemDCriticalChainServiceTime(systemd_out: str) -> Optional[float]:
  """Returns critical chain total service time.

  Given the output from systemd-analyze critical-chain <target> will
  return the number if seconds it took for that service to launch just itself.
  Sample:
    systemd-sysctl.service +103ms
    systemd-modules-load.service @671ms +150ms
      systemd-journald.socket @530ms
        -.mount @300ms
          -.slice @300ms

  Args:
    systemd_out: the output from systemd-analyze critical-chain command.

  Returns:
    The number of seconds as a float, or None if service not found.
  """
  lines = systemd_out.split('\n')
  if len(lines) < 4:
    return None
  first_line = lines[3]

  if '+' in first_line:
    for match in PATTERN_SYSTEMD_LINE1.finditer(first_line):
      return _ParseSeconds(match.group()[1:])
  else:
    return None


def WaitForReady(
    vm: virtual_machine.VirtualMachine, timeout: int
) -> Tuple[float, float]:
  """Ensure the system is ready for boot metrics inspection.

  Args:
    vm: The vm to wait for.
    timeout: The max additional amount of time to block for after boot.

  Returns:
    The amount of (user_start, total) time it took for waiting for boot
    metrics ready.

  Raises:
    RuntimeError: if system is not ready within timeout.
  """
  start_time = time.time()
  while start_time + timeout > time.time():
    stdout, _, code = vm.RemoteCommandWithReturnCode(
        'sudo systemd-analyze', ignore_failure=True
    )
    if code == 0:
      return ParseUserTotalTimes(stdout)
    time.sleep(1)

  stdout, _ = vm.RemoteCommand('sudo systemctl list-jobs')
  logging.error('Timed out waiting for boot.  jobs: %s', stdout)
  raise RuntimeError('Timed out waiting for boot.')


def GetSshServiceReady(vm: virtual_machine.VirtualMachine) -> Optional[float]:
  """Get ssh service ready time (the service name is OS dependent)."""
  return GetSystemDCriticalChainMetric(
      vm, 'ssh.service'
  ) or GetSystemDCriticalChainMetric(vm, 'sshd.service')


def GetGuestScriptsStart(vm: virtual_machine.VirtualMachine) -> Optional[float]:
  stdout, _ = vm.RemoteCommand(
      'sudo systemd-analyze critical-chain google-startup-scripts.service'
  )
  total = ParseSystemDCriticalChainOutput(stdout)
  service_time = ParseSystemDCriticalChainServiceTime(stdout)
  if total and service_time:
    return total - service_time
  else:
    return None


def GetSystemDCriticalChainMetric(
    vm: virtual_machine.VirtualMachine, metric: str
) -> Optional[float]:
  stdout, _ = vm.RemoteCommand(f'sudo systemd-analyze critical-chain {metric}')
  return ParseSystemDCriticalChainOutput(stdout)


# List of tuples, containing metric name, service names or parsing method
SYSTEMD_CRITICAL_CHAIN_METRICS = [
    ('local_fs_ready', 'local-fs.target'),
    ('network_ready', 'network.target'),
    ('ssh_ready', GetSshServiceReady),
    ('guest_startup_start', GetGuestScriptsStart),
    ('guest_startup_end', 'google-startup-scripts.service'),
    ('graphical_target', 'graphical.target'),
    ('multi_user_target', 'multi-user.target'),
    ('basic_target', 'basic.target'),
    ('sockets_target', 'sockets.target'),
    ('time_sync_target', 'time-sync.target'),
    ('slices_target', 'slices.target'),
    ('paths_target', 'paths.target'),
    ('sysinit_target', 'sysinit.target'),
    ('cryptsetup_target', 'cryptsetup.target'),
    ('emergency_target', 'emergency.target'),
    ('getty_target', 'getty.target'),
    ('remote_fs_target', 'remote-fs.target'),
    ('swap_target', 'swap.target'),
    ('timers_target', 'timers.target'),
    ('network_pre_target', 'network-pre.target'),
    ('rescue_target', 'rescue.target'),
    ('local_fs_pre_target', 'local-fs-pre.target'),
    ('remote_fs_pre_target', 'remote-fs-pre.target'),
    ('cloud_config_target', 'cloud-config.target'),
    ('shutdown_target', 'shutdown.target'),
    ('systemd_sysctl_service', 'systemd-sysctl.service'),
    ('systemd_journald_service', 'systemd-journald.service'),
    ('systemd_modules_load_service', 'systemd-modules-load.service'),
    ('systemd_remount_fs_service', 'systemd-remount-fs.service'),
    ('systemd_initctl_service', 'systemd-initctl.service'),
    ('systemd_networkd_service', 'systemd-networkd.service'),
    ('google_instance_setup_service', 'google-instance-setup.service'),
    ('cloud_init_local_service', 'cloud-init-local.service'),
    ('cloud_init_service', 'cloud-init.service'),
    ('cloud_final_service', 'cloud-final.service'),
    ('dev_hugepages_mount', 'dev-hugepages.mount'),
    ('dev_sda1_device', 'dev-sda1.device'),
]

# List of tuples, containing metric name and regex
DMESG_METRICS = [
    ('check_timer', r'\.\.TIMER'),
    ('aesni_init', r'AES'),
    ('pci_bridge_created', r'PCI host bridge'),
    ('pci_dma_setup', r'PCI-DMA'),
    ('scsi_init', r'scsi host0'),
    ('nvme_init', r'nvme nvme0: pci function'),
    ('serial_8250_init', r'Serial: 8250'),
    ('tcp_bind_alloc', r'TCP bind'),
    ('rod_marked', r'Write protecting the kernel read-only'),
    ('ps2_controller', r'i8042: PNP: PS/2 Controller'),
    ('rng_init', r'random: crng'),
    ('taskstats_reg', r'registered taskstats'),
    ('scsi_pd', r'scsi.*PersistentDisk'),
    ('systemd_run', r'Run /usr/lib/systemd/systemd'),
]


def CollectVmToVmSamples(
    vm: virtual_machine.VirtualMachine,
    runner_ip: Tuple[str, str],
    create_time: datetime.datetime,
) -> List[sample.Sample]:
  """Collect samples related to vm-to-vm networking."""
  samples = []
  vm_output = GetStartupScriptOutput(vm, BOOT_SCRIPT_OUTPUT)
  vm_internal_ip = vm.internal_ip
  vm_external_ip = vm.ip_address

  with open(vm_util.PrependTempDir(TCPDUMP_OUTPUT), 'r') as f:
    tcpdump_output = f.read()
  runner_internal_ip, runner_external_ip = runner_ip

  def DeltaSec(t):
    delta = (
        datetime.datetime.fromtimestamp(float(t), tz=datetime.timezone.utc)
        - create_time
    )
    return delta.total_seconds()

  for group in re.findall('Connection refused by (.+) at ([0-9.]+)', vm_output):
    ip, t = group
    if ip == runner_internal_ip:
      samples.append(
          sample.Sample('internal_ingress', DeltaSec(t), 'second', {})
      )
    elif ip == runner_external_ip:
      samples.append(
          sample.Sample('external_ingress', DeltaSec(t), 'second', {})
      )

  # Sample TCPDUMP output:
  # 1680653297.391525 IP 34.83.176.250.33216 > 10.240.0.9.8080: Flags [S]...
  # Extracts datetime, src, dest ip:  (datetime) IP (src) > (dest)
  internal_egress_sample = None
  external_egress_sample = None
  for group in re.findall(
      r'([0-9.:]+) IP ([0-9.]+)\.\d+ > [0-9.]+\.\d+.*', tcpdump_output
  ):
    t, src = group
    delta = DeltaSec(t)
    # Guard against recycled IPs causing negative deltas (captured during the
    # period between tcpdump starting and VM creation)
    if delta < 0:
      continue
    # Capture only the first positive egress delta for each IP.
    if src == vm_internal_ip and not internal_egress_sample:
      internal_egress_sample = sample.Sample(
          'internal_egress', delta, 'second', {}
      )
    elif src == vm_external_ip and not external_egress_sample:
      external_egress_sample = sample.Sample(
          'external_egress', delta, 'second', {}
      )
    if internal_egress_sample and external_egress_sample:
      break
  if internal_egress_sample:
    samples.append(internal_egress_sample)
  if external_egress_sample:
    samples.append(external_egress_sample)
  return samples
