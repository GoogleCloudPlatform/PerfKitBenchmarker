# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containning methods for triggering maintenance simulation."""

from collections.abc import Mapping, MutableSequence
import datetime
import json
import logging
import ntpath
import posixpath
import threading
import time
import typing
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import gce_virtual_machine
from perfkitbenchmarker.providers.gcp import gce_windows_virtual_machine
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.time_triggers import base_disruption_trigger

WindowsGceVirtualMachine = gce_windows_virtual_machine.WindowsGceVirtualMachine

SIMULATE_MAINTENANCE = flags.DEFINE_boolean(
    'simulate_maintenance',
    False,
    (
        'Whether to simulate VM maintenance during the benchmark. '
        'This simulate maintenance happens right after run stage starts.'
    ),
)

SIMULATE_MAINTENANCE_WITH_LOG = flags.DEFINE_boolean(
    'simulate_maintenance_with_log',
    False,
    (
        'Whether to create a log file with the vm information instead of '
        'invoking trigger simulation.'
    ),
)

SIMULATE_MAINTENANCE_DELAY = flags.DEFINE_integer(
    'simulate_maintenance_delay',
    0,
    (
        'The number of seconds to wait to start simulating '
        'maintenance after run stage.'
    ),
)

CAPTURE_LIVE_MIGRATION_TIMESTAMPS = flags.DEFINE_boolean(
    'capture_live_migration_timestamps',
    False,
    (
        'Whether to capture maintenance times during migration. '
        'This requires external python script for notification.'
    ),
)

# 2h timeout for LM notification
LM_NOTIFICATION_TIMEOUT_SECONDS = 60 * 60 * 2

# 10m wait time prior to checking log for LM status
LM_UNAVAILABLE_STATUS_WAIT_TIME_MIN = 10


class GCESimulateMaintenanceTool:
  """Helper class for simulating maintenance on a GCE VM."""

  def __init__(self, vm: gce_virtual_machine.GceVirtualMachine):
    self.vm = vm
    self._lm_times_semaphore = threading.Semaphore(0)
    self._lm_notice_script = 'gce_maintenance_notice.py'
    self._lm_signal_log = 'lm_signal.log'
    self._lm_notice_log = 'gce_maintenance_notice.log'

  def SimulateMaintenanceWithLog(self):
    """Create a json file with information related to the vm."""
    simulate_maintenance_json = {
        'current_time': datetime.datetime.now().timestamp() * 1000,
        'instance_id': self.vm.id,
        'project': self.vm.project,
        'instance_name': self.vm.name,
        'zone': self.vm.zone,
    }
    vm_path = posixpath.join(vm_util.GetTempDir(), self._lm_signal_log)
    with open(vm_path, 'w+') as f:
      json.dump(simulate_maintenance_json, f, indent=2, sort_keys=True)

  def SimulateMaintenanceEvent(self):
    """Simulates a maintenance event on the VM."""
    cmd = util.GcloudCommand(
        self.vm,
        'compute',
        'instances',
        'simulate-maintenance-event',
        self.vm.name,
        '--async',
    )
    logcmd = util.GcloudCommand(
        None,
        'logging',
        'read',
        '"protoPayload.methodName=v1.compute.instances.simulateMaintenanceEvent'
        f' resource.labels.instance_id={self.vm.id}"',
    )
    logcmd.flags['freshness'] = f'{LM_UNAVAILABLE_STATUS_WAIT_TIME_MIN}M'

    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if retcode or 'error' in stdout:
      raise errors.VirtualMachine.VirtualMachineError(
          'Unable to simulate maintenance event.'
      )

    time.sleep(LM_UNAVAILABLE_STATUS_WAIT_TIME_MIN * 60)
    stdout, _, retcode = logcmd.Issue(raise_on_failure=False)
    # if the migration is temporarily unavailable, retry the migration command
    if not retcode and 'MIGRATION_TEMPORARILY_UNAVAILABLE' in stdout:
      stdout, _, retcode = cmd.Issue(raise_on_failure=False)
      if retcode or 'error' in stdout:
        raise errors.VirtualMachine.VirtualMachineError(
            'Unable to simulate maintenance event.'
        )

  def SetupLMNotification(self):
    """Prepare environment for /scripts/gce_maintenance_notify.py script."""
    self.vm.Install('pip')
    self.vm.RemoteCommand('sudo pip3 install requests')
    self.vm.PushDataFile(self._lm_notice_script, vm_util.VM_TMP_DIR)

  def _GetLMNotificationCommand(self):
    """Return Remote python execution command for LM notify script."""
    vm_path = posixpath.join(vm_util.VM_TMP_DIR, self._lm_notice_script)
    server_log = self._lm_notice_log
    return (
        f'python3 {vm_path} {gcp_flags.LM_NOTIFICATION_METADATA_NAME.value} >'
        f' {server_log} 2>&1'
    )

  def _PullLMNoticeLog(self):
    """Pull the LM Notice Log onto the local VM."""
    self.vm.PullFile(vm_util.GetTempDir(), self._lm_notice_log)

  def StartLMNotification(self):
    """Start meta-data server notification subscription."""

    def _Subscribe():
      self.vm.RemoteCommand(
          self._GetLMNotificationCommand(),
          timeout=LM_NOTIFICATION_TIMEOUT_SECONDS,
          ignore_failure=True,
      )
      self._PullLMNoticeLog()
      logging.info('[LM Notify] Release live migration lock.')
      self._lm_times_semaphore.release()

    logging.info('[LM Notify] Create live migration timestamp thread.')
    t = threading.Thread(target=_Subscribe)
    t.daemon = True
    t.start()

  def WaitLMNotificationRelease(self):
    """Block main thread until LM ended."""
    logging.info('[LM Notify] Wait for live migration to finish.')
    self._lm_times_semaphore.acquire()
    logging.info('[LM Notify] Live migration is done.')

  def _ReadLMNoticeContents(self):
    """Read the contents of the LM Notice Log into a string."""
    return self.vm.RemoteCommand(f'cat {self._lm_notice_log}')[0]

  def CollectLMNotificationsTime(self):
    """Extract LM notifications from log file.

    Sample Log file to parse:
      Host_maintenance_start _at_ 1656555520.78123
      Host_maintenance_end _at_ 1656557227.63631

    Returns:
      Live migration events timing info dictionary
    """
    lm_total_time_key = 'LM_total_time'
    lm_start_time_key = 'Host_maintenance_start'
    lm_end_time_key = 'Host_maintenance_end'
    events_dict = {
        'machine_instance': self.vm.instance_number,
        lm_start_time_key: 0,
        lm_end_time_key: 0,
        lm_total_time_key: 0,
    }
    lm_times = self._ReadLMNoticeContents()
    if not lm_times:
      raise ValueError('Cannot collect lm times. Live Migration might failed.')

    # Result may contain errors captured, so we need to skip them
    for event_info in lm_times.splitlines():
      event_info_parts = event_info.split(' _at_ ')
      if len(event_info_parts) == 2:
        events_dict[event_info_parts[0]] = event_info_parts[1]

    events_dict[lm_total_time_key] = float(
        events_dict[lm_end_time_key]
    ) - float(events_dict[lm_start_time_key])
    return events_dict


class GCESimulateMaintenanceToolForWindows(GCESimulateMaintenanceTool):
  """Helper class for simulating maintenance on a GCE VM."""

  def __init__(self, vm: gce_virtual_machine.GceVirtualMachine):
    super().__init__(vm)
    if vm is not WindowsGceVirtualMachine:
      raise ValueError('WindowsGceVirtualMachine is required.')

    self.vm_temp_dir = typing.cast(WindowsGceVirtualMachine, vm).temp_dir

  def SetupLMNotification(self):
    """Prepare environment for /scripts/gce_maintenance_notify.py script."""
    self.vm.Install('python')
    self.vm.RemoteCommand('pip install requests')
    self.vm.PushDataFile(
        self._lm_notice_script, f'{self.vm_temp_dir}\\{self._lm_notice_script}'
    )

  def _GetLMNotificationCommand(self):
    """Return Remote python execution command for LM notify script."""
    vm_path = ntpath.join(self.vm_temp_dir, self._lm_notice_script)
    return (
        f'python {vm_path} {gcp_flags.LM_NOTIFICATION_METADATA_NAME.value}'
        f' {gcp_flags.LM_NOTIFICATION_TIMEOUT.value} >'
        f' {self.vm_temp_dir}\\{self._lm_notice_log} 2>&1'
    )

  def _PullLMNoticeLog(self):
    """Pull the LM Notice Log onto the local VM."""
    self.vm.PullFile(
        f'{vm_util.GetTempDir()}/{self._lm_notice_log}',
        f'{self.vm_temp_dir}\\{self._lm_notice_log}',
    )

  def StartLMNotification(self):
    """Start meta-data server notification subscription."""

    def _Subscribe():
      self.vm.RemoteCommand(
          self._GetLMNotificationCommand(),
          timeout=LM_NOTIFICATION_TIMEOUT_SECONDS,
          ignore_failure=True,
      )
      self._PullLMNoticeLog()
      logging.info('[LM Notify] Release live migration lock.')
      self._lm_times_semaphore.release()

    logging.info('[LM Notify] Create live migration timestamp thread.')
    t = threading.Thread(target=_Subscribe)
    t.daemon = True
    t.start()

  def WaitLMNotificationRelease(self):
    """Block main thread until LM ended."""
    logging.info('[LM Notify] Wait for live migration to finish.')
    self._lm_times_semaphore.acquire()
    logging.info('[LM Notify] Live migration is done.')

  def _ReadLMNoticeContents(self):
    """Read the contents of the LM Notice Log into a string."""
    return self.vm.RemoteCommand(
        f'type {self.vm_temp_dir}\\{self._lm_notice_log}'
    )[0]

  def CollectLMNotificationsTime(self):
    """Extract LM notifications from log file.

    Sample Log file to parse:
      Host_maintenance_start _at_ 1656555520.78123
      Host_maintenance_end _at_ 1656557227.63631

    Returns:
      Live migration events timing info dictionary
    """
    lm_total_time_key = 'LM_total_time'
    lm_start_time_key = 'Host_maintenance_start'
    lm_end_time_key = 'Host_maintenance_end'
    events_dict = {
        'machine_instance': self.vm.instance_number,
        lm_start_time_key: 0,
        lm_end_time_key: 0,
        lm_total_time_key: 0,
    }
    lm_times = self._ReadLMNoticeContents()
    if not lm_times:
      raise ValueError('Cannot collect lm times. Live Migration might failed.')

    # Result may contain errors captured, so we need to skip them
    for event_info in lm_times.splitlines():
      event_info_parts = event_info.split(' _at_ ')
      if len(event_info_parts) == 2:
        events_dict[event_info_parts[0]] = event_info_parts[1]

    events_dict[lm_total_time_key] = float(
        events_dict[lm_end_time_key]
    ) - float(events_dict[lm_start_time_key])
    return events_dict


class MaintenanceEventTrigger(base_disruption_trigger.BaseDisruptionTrigger):
  """Class contains logic for triggering maintenance events."""

  def __init__(self):
    super().__init__(SIMULATE_MAINTENANCE_DELAY.value)
    self.capture_live_migration_timestamps = (
        CAPTURE_LIVE_MIGRATION_TIMESTAMPS.value
    )
    self.gce_simulate_maintenance_helpers: Dict[
        gce_virtual_machine.GceVirtualMachine, GCESimulateMaintenanceTool
    ] = {}

  def TriggerMethod(self, vm: virtual_machine.VirtualMachine):
    if self.capture_live_migration_timestamps:
      self.gce_simulate_maintenance_helpers[vm].StartLMNotification()
    if SIMULATE_MAINTENANCE_WITH_LOG.value:
      self.gce_simulate_maintenance_helpers[vm].SimulateMaintenanceWithLog()
    else:
      self.gce_simulate_maintenance_helpers[vm].SimulateMaintenanceEvent()

  def SetUp(self):
    """Sets up notification if live migration timestamps are captured."""
    for vm in self.vms:
      self.gce_simulate_maintenance_helpers[vm] = GCESimulateMaintenanceFactory(
          vm
      )
    if self.capture_live_migration_timestamps:
      for helper in self.gce_simulate_maintenance_helpers.values():
        helper.SetupLMNotification()

  def WaitForDisruption(self) -> MutableSequence[Mapping[str, Any]]:
    """Wait for the disruption to end and return the end time."""
    if self.capture_live_migration_timestamps:
      # Block test exit until LM ended.
      lm_events = []
      for helper in self.gce_simulate_maintenance_helpers.values():
        helper.WaitLMNotificationRelease()
        lm_events.append(helper.CollectLMNotificationsTime())
      return lm_events
    else:
      return []

  def GetDisruptionEnds(self) -> float | None:
    """Get the disruption ends."""
    if self.capture_live_migration_timestamps:
      # lm ends is computed from LM notification
      return self.disruption_ends
    return None

  @property
  def trigger_name(self) -> str:
    return 'simulate_maintenance'


def GCESimulateMaintenanceFactory(
    vm: virtual_machine.VirtualMachine,
) -> GCESimulateMaintenanceTool:
  """Factory method for GCESimulateMaintenanceTool  ."""
  if isinstance(vm, WindowsGceVirtualMachine):
    return GCESimulateMaintenanceToolForWindows(vm)
  return GCESimulateMaintenanceTool(vm)  # pytype: disable=wrong-arg-types


def Register(parsed_flags):
  """Registers the simulate maintenance trigger if FLAGS.simulate_maintenance is set."""
  if not parsed_flags.simulate_maintenance:
    return
  trigger = MaintenanceEventTrigger()
  trigger.Register()
