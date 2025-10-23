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
from typing import Any

from absl import flags
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.time_triggers import base_disruption_trigger

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


class MaintenanceEventTrigger(base_disruption_trigger.BaseDisruptionTrigger):
  """Class contains logic for triggering maintenance events."""

  def __init__(self):
    super().__init__(SIMULATE_MAINTENANCE_DELAY.value)
    self.capture_live_migration_timestamps = (
        CAPTURE_LIVE_MIGRATION_TIMESTAMPS.value
    )

  def TriggerMethod(self, vm: virtual_machine.VirtualMachine):
    if self.capture_live_migration_timestamps:
      vm.StartLMNotification()
    if SIMULATE_MAINTENANCE_WITH_LOG.value:
      vm.SimulateMaintenanceWithLog()  # pytype: disable=attribute-error
    else:
      vm.SimulateMaintenanceEvent()

  def SetUp(self):
    """Sets up notification if live migration timestamps are captured."""
    if self.capture_live_migration_timestamps:
      for vm in self.vms:
        vm.SetupLMNotification()

  def WaitForDisruption(self) -> MutableSequence[Mapping[str, Any]]:
    """Wait for the disruption to end and return the end time."""
    if self.capture_live_migration_timestamps:
      # Block test exit until LM ended.
      lm_events = []
      for vm in self.vms:
        vm.WaitLMNotificationRelease()
        lm_events.append(vm.CollectLMNotificationsTime())
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


def Register(parsed_flags):
  """Registers the simulate maintenance trigger if FLAGS.simulate_maintenance is set."""
  if not parsed_flags.simulate_maintenance:
    return
  trigger = MaintenanceEventTrigger()
  trigger.Register()
