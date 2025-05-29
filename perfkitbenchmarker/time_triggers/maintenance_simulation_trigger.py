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

import collections
import copy
import logging
import statistics
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.time_triggers import base_time_trigger

TIME_SERIES_SAMPLES_FOR_AGGREGATION = [
    sample.TPM_TIME_SERIES,
    sample.OPS_TIME_SERIES,
]
PERCENTILES = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

DEGRADATION_PERCENT = flags.DEFINE_float(
    'maintenance_degradation_percent',
    95.0,
    (
        'Percentage to consider a given second is a degradation. This should'
        ' set to (1 - max variance) of the benchmark. I.e if the benchmark have'
        ' max variance of 5%, this should be set to 95.'
    ),
)

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

MAINTENANCE_DEGRADATION_WINDOW = flags.DEFINE_float(
    'maintenance_degradation_window',
    None,
    'Multiple of LM duration to consider the degradation after LM starts.',
)

CAPTURE_LIVE_MIGRATION_TIMESTAMPS = flags.DEFINE_boolean(
    'capture_live_migration_timestamps',
    False,
    (
        'Whether to capture maintenance times during migration. '
        'This requires external python script for notification.'
    ),
)


class MaintenanceEventTrigger(base_time_trigger.BaseTimeTrigger):
  """Class contains logic for triggering maintenance events."""

  def __init__(self):
    super().__init__(SIMULATE_MAINTENANCE_DELAY.value)
    self.capture_live_migration_timestamps = (
        CAPTURE_LIVE_MIGRATION_TIMESTAMPS.value
    )
    self.lm_ends = None

  def TriggerMethod(self, vm: virtual_machine.VirtualMachine):
    if self.capture_live_migration_timestamps:
      vm.StartLMNotification()
    if SIMULATE_MAINTENANCE_WITH_LOG.value:
      vm.SimulateMaintenanceWithLog()  # pytype: disable=attribute-error
    else:
      vm.SimulateMaintenanceEvent()

  def SetUp(self):
    """Base class."""
    if self.capture_live_migration_timestamps:
      for vm in self.vms:
        vm.SetupLMNotification()

  def AppendSamples(
      self,
      unused_sender,
      benchmark_spec: bm_spec.BenchmarkSpec,
      samples: List[sample.Sample],
  ):
    """Append samples related to Live Migration."""

    def generate_lm_total_time_samples() -> List[sample.Sample]:
      lm_event_dicts = []
      if self.capture_live_migration_timestamps:
        # Block test exit until LM ended.
        for vm in self.vms:
          vm.WaitLMNotificationRelease()
          lm_event_dicts.append(vm.CollectLMNotificationsTime())
      else:
        return []

      # Host maintenance is in s
      self.lm_ends = max(
          [float(d['Host_maintenance_end']) * 1000 for d in lm_event_dicts],
          default=0,
      )

      # Populate the run_number "LM Total Time" by copying the metadata from
      # (one of) the existing samples. Ideally pkb.DoRunPhase() would have sole
      # responsibility for populating run_number for all samples, but making
      # that change might be risky.
      sample_metadata = (
          copy.deepcopy(samples[0].metadata) if len(samples) > 0 else {}
      )

      return [
          sample.Sample(
              'LM Total Time',
              d['LM_total_time'],
              'seconds',
              sample_metadata | d,
          )
          for d in lm_event_dicts
      ]

    samples.extend(generate_lm_total_time_samples())
    self._AppendAggregatedMetrics(samples)

  def _AppendAggregatedMetrics(self, samples: List[sample.Sample]):
    """Finds the time series samples and add generate the aggregated metrics."""
    additional_samples = []
    for s in samples:
      if s.metric in TIME_SERIES_SAMPLES_FOR_AGGREGATION:
        additional_samples += self._AggregateThroughputSample(s)
    samples.extend(additional_samples)

  def _AggregateThroughputSample(self, s: sample.Sample) -> List[sample.Sample]:
    """Aggregate a time series sample into Live migration metrics.

    Split the samples and compute mean and median and calls relevant
    methods to generate an aggregated sample based on the time series sample.

    Args:
      s: A time series sample create using CreateTimeSeriesSample in samples.py

    Returns:
      A list of samples.
    """
    metadata = copy.deepcopy(s.metadata)
    time_series = metadata['timestamps']
    values = metadata['values']
    interval = metadata['interval']

    # Default ramp up starts and ramp down starts if the benchmark does not
    # provide it in the metadata.
    ramp_up_ends = time_series[0]
    ramp_down_starts = time_series[-1]
    lm_ends = time_series[-1]
    if sample.RAMP_DOWN_STARTS in metadata:
      ramp_down_starts = metadata[sample.RAMP_DOWN_STARTS]
    if sample.RAMP_UP_ENDS in metadata:
      ramp_up_ends = metadata[sample.RAMP_UP_ENDS]

    if self.capture_live_migration_timestamps:
      # lm ends is computed from LM notification
      lm_ends = self.lm_ends

    lm_start = sample.ConvertDateTimeToUnixMs(self.trigger_time)

    lm_duration = lm_ends - lm_start

    base_line_values = []
    values_after_lm_starts = []
    values_after_lm_ends = []
    total_missing_seconds = 0
    for i in range(len(values)):
      time = time_series[i]
      if time >= ramp_up_ends and time <= ramp_down_starts:
        interval_values = []
        # If more than 1 sequential value is missing from the time series.
        # Distrubute the ops throughout the time series
        if i > 0:
          time_gap_in_seconds = (time - time_series[i - 1]) / 1000
          missing_entry_count = int((time_gap_in_seconds / interval) - 1)
          if missing_entry_count > 1:
            total_missing_seconds += missing_entry_count * interval
            interval_values.extend(
                [int(values[i] / float(missing_entry_count + 1))]
                * (missing_entry_count + 1)
            )

          else:
            interval_values.append(values[i])
        if time <= lm_start:
          base_line_values.extend(interval_values)
        else:
          if MAINTENANCE_DEGRADATION_WINDOW.value is not None:
            window = MAINTENANCE_DEGRADATION_WINDOW.value
            assert 1 <= window <= 10, (
                'maintenance_degradation_window must be between 1 and 10'
                f' inclusive, or None. Got: {window}'
            )
            if time <= lm_start + lm_duration * window:
              values_after_lm_starts.extend(interval_values)
          else:
            values_after_lm_starts.extend(interval_values)

        if time > lm_ends:
          values_after_lm_ends.extend(interval_values)

    median = statistics.median(base_line_values)
    mean = statistics.mean(base_line_values)

    logging.info('LM Baseline median: %s', median)
    logging.info('LM Baseline mean: %s', mean)

    # Keep the metadata from the original sample except time series metadata
    for field in sample.TIME_SERIES_METADATA:
      if field in metadata:
        del metadata[field]

    samples = self._ComputeLossPercentile(
        mean, values_after_lm_starts, metadata
    ) + self._ComputeLossWork(
        median, values_after_lm_starts, interval, metadata
    )

    if values_after_lm_ends:
      mean_after_lm_ends = statistics.mean(values_after_lm_ends)
      samples += self._ComputeDegradation(mean, mean_after_lm_ends, metadata)
      logging.info('Mean after LM ends: %s', mean_after_lm_ends)
      logging.info(
          'Number of samples after LM ends: %s', len(values_after_lm_ends)
      )

    # Seconds that are missing i.e without throughput.
    samples.append(
        sample.Sample(
            'total_missing_seconds',
            total_missing_seconds,
            's',
            metadata=metadata,
        )
    )
    return samples

  def _ComputeLossPercentile(
      self, mean: float, values_after_lm: List[float], metadata: Dict[str, Any]
  ) -> List[sample.Sample]:
    """Compute loss percentile metrics.

    This method samples of seconds_dropped_below_x_percent from 0% to 90%
    in 10 percent increment. This is computed by a nested for loop and
    comparing if value dropped below a given percentile.

    Args:
      mean: Mean of the baseline
      values_after_lm: List of samples after Live migration.
      metadata: Metadata for samples

    Returns:
      Samples of loss percentile metrics.
    """
    number_of_seconds_dropped_below_percentile = collections.defaultdict(int)
    for value in values_after_lm:
      for p in PERCENTILES:
        if value <= mean * p:
          number_of_seconds_dropped_below_percentile[p] += 1

    samples = []
    for p in PERCENTILES:
      samples.append(
          sample.Sample(
              f'seconds_dropped_below_{int(p * 100)}_percent',
              number_of_seconds_dropped_below_percentile[p],
              's',
              metadata=metadata,
          )
      )
    return samples

  def _ComputeLossWork(
      self,
      median: float,
      values_after_lm: List[float],
      interval: float,
      metadata: Dict[str, Any],
  ):
    """Compute the loss work metrics for Live Migration.

    This method returns two metrics.
    1. The totl loss seconds. This is defined as the loss time across the LM.
    It is the sum of (Median - value) / median * interval
    2. Unresponsive metrics. This is the cube of the lost time to degrade
    the impact of LM in the higher percentile.
    It is the sum of  ((Median - value) / median) ** 3 * interval

    Args:
      median: median of the baseline
      values_after_lm: List of samples after LM
      interval: Interval of the metrics.
      metadata: Metadata for samples

    Returns:
      List of samples.
    """
    total_loss_seconds = 0
    unresponsive_metric = 0
    for value in values_after_lm:
      if value < median * DEGRADATION_PERCENT.value / 100.0:
        total_loss_seconds += (median - value) / median * interval
        unresponsive_metric += (((median - value) / median) ** (3.0)) * interval

    samples = []
    samples.append(
        sample.Sample(
            'unresponsive_metric',
            round(unresponsive_metric, 4),
            'metric',
            metadata=metadata,
        )
    )
    samples.append(
        sample.Sample(
            'total_loss_seconds',
            round(total_loss_seconds, 4),
            'seconds',
            metadata=metadata,
        )
    )
    return samples

  def _ComputeDegradation(
      self, baseline_mean: float, mean: float, metadata: Dict[str, Any]
  ) -> List[sample.Sample]:
    """Compute the degradation after LM ends to baseline."""
    return [
        sample.Sample(
            'degradation_percent',
            round((baseline_mean - mean) / baseline_mean * 100, 4),
            '%',
            metadata=metadata,
        )
    ]

  @property
  def trigger_name(self) -> str:
    return 'simulate_maintenance'


def Register(parsed_flags):
  """Registers the simulate maintenance trigger if FLAGS.simulate_maintenance is set."""
  if not parsed_flags.simulate_maintenance:
    return
  trigger = MaintenanceEventTrigger()
  trigger.Register()
