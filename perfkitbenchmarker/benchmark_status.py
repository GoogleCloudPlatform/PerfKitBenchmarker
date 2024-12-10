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
"""Constants and helpers for reporting the success status of each benchmark."""

import os

SUCCEEDED = 'SUCCEEDED'
FAILED = 'FAILED'
SKIPPED = 'SKIPPED'

ALL = SUCCEEDED, FAILED, SKIPPED

_COL_SEPARATOR = '  '


class FailedSubstatus:
  """Failure modes for benchmarks."""

  # Failure due to insufficient quota, user preventable
  QUOTA = 'QUOTA_EXCEEDED'

  # Failure due to insufficient capacity in the cloud provider, user
  # non-preventable.
  INSUFFICIENT_CAPACITY = 'INSUFFICIENT_CAPACITY'

  # Failure during the execution of the benchmark. These are non-retryable,
  # known failure modes of the benchmark.  It is recommended that the benchmark
  # be completely re-run.
  KNOWN_INTERMITTENT = 'KNOWN_INTERMITTENT'

  # Failure due to an interruptible vm being interrupted before the benchmark
  # completes. User non-preventable.
  INTERRUPTED = 'INTERRUPTED'

  # Failure due to an unsupported configuration running. Ex. Machine type not
  # supported in a zone. For retries, this will pick a new region to maximize
  # the chance of success.
  UNSUPPORTED = 'UNSUPPORTED'

  # Failure as process received a SIGINT or SIGTERM signal. This can be caused
  # by manual Ctrl-C or a few forms of timeout - blaze sends it when
  # its test_timeout flag is reached, as does artemis for timeout_sec. The
  # reaper may (possibly, unproven) also kill VMs which have an artemis process
  # running on them, which may also send this signal.
  PROCESS_KILLED = 'PROCESS_KILLED'

  # General failure that don't fit in the above categories.
  UNCATEGORIZED = 'UNCATEGORIZED'

  # Failure when restoring resource.
  RESTORE_FAILED = 'RESTORE_FAILED'

  # Failure when freezing resource.
  FREEZE_FAILED = 'FREEZE_FAILED'

  # Failure when a retryable command execution times out.
  COMMAND_TIMEOUT = 'COMMAND_TIMEOUT'

  # Failure when a retryable command execution exceeds the retry limit.
  RETRIES_EXCEEDED = 'RETRIES_EXCEEDED'

  # Failure when config values are invalid.
  INVALID_VALUE = 'INVALID_VALUE'

  # List of valid substatuses for use with --retries.
  # UNCATEGORIZED failures are not retryable. To make a specific UNCATEGORIZED
  # failure retryable, please raise an errors.Benchmarks.KnownIntermittentError.
  # RESTORE_FAILED/FREEZE_FAILED failures are not retryable since generally
  # logic for freeze/restore is already retried in the BaseResource
  # Create()/Delete().
  RETRYABLE_SUBSTATUSES = [
      QUOTA,
      INSUFFICIENT_CAPACITY,
      KNOWN_INTERMITTENT,
      INTERRUPTED,
      UNSUPPORTED,
      COMMAND_TIMEOUT,
      RETRIES_EXCEEDED,
  ]


def _CreateSummaryTable(benchmark_specs):
  """Converts statuses of benchmark runs into a formatted string table.

  Args:
    benchmark_specs: List of BenchmarkSpecs.

  Returns:
    string. Multi-line string summarizing benchmark success statuses. Example:
        --------------------------------------------------------
        Name          UID            Status     Failed Substatus
        --------------------------------------------------------
        iperf         iperf0         SUCCEEDED
        iperf         iperf1         FAILED
        iperf         iperf2         FAILED     QUOTA_EXCEEDED
        cluster_boot  cluster_boot0  SKIPPED
        --------------------------------------------------------
  """
  run_status_tuples = [
      (
          spec.name,
          spec.uid,
          spec.status,
          spec.failed_substatus if spec.failed_substatus else '',
      )
      for spec in benchmark_specs
  ]
  assert (
      run_status_tuples
  ), 'run_status_tuples must contain at least one element.'
  col_headers = 'Name', 'UID', 'Status', 'Failed Substatus'
  col_lengths = []
  for col_header, col_entries in zip(
      col_headers, list(zip(*run_status_tuples))
  ):
    max_col_content_length = max(len(entry) for entry in col_entries)
    col_lengths.append(max(len(col_header), max_col_content_length))
  line_length = (len(col_headers) - 1) * len(_COL_SEPARATOR) + sum(col_lengths)
  dash_line = '-' * line_length
  line_format = _COL_SEPARATOR.join(
      '{{{0}:<{1}s}}'.format(col_index, col_length)
      for col_index, col_length in enumerate(col_lengths)
  )
  msg = [dash_line, line_format.format(*col_headers), dash_line]
  msg.extend(
      line_format.format(*row_entries) for row_entries in run_status_tuples
  )
  msg.append(dash_line)
  return os.linesep.join(msg)


def CreateSummary(benchmark_specs):
  """Logs a summary of benchmark run statuses.

  Args:
    benchmark_specs: List of BenchmarkSpecs.

  Returns:
    string. Multi-line string summarizing benchmark success statuses. Example:
        Benchmark run statuses:
        --------------------------------------------------------
        Name          UID            Status     Failed Substatus
        --------------------------------------------------------
        iperf         iperf0         SUCCEEDED
        iperf         iperf1         FAILED
        iperf         iperf2         FAILED     QUOTA_EXCEEDED
        cluster_boot  cluster_boot0  SKIPPED
        --------------------------------------------------------
        Success rate: 25.00% (1/4)
  """
  run_status_tuples = [
      (spec.name, spec.uid, spec.status) for spec in benchmark_specs
  ]
  assert (
      run_status_tuples
  ), 'run_status_tuples must contain at least one element.'
  benchmark_count = len(run_status_tuples)
  successful_benchmark_count = sum(
      1 for _, _, status in run_status_tuples if status == SUCCEEDED
  )
  return os.linesep.join((
      'Benchmark run statuses:',
      _CreateSummaryTable(benchmark_specs),
      'Success rate: {:.2f}% ({}/{})'.format(
          100.0 * successful_benchmark_count / benchmark_count,
          successful_benchmark_count,
          benchmark_count,
      ),
  ))
