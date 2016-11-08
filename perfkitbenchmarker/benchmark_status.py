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


def _CreateSummaryTable(benchmark_specs):
  """Converts statuses of benchmark runs into a formatted string table.

  Args:
    benchmark_specs: List of BenchmarkSpecs.

  Returns:
    string. Multi-line string summarizing benchmark success statuses. Example:
        --------------------------------------
        Name          UID            Status
        --------------------------------------
        iperf         iperf0         SUCCEEDED
        iperf         iperf1         FAILED
        cluster_boot  cluster_boot0  SKIPPED
        --------------------------------------
  """
  run_status_tuples = [(spec.name, spec.uid, spec.status)
                       for spec in benchmark_specs]
  assert run_status_tuples, ('run_status_tuples must contain at least one '
                             'element.')
  col_headers = 'Name', 'UID', 'Status'
  col_lengths = []
  for col_header, col_entries in zip(col_headers, zip(*run_status_tuples)):
    max_col_content_length = max(len(entry) for entry in col_entries)
    col_lengths.append(max(len(col_header), max_col_content_length))
  line_length = (len(col_headers) - 1) * len(_COL_SEPARATOR) + sum(col_lengths)
  dash_line = '-' * line_length
  line_format = _COL_SEPARATOR.join(
      '{{{0}:<{1}s}}'.format(col_index, col_length)
      for col_index, col_length in enumerate(col_lengths))
  msg = [dash_line, line_format.format(*col_headers), dash_line]
  msg.extend(line_format.format(*row_entries)
             for row_entries in run_status_tuples)
  msg.append(dash_line)
  return os.linesep.join(msg)


def CreateSummary(benchmark_specs):
  """Logs a summary of benchmark run statuses.

  Args:
    benchmark_specs: List of BenchmarkSpecs.

  Returns:
    string. Multi-line string summarizing benchmark success statuses. Example:
        Benchmark run statuses:
        --------------------------------------
        Name          UID            Status
        --------------------------------------
        iperf         iperf0         SUCCEEDED
        iperf         iperf1         FAILED
        cluster_boot  cluster_boot0  SKIPPED
        --------------------------------------
        Success rate: 33.33% (1/3)
  """
  run_status_tuples = [(spec.name, spec.uid, spec.status)
                       for spec in benchmark_specs]
  assert run_status_tuples, ('run_status_tuples must contain at least one '
                             'element.')
  benchmark_count = len(run_status_tuples)
  successful_benchmark_count = sum(1 for _, _, status in run_status_tuples
                                   if status == SUCCEEDED)
  return os.linesep.join((
      'Benchmark run statuses:',
      _CreateSummaryTable(benchmark_specs),
      'Success rate: {0:.2f}% ({1}/{2})'.format(
          100. * successful_benchmark_count / benchmark_count,
          successful_benchmark_count, benchmark_count)))
