# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.linux_packages.hpcc."""

import os
import tempfile
import unittest

import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hpcc

FAKE_HPCC_MAIN_SOURCE = """/* comments redacted */

#include <redacted.h>

int
main(int argc, char *argv[]) {
  int redacted;
  MPI_Init( &argc, &argv );

  if (redacted)
    goto hpcc_end;

  /* -------------------------------------------------- */
  /*                 MPI RandomAccess                   */
  /* -------------------------------------------------- */

  MPI RandomAccess line 1 redacted.
  MPI RandomAccess line 2 redacted.

  /* -------------------------------------------------- */
  /*                  StarRandomAccess                  */
  /* -------------------------------------------------- */

  StarRandomAccess line 1 redacted.
  StarRandomAccess line 2 redacted.

  /* -------------------------------------------------- */
  /*                 SingleRandomAccess                 */
  /* -------------------------------------------------- */

  SingleRandomAccess line 1 redacted.
  SingleRandomAccess line 2 redacted.

  hpcc_end:

  redacted();
  return 0;
}
"""


class HpccTest(unittest.TestCase):

  def _RunLimitBenchmarksToRun(self, selected_hpcc_benchmarks):
    """Calls _LimitBenchmarksToRun for tests."""
    self._temp_dir = tempfile.mkdtemp()
    with open(os.path.join(self._temp_dir, 'hpcc.c'), 'w+') as f:
      f.write(FAKE_HPCC_MAIN_SOURCE)
    with mock.patch(vm_util.__name__ + '.GetTempDir') as mock_gettempdir:
      mock_gettempdir.return_value = self._temp_dir
      hpcc._LimitBenchmarksToRun(mock.Mock(), selected_hpcc_benchmarks)

  def _ValidateRunLimitBenchmarksToRun(self, expected_lines):
    """Validates that expected lines are found in the modified hpcc.c file.

    Args:
      expected_lines: An iterable of expected lines of code.
    """
    expected_lines = set(expected_lines)
    with open(os.path.join(self._temp_dir, 'hpcc.c')) as f:
      for line in f:
        expected_lines.discard(line.rstrip())
    if expected_lines:
      self.fail('Unexpected lines in hpcc.c: %s' % expected_lines)

  def testLimitBenchmarksToRunToFirstBenchmark(self):
    """Tests limiting the benchmarks to run to the first benchmark."""
    self._RunLimitBenchmarksToRun(set(['MPI RandomAccess']))
    self._ValidateRunLimitBenchmarksToRun([
        '  MPI_Init( &argc, &argv );',
        '  if (redacted)',
        '    goto hpcc_end;',
        '  MPI RandomAccess line 1 redacted.',
        '  MPI RandomAccess line 2 redacted.',
        '//   StarRandomAccess line 1 redacted.',
        '//   StarRandomAccess line 2 redacted.',
        '//   SingleRandomAccess line 1 redacted.',
        '//   SingleRandomAccess line 2 redacted.',
        '  hpcc_end:',
    ])

  def testLimitBenchmarksToRunToLastBenchmark(self):
    """Tests limiting the benchmarks to run to the first benchmark."""
    self._RunLimitBenchmarksToRun(set(['SingleRandomAccess']))
    self._ValidateRunLimitBenchmarksToRun([
        '  MPI_Init( &argc, &argv );',
        '  if (redacted)',
        '    goto hpcc_end;',
        '//   MPI RandomAccess line 1 redacted.',
        '//   MPI RandomAccess line 2 redacted.',
        '//   StarRandomAccess line 1 redacted.',
        '//   StarRandomAccess line 2 redacted.',
        '  SingleRandomAccess line 1 redacted.',
        '  SingleRandomAccess line 2 redacted.',
        '  hpcc_end:',
    ])

  def testLimitBenchmarksToRunToMultipleBenchmarks(self):
    """Tests limiting the benchmarks to run to the first benchmark."""
    self._RunLimitBenchmarksToRun(
        set(['StarRandomAccess', 'SingleRandomAccess']))
    self._ValidateRunLimitBenchmarksToRun([
        '  MPI_Init( &argc, &argv );',
        '  if (redacted)',
        '    goto hpcc_end;',
        '//   MPI RandomAccess line 1 redacted.',
        '//   MPI RandomAccess line 2 redacted.',
        '  StarRandomAccess line 1 redacted.',
        '  StarRandomAccess line 2 redacted.',
        '  SingleRandomAccess line 1 redacted.',
        '  SingleRandomAccess line 2 redacted.',
        '  hpcc_end:',
    ])


if __name__ == '__main__':
  unittest.main()
