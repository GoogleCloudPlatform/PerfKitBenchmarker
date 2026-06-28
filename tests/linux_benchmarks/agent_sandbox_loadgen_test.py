# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for the agent_sandbox load generator."""

import threading
import unittest

from perfkitbenchmarker.linux_benchmarks import agent_sandbox_loadgen
from tests import pkb_common_test_case


class ResolveRunShapeTest(pkb_common_test_case.PkbCommonTestCase):

  def testQpsAndDurationDeriveTotal(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, duration=5.0)
    self.assertEqual(shape.qps, 10.0)
    self.assertEqual(shape.duration, 5.0)
    self.assertEqual(shape.total, 50)

  def testQpsAndTotalDeriveDuration(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=100)
    self.assertEqual(shape.total, 100)
    self.assertAlmostEqual(shape.duration, 10.0)

  def testDurationAndTotalDeriveQps(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(duration=4.0, total=20)
    self.assertEqual(shape.total, 20)
    self.assertEqual(shape.duration, 4.0)
    self.assertAlmostEqual(shape.qps, 5.0)

  def testAllThreeProvidedPassesThrough(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(
        qps=2.0, duration=3.0, total=999
    )
    self.assertEqual(shape.qps, 2.0)
    self.assertEqual(shape.duration, 3.0)
    self.assertEqual(shape.total, 999)

  def testFewerThanTwoArgsRaises(self):
    with self.assertRaises(ValueError):
      agent_sandbox_loadgen.resolve_run_shape(qps=5.0)

  def testNoArgsRaises(self):
    with self.assertRaises(ValueError):
      agent_sandbox_loadgen.resolve_run_shape()

  def testTotalIsRoundedInt(self):
    # qps=3, duration=2 -> 6.0 exact
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=3.0, duration=2.0)
    self.assertIsInstance(shape.total, int)
    self.assertEqual(shape.total, 6)

  def testQpsAndDurationRoundsTotal(self):
    # 10 * 0.33 = 3.3 -> rounds to 3
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, duration=0.33)
    self.assertIsInstance(shape.total, int)
    self.assertEqual(shape.total, 3)


class ClaimRecordTest(pkb_common_test_case.PkbCommonTestCase):

  def testStartupTimeWhenReadyAtSet(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c0', requested_at=100.0, ready_at=102.5
    )
    self.assertAlmostEqual(rec.startup_time_s, 2.5)

  def testStartupTimeIsNoneWhenReadyAtUnset(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=100.0)
    self.assertIsNone(rec.startup_time_s)

  def testExecDuration(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c1',
        requested_at=100.0,
        exec_started_at=103.0,
        exec_completed_at=108.0,
    )
    self.assertAlmostEqual(rec.exec_duration_s, 5.0)

  def testExecDurationNoneWhenStartedAtUnset(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c1', requested_at=100.0, exec_completed_at=108.0
    )
    self.assertIsNone(rec.exec_duration_s)

  def testExecDurationNoneWhenCompletedAtUnset(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c1', requested_at=100.0, exec_started_at=103.0
    )
    self.assertIsNone(rec.exec_duration_s)

  def testTotalLifecycle(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c2',
        requested_at=100.0,
        released_at=110.0,
    )
    self.assertAlmostEqual(rec.total_lifecycle_s, 10.0)

  def testTotalLifecycleNoneWhenReleasedAtUnset(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c2', requested_at=100.0)
    self.assertIsNone(rec.total_lifecycle_s)

  def testAllTimingsTogether(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c3',
        requested_at=100.0,
        ready_at=102.0,
        exec_started_at=103.0,
        exec_completed_at=107.0,
        released_at=109.0,
    )
    self.assertAlmostEqual(rec.startup_time_s, 2.0)
    self.assertAlmostEqual(rec.exec_duration_s, 4.0)
    self.assertAlmostEqual(rec.total_lifecycle_s, 9.0)


class LoadGeneratorRunTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests LoadGenerator.run() using a fake in-memory driver.

  The fake driver:
  - create(): records the name and immediately marks it ready (via the
    stream_claim_events generator that the watch thread drains).
  - stream_claim_events(): yields one Ready event per created claim, then
    blocks on the stop_event so the watch thread exits cleanly.
  - is_ready(): always True.
  - served_warm(): always False.
  - delete(): no-op.
  """

  def _make_driver(self, ready_at_delay=0.0):
    """Builds a fake ClaimDriver-like object."""
    driver = _FakeDriver(ready_at_delay=ready_at_delay)
    return driver

  def testRunReturnOneRecordPerClaim(self):
    driver = self._make_driver()
    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
    )
    counter = [0.0]

    def clock():
      counter[0] += 0.01
      return counter[0]

    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=3)
    records = lg.run(shape, clock=clock, sleeper=lambda _: None)

    self.assertLen(records, 3)
    self.assertEqual([r.name for r in records], ['claim-0', 'claim-1', 'claim-2'])

  def testRunRecordsReadyAt(self):
    # Asserting ready_at is recorded depends on the watch-consumer thread
    # observing the Ready event before the drain loop times out. A mocked clock
    # plus a no-op sleeper lets the main thread blow through the logical drain
    # deadline in microseconds of wall-clock time, before the real watch thread
    # records ready_at (a test-only race). Use the real monotonic clock and real
    # sleep (the run() defaults) so the drain window is a real 5s, which the
    # watch thread reaches in milliseconds. The drain loop still exits early once
    # both claims are accounted, so this runs in a fraction of a second.
    driver = self._make_driver()
    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
    )

    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=2)
    records = lg.run(shape)

    for rec in records:
      self.assertIsNotNone(
          rec.ready_at,
          msg=f'ready_at should be set for {rec.name}',
      )
      self.assertIsNone(rec.error)


class _FakeDriver:
  """Minimal in-memory fake for ClaimDriver.

  Implements the interface consumed by LoadGenerator:
    - create(name)
    - stream_claim_events(stop_event) -> iterator of {'name', 'status'}
    - is_ready(status) -> bool
    - served_warm(status) -> bool or None
    - delete(name)
  """

  _READY_STATUS = {'conditions': [{'type': 'Ready', 'status': 'True'}]}

  def __init__(self, ready_at_delay=0.0):
    self._ready_at_delay = ready_at_delay
    self._queue = []
    self._queue_cv = threading.Condition()

  def create(self, name):
    with self._queue_cv:
      self._queue.append(name)
      self._queue_cv.notify_all()

  def stream_claim_events(self, stop_event):
    seen = 0
    while not stop_event.is_set():
      with self._queue_cv:
        while len(self._queue) <= seen and not stop_event.is_set():
          self._queue_cv.wait(timeout=0.05)
        if stop_event.is_set():
          return
        batch = self._queue[seen:]
        seen += len(batch)
      for name in batch:
        yield {'name': name, 'status': self._READY_STATUS}

  def is_ready(self, status):
    return True

  def served_warm(self, status):
    return False

  def delete(self, name):
    pass


if __name__ == '__main__':
  unittest.main()
