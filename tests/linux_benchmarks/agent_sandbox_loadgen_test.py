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

from perfkitbenchmarker.data.agent_sandbox import agent_sandbox_loadgen
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
    assert rec.startup_time_s is not None
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
    assert rec.exec_duration_s is not None
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
    assert rec.total_lifecycle_s is not None
    self.assertAlmostEqual(rec.total_lifecycle_s, 10.0)

  def testTotalLifecycleNoneWhenReleasedAtUnset(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c2', requested_at=100.0)
    self.assertIsNone(rec.total_lifecycle_s)

  def testDeleteDuration(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c2',
        requested_at=100.0,
        exec_completed_at=107.5,
        released_at=109.0,
    )
    assert rec.delete_duration_s is not None
    self.assertAlmostEqual(rec.delete_duration_s, 1.5)

  def testDeleteDurationNoneWhenMissingTimestamps(self):
    rec1 = agent_sandbox_loadgen.ClaimRecord(
        name='c2', requested_at=100.0, released_at=109.0
    )
    self.assertIsNone(rec1.delete_duration_s)
    rec2 = agent_sandbox_loadgen.ClaimRecord(
        name='c2', requested_at=100.0, exec_completed_at=107.5
    )
    self.assertIsNone(rec2.delete_duration_s)

  def testAllTimingsTogether(self):
    rec = agent_sandbox_loadgen.ClaimRecord(
        name='c3',
        requested_at=100.0,
        ready_at=102.0,
        exec_started_at=103.0,
        exec_completed_at=107.0,
        released_at=109.0,
    )
    assert rec.startup_time_s is not None
    self.assertAlmostEqual(rec.startup_time_s, 2.0)
    assert rec.exec_duration_s is not None
    self.assertAlmostEqual(rec.exec_duration_s, 4.0)
    assert rec.delete_duration_s is not None
    self.assertAlmostEqual(rec.delete_duration_s, 2.0)
    assert rec.total_lifecycle_s is not None
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
    self.assertEqual(
        [r.name for r in records], ['claim-0', 'claim-1', 'claim-2']
    )

  def testRunCustomClaimPrefix(self):
    driver = self._make_driver()
    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
        claim_prefix='claim-pod-a',
    )
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=2)
    records = lg.run(shape)
    self.assertEqual(
        [r.name for r in records], ['claim-pod-a-0', 'claim-pod-a-1']
    )

  def testRunRecordsReadyAt(self):
    # Asserting ready_at is recorded depends on the watch-consumer thread
    # observing the Ready event before the drain loop times out. A mocked clock
    # plus a no-op sleeper lets the main thread blow through the logical drain
    # deadline in microseconds of wall-clock time, before the real watch thread
    # records ready_at (a test-only race). Use the real monotonic clock and real
    # sleep (the run() defaults) so the drain window is a real 5s, which the
    # watch thread reaches in milliseconds. The drain loop still exits early
    # once both claims are accounted, so this runs in a fraction of a second.
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

  def testFailedHoldDeleteRetriedInCleanup(self):
    driver = self._make_driver()
    delete_attempts = []

    def flaky_delete(name):
      delete_attempts.append(name)
      if len(delete_attempts) == 1:
        raise RuntimeError('transient API error')

    driver.delete = flaky_delete
    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
    )
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=1)
    records = lg.run(shape)

    self.assertLen(records, 1)
    self.assertIsNone(records[0].error)
    # First delete in _hold_task failed, second delete in cleanup succeeded.
    self.assertEqual(delete_attempts, ['claim-0', 'claim-0'])

  def testReadyBeforeCreateReturnsTracksPeakConcurrency(self):
    driver = self._make_driver()
    orig_create = driver.create

    def slow_return_create(name, sleeper=None):
      orig_create(name, sleeper=sleeper)
      # Yield briefly so the watch consumer and hold task can finish before
      # create() returns to _create_task.
      threading.Event().wait(0.05)

    driver.create = slow_return_create
    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
    )
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=1)
    records = lg.run(shape)

    self.assertLen(records, 1)
    self.assertIsNotNone(records[0].ready_at)
    self.assertGreaterEqual(lg.peak_concurrency, 1)

  def testWorkloadExecErrorRecordedInClaimRecord(self):
    driver = self._make_driver()

    class _FailingExecutor(agent_sandbox_loadgen.WorkloadExecutor):

      def execute(self, claim_name, status, prepared):
        del claim_name, status, prepared
        raise RuntimeError('exec boom')

    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
        workload_executor=_FailingExecutor(),
    )
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=2)
    records = lg.run(shape)

    self.assertLen(records, 2)
    for rec in records:
      self.assertEqual(rec.error, 'RuntimeError')
      self.assertIsNone(rec.exec_duration_s)
      self.assertIsNotNone(rec.released_at)

  def testPrepareRunsOnHoldWorkerThread(self):
    driver = self._make_driver()
    prepare_threads = []

    class _TrackingExecutor(agent_sandbox_loadgen.WorkloadExecutor):

      def prepare(self, claim_name, status):
        del claim_name, status
        prepare_threads.append(threading.current_thread().name)
        return 'pod-0'

      def execute(self, claim_name, status, prepared):
        del claim_name, status
        assert prepared == 'pod-0'

    lg = agent_sandbox_loadgen.LoadGenerator(
        driver=driver,
        ready_timeout=5.0,
        max_concurrent=4,
        workload_executor=_TrackingExecutor(),
    )
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10.0, total=1)
    records = lg.run(shape)

    self.assertLen(records, 1)
    self.assertIsNone(records[0].error)
    self.assertLen(prepare_threads, 1)
    self.assertStartsWith(prepare_threads[0], 'claim-hold')


class _FakeDriver(agent_sandbox_loadgen.ClaimDriver):
  """Minimal in-memory fake for ClaimDriver.

  Implements the interface consumed by LoadGenerator:
    - create(name)
    - stream_claim_events(stop_event) -> iterator of {'name', 'status'}
    - is_ready(status) -> bool
    - served_warm(status) -> bool or None
    - delete(name)
  """

  _READY_STATUS = {'conditions': [{'type': 'Ready', 'status': 'True'}]}

  def __init__(self, ready_at_delay=0.0):  # pylint: disable=super-init-not-called
    self._ready_at_delay = ready_at_delay
    self._queue = []
    self._queue_cv = threading.Condition()

  def create(self, name, sleeper=None):
    del sleeper
    with self._queue_cv:
      self._queue.append(name)
      self._queue_cv.notify_all()

  def stream_claim_events(self, stop_event, timeout_seconds=5):
    del timeout_seconds
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
    del status
    return True

  def served_warm(self, status):
    del status
    return False

  def delete(self, name):
    del name


class ClaimDriverReadyTest(pkb_common_test_case.PkbCommonTestCase):

  def testIsReadyHandlesNoneConditions(self):
    driver = _FakeDriver()
    self.assertFalse(
        agent_sandbox_loadgen.ClaimDriver.is_ready(driver, {'conditions': None})
    )
    self.assertFalse(agent_sandbox_loadgen.ClaimDriver.is_ready(driver, {}))
    self.assertTrue(
        agent_sandbox_loadgen.ClaimDriver.is_ready(
            driver,
            {'conditions': [{'type': 'Ready', 'status': 'True'}]},
        )
    )

  def testShardedWarmpoolRoundRobinAndServedWarm(self):
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        warmpool_name='agent-sandbox-0,agent-sandbox-1',
        load_config=False,
    )
    b0 = driver._body('claim-0')
    b1 = driver._body('claim-1')
    b2 = driver._body('claim-2')
    self.assertEqual(b0['spec']['warmPoolRef']['name'], 'agent-sandbox-0')
    self.assertEqual(b1['spec']['warmPoolRef']['name'], 'agent-sandbox-1')
    self.assertEqual(b2['spec']['warmPoolRef']['name'], 'agent-sandbox-0')
    self.assertTrue(
        driver.served_warm({'sandbox': {'name': 'agent-sandbox-1-xyz'}})
    )
    self.assertFalse(driver.served_warm({'sandbox': {'name': 'claim-0'}}))


if __name__ == '__main__':
  unittest.main()
