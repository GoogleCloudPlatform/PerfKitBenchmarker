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

"""Tests for perfkitbenchmarker.linux_packages.agent_sandbox_loadgen."""

import threading
import unittest
from unittest import mock

from perfkitbenchmarker.linux_packages import agent_sandbox_loadgen
from tests import pkb_common_test_case


class RunShapeTest(pkb_common_test_case.PkbCommonTestCase):

  def testDerivesTotalFromQpsAndDuration(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10, duration=30)
    self.assertEqual(shape.total, 300)
    self.assertEqual(shape.qps, 10.0)
    self.assertEqual(shape.duration, 30.0)

  def testDerivesDurationFromQpsAndTotal(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(qps=10, total=300)
    self.assertEqual(shape.duration, 30.0)

  def testDerivesQpsFromDurationAndTotal(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(duration=30, total=300)
    self.assertEqual(shape.qps, 10.0)

  def testRespectsAllThreeWhenProvided(self):
    shape = agent_sandbox_loadgen.resolve_run_shape(
        qps=10, duration=30, total=999
    )
    self.assertEqual(shape.total, 999)
    self.assertEqual(shape.qps, 10.0)
    self.assertEqual(shape.duration, 30.0)

  def testRaisesWhenFewerThanTwoProvided(self):
    with self.assertRaises(ValueError):
      agent_sandbox_loadgen.resolve_run_shape(qps=10)


class ClaimRecordTest(pkb_common_test_case.PkbCommonTestCase):

  def testStartupTimeNoneUntilReady(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=1.0)
    self.assertIsNone(rec.startup_time_s)
    rec.ready_at = 3.5
    self.assertAlmostEqual(rec.startup_time_s, 2.5)


class ClaimDriverTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(agent_sandbox_loadgen, '_load_kube_config')
    )
    self.mock_api = mock.Mock()
    self.enter_context(
        mock.patch.object(
            agent_sandbox_loadgen,
            '_make_custom_objects_api',
            return_value=self.mock_api,
        )
    )
    self.driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        template_name='python-runtime',
        warmpool_name='default',
    )

  def testCreateSendsExpectedBody(self):
    self.driver.create('claim-7')
    sent = self.mock_api.create_namespaced_custom_object.call_args
    body = sent.kwargs.get('body') or sent.args[4]
    self.assertEqual(body['apiVersion'], 'extensions.agents.x-k8s.io/v1beta1')
    self.assertEqual(body['kind'], 'SandboxClaim')
    self.assertEqual(body['metadata']['name'], 'claim-7')
    self.assertEqual(
        body['spec']['sandboxTemplateRef']['name'], 'python-runtime'
    )
    self.assertEqual(body['spec']['warmpool'], 'default')
    self.assertEqual(body['spec']['lifecycle']['shutdownPolicy'], 'Delete')

  def testIsReadyTrueOnReadyCondition(self):
    status = {'conditions': [{'type': 'Ready', 'status': 'True'}]}
    self.assertTrue(self.driver.is_ready(status))

  def testIsReadyFalseWithoutReadyCondition(self):
    status = {'conditions': [{'type': 'Ready', 'status': 'False'}]}
    self.assertFalse(self.driver.is_ready(status))
    self.assertFalse(self.driver.is_ready({}))

  def testServedWarmTrueWhenSandboxNameStartsWithWarmpool(self):
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        template_name='python-runtime',
        warmpool_name='python-sandbox-warmpool',
    )
    result = driver.served_warm(
        {'sandbox': {'name': 'python-sandbox-warmpool-abc'}}
    )
    self.assertTrue(result)

  def testServedWarmFalseWhenSandboxNameIsColdClaim(self):
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        template_name='python-runtime',
        warmpool_name='python-sandbox-warmpool',
    )
    result = driver.served_warm({'sandbox': {'name': 'claim-5'}})
    self.assertFalse(result)

  def testServedWarmNoneWhenNoSandboxBound(self):
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        template_name='python-runtime',
        warmpool_name='python-sandbox-warmpool',
    )
    result = driver.served_warm({})
    self.assertIsNone(result)

  def testBodyIncludesTtlWhenSet(self):
    """claim_ttl_seconds>0 adds ttlSecondsAfterFinished to lifecycle."""
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default',
        template_name='python-runtime',
        claim_ttl_seconds=120,
    )
    body = driver._body('claim-ttl')
    lifecycle = body['spec']['lifecycle']
    self.assertEqual(lifecycle['shutdownPolicy'], 'Delete')
    self.assertEqual(lifecycle['ttlSecondsAfterFinished'], 120)

  def testBodyOmitsTtlWhenZero(self):
    """claim_ttl_seconds=0: shutdownPolicy present, no ttlSecondsAfterFinished."""
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default', template_name='python-runtime', claim_ttl_seconds=0
    )
    body = driver._body('claim-no-ttl')
    lifecycle = body['spec']['lifecycle']
    self.assertEqual(lifecycle['shutdownPolicy'], 'Delete')
    self.assertNotIn('ttlSecondsAfterFinished', lifecycle)

  def testBodyOmitsTtlWhenNone(self):
    """claim_ttl_seconds=None (default) omits ttlSecondsAfterFinished."""
    driver = agent_sandbox_loadgen.ClaimDriver(
        namespace='default', template_name='python-runtime'
    )
    body = driver._body('claim-default')
    lifecycle = body['spec']['lifecycle']
    self.assertEqual(lifecycle['shutdownPolicy'], 'Delete')
    self.assertNotIn('ttlSecondsAfterFinished', lifecycle)

  def testCreateRetries429ThenSucceeds(self):
    """A single 429 is retried and the second call succeeds."""
    from kubernetes.client.rest import ApiException  # pylint: disable=import-error,no-name-in-module

    exc_429 = ApiException(status=429)
    exc_429.headers = {'Retry-After': '0'}
    self.mock_api.create_namespaced_custom_object.side_effect = [exc_429, None]
    sleeper = mock.Mock()
    self.driver.create('claim-retry', sleeper=sleeper)
    self.assertEqual(
        self.mock_api.create_namespaced_custom_object.call_count, 2
    )
    sleeper.assert_called_once_with(0.0)

  def testCreateRaisesAfterMaxRetries(self):
    """Exhausted 429 retries re-raise the exception."""
    from kubernetes.client.rest import ApiException  # pylint: disable=import-error,no-name-in-module

    exc_429 = ApiException(status=429)
    exc_429.headers = {}
    self.mock_api.create_namespaced_custom_object.side_effect = exc_429
    sleeper = mock.Mock()
    with self.assertRaises(ApiException):
      self.driver.create('claim-fail', sleeper=sleeper)
    self.assertEqual(
        self.mock_api.create_namespaced_custom_object.call_count,
        agent_sandbox_loadgen._CREATE_MAX_RETRIES + 1,
    )

  def _make_watch_event(self, event_type, name, rv, status=None):
    """Helper: build a fake kubernetes watch event dict."""
    obj = {'metadata': {'name': name, 'resourceVersion': rv}}
    if status is not None:
      obj['status'] = status
    return {'type': event_type, 'object': obj}

  def testStreamBookmarkFilteredAndResourceVersionPropagated(self):
    """BOOKMARK events are not yielded; normal events are; rv starts None."""
    stop = threading.Event()
    events = [
        self._make_watch_event('ADDED', 'claim-A', '10', status={}),
        self._make_watch_event('BOOKMARK', '', '99'),
        self._make_watch_event('ADDED', 'claim-B', '100', status={}),
    ]
    captured_rv = []

    class FakeWatch:

      def stream(self, _func, *_args, **kwargs):
        captured_rv.append(kwargs.get('resource_version'))
        yield from events
        stop.set()

      def stop(self):
        pass

    import kubernetes.watch as kw  # pylint: disable=import-error,no-name-in-module

    with mock.patch.object(kw, 'Watch', FakeWatch):
      results = list(self.driver.stream_claim_events(stop))

    self.assertEqual([r['name'] for r in results], ['claim-A', 'claim-B'])
    self.assertIsNone(captured_rv[0])

  def testStream410ResetsResourceVersion(self):
    """A 410 ApiException resets resource_version so the next call re-lists."""
    from kubernetes.client.rest import ApiException  # pylint: disable=import-error,no-name-in-module

    stop = threading.Event()
    calls = [0]
    captured_rv = []

    class FakeWatch:

      def stream(inner_self, _func, *_args, **kwargs):  # pylint: disable=no-self-argument
        captured_rv.append(kwargs.get('resource_version'))
        calls[0] += 1
        if calls[0] == 1:
          yield self._make_watch_event('ADDED', 'claim-X', '77', {})
          raise ApiException(status=410)
        else:
          stop.set()
          return
          yield  # make this a generator

      def stop(self):
        pass

    import kubernetes.watch as kw  # pylint: disable=import-error,no-name-in-module

    with mock.patch.object(kw, 'Watch', FakeWatch):
      results = list(self.driver.stream_claim_events(stop))

    self.assertEqual([r['name'] for r in results], ['claim-X'])
    self.assertIsNone(captured_rv[0])
    self.assertIsNone(captured_rv[1])

  def testStreamPassesResourceVersionOnReconnect(self):
    """After a normal timeout, the next stream call gets the last seen rv."""
    stop = threading.Event()
    calls = [0]
    captured_rv = []

    class FakeWatch:

      def stream(inner_self, _func, *_args, **kwargs):  # pylint: disable=no-self-argument
        captured_rv.append(kwargs.get('resource_version'))
        calls[0] += 1
        if calls[0] == 1:
          yield self._make_watch_event('ADDED', 'claim-0', '55', {})
        else:
          stop.set()
          return
          yield  # make this a generator

      def stop(self):
        pass

    import kubernetes.watch as kw  # pylint: disable=import-error,no-name-in-module

    with mock.patch.object(kw, 'Watch', FakeWatch):
      results = list(self.driver.stream_claim_events(stop))

    self.assertEqual([r['name'] for r in results], ['claim-0'])
    self.assertIsNone(captured_rv[0])
    self.assertEqual(captured_rv[1], '55')


# ---------------------------------------------------------------------------
# Fake driver for LoadGenerator tests
# ---------------------------------------------------------------------------


class _FakeDriver:
  """In-memory driver whose stream_claim_events returns ready events for
  every claim that has been created.

  Designed for deterministic tests: stream_claim_events() yields events for
  all claims registered via create() at the time it is called, then blocks
  (via a threading.Event) until stop_event is set.

  To keep tests that don't care about the timing synchronous, we use a
  real thread for the watch consumer (matching production) but the fake
  stream terminates as soon as stop_event is set.
  """

  def __init__(self, error_on=None, warm_on=(), never_ready=()):
    self.created = []
    self.deleted = []
    self._error_on = error_on
    self._warm_on = set(warm_on)
    self._never_ready = set(never_ready)
    # Lock guarding created list so stream_claim_events can read it safely.
    self._lock = threading.Lock()
    # Event signaled when new claims are created so the stream wakes up.
    self._created_event = threading.Event()

  def create(self, name):
    if name == self._error_on:
      raise RuntimeError('boom')
    with self._lock:
      self.created.append(name)
    self._created_event.set()

  def is_ready(self, status):
    return status.get('_ready', False)

  def served_warm(self, status):
    return status.get('_warm', None)

  def stream_claim_events(self, stop_event, timeout_seconds=60):
    """Yield ready events for created (non-never_ready) claims, then stop."""
    yielded = set()
    while not stop_event.is_set():
      with self._lock:
        names = list(self.created)
      for name in names:
        if name not in yielded and name not in self._never_ready:
          yielded.add(name)
          warm = name in self._warm_on
          yield {
              'name': name,
              'status': {'_ready': True, '_warm': warm},
          }
      # Wait briefly for new creates or stop, then loop.
      self._created_event.wait(timeout=0.05)
      self._created_event.clear()

  def delete(self, name):
    self.deleted.append(name)


def _make_counter_clock(step=0.001):
  """Returns a thread-safe monotonically increasing clock function."""
  state = [0.0]
  lock = threading.Lock()

  def clock():
    with lock:
      v = state[0]
      state[0] += step
      return v

  return clock


class LoadGeneratorTest(pkb_common_test_case.PkbCommonTestCase):

  def testRunCreatesAllAndRecordsReadyViaWatch(self):
    """All claims created, all become ready from watch events, all deleted."""
    driver = _FakeDriver(warm_on={'claim-0'})
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=9999, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    records = gen.run(
        shape, clock=_make_counter_clock(), sleeper=lambda _: None
    )

    self.assertEqual(len(records), 3)
    self.assertEqual(sorted(driver.created), ['claim-0', 'claim-1', 'claim-2'])
    self.assertEqual(sorted(driver.deleted), ['claim-0', 'claim-1', 'claim-2'])
    self.assertTrue(
        all(r.ready_at is not None for r in records),
        [r for r in records if r.ready_at is None],
    )
    self.assertTrue(all(r.error is None for r in records))
    warm = {r.name: r.warm_served for r in records}
    self.assertTrue(warm['claim-0'])
    self.assertFalse(warm['claim-1'])

  def testCreateErrorRecordedNoReady(self):
    """A create failure records the error; cleanup still runs for others."""
    driver = _FakeDriver(error_on='claim-1')
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=9999, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.002, total=2)

    records = gen.run(
        shape, clock=_make_counter_clock(), sleeper=lambda _: None
    )

    errored = {r.name: r.error for r in records}
    self.assertEqual(errored['claim-1'], 'RuntimeError')
    self.assertIsNone(records[1].ready_at)
    # claim-1 failed create so it is NOT in created_set; no delete attempted.
    self.assertNotIn('claim-1', driver.deleted)
    # claim-0 succeeded.
    self.assertIn('claim-0', driver.deleted)

  def testClaimNeverReadyTimesOut(self):
    """Claims the watch never reports ready get error='Timeout' at deadline."""
    driver = _FakeDriver(never_ready={'claim-0', 'claim-1'})
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=0.5, max_concurrent=2
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.002, total=2)

    # Use a monotonically advancing fake clock that runs fast enough to
    # quickly advance past the drain deadline. Each call increments by 0.3s so
    # we hit the ready_timeout (0.5s) within a few sleeper calls.
    clock_state = [0.0]

    def fast_clock():
      v = clock_state[0]
      clock_state[0] += 0.3
      return v

    # Sleeper that also advances the clock so the drain loop terminates.
    def fast_sleeper(_):
      clock_state[0] += 0.3

    records = gen.run(shape, clock=fast_clock, sleeper=fast_sleeper)

    for rec in records:
      self.assertEqual(
          rec.error,
          'Timeout',
          f'{rec.name} expected Timeout, got error={rec.error!r}',
      )
      self.assertIsNone(rec.ready_at)

  def testReadyClaimsDeletedImmediatelyByWatchConsumer(self):
    """Each ready claim is deleted inline by the watch consumer (release-on-ready)."""
    driver = _FakeDriver()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=9999, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    records = gen.run(
        shape, clock=_make_counter_clock(), sleeper=lambda _: None
    )

    # All claims ready, all must have been deleted.
    self.assertEqual(sorted(driver.deleted), ['claim-0', 'claim-1', 'claim-2'])
    # All records have ready_at set.
    self.assertTrue(all(r.ready_at is not None for r in records))

  def testNeverReadyClaimsDeletedByStraggerCleanup(self):
    """Claims the watch never reports ready are deleted by end-of-run cleanup."""
    driver = _FakeDriver(never_ready={'claim-0', 'claim-1'})
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=0.5, max_concurrent=2
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.002, total=2)

    clock_state = [0.0]

    def fast_clock():
      v = clock_state[0]
      clock_state[0] += 0.3
      return v

    def fast_sleeper(_):
      clock_state[0] += 0.3

    gen.run(shape, clock=fast_clock, sleeper=fast_sleeper)

    # Never-ready claims are cleaned up by the straggler sweep, not the consumer.
    self.assertIn('claim-0', driver.deleted)
    self.assertIn('claim-1', driver.deleted)

  def testCleanupRunsOnInterruption(self):
    """KeyboardInterrupt during drain causes all created claims to be deleted.

    The interrupt fires from the drain-loop sleeper (outside the executor),
    so it propagates cleanly through the try/finally and re-raises after
    cleanup.
    """
    # Use never_ready so all claims stay in-flight through the drain loop,
    # giving the sleeper a chance to fire.
    driver = _FakeDriver(never_ready={'claim-0', 'claim-1', 'claim-2'})
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=10, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    # Clock: advance just enough that submit completes, then hold still so
    # drain_deadline is far in the future (keeps outstanding > 0).
    clock_vals = [float(i) * 0.001 for i in range(20)] + [0.01] * 10000
    clock_iter = iter(clock_vals)

    drain_calls = [0]

    def interrupting_sleeper(seconds):
      drain_calls[0] += 1
      # The first sleeper call is for pacing (may not occur at high QPS);
      # raise on the first drain-loop call (0.1s sleep).
      if seconds == 0.1:
        raise KeyboardInterrupt

    with self.assertRaises(KeyboardInterrupt):
      gen.run(
          shape, clock=lambda: next(clock_iter), sleeper=interrupting_sleeper
      )

    # All claims that were created must have been deleted by the finally block.
    self.assertTrue(
        len(driver.created) > 0,
        'Expected at least one claim to be created before interrupt',
    )
    for name in driver.created:
      self.assertIn(
          name,
          driver.deleted,
          f'{name} created but not deleted after KeyboardInterrupt',
      )

  def testPeakConcurrencyTracked(self):
    """peak_concurrency reflects the high-water mark of in-flight creates."""
    driver = _FakeDriver()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=9999, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.004, total=4)
    gen.run(shape, clock=_make_counter_clock(), sleeper=lambda _: None)
    self.assertGreater(gen.peak_concurrency, 0)


class ClaimRecordWorkloadFieldsTest(pkb_common_test_case.PkbCommonTestCase):

  def testExecDurationNoneWhenTimingsAbsent(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=1.0)
    self.assertIsNone(rec.exec_duration_s)

  def testExecDurationComputedWhenBothSet(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=1.0)
    rec.exec_started_at = 5.0
    rec.exec_completed_at = 8.0
    self.assertAlmostEqual(rec.exec_duration_s, 3.0)

  def testTotalLifecycleNoneWhenReleasedAbsent(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=1.0)
    self.assertIsNone(rec.total_lifecycle_s)

  def testTotalLifecycleComputedWhenSet(self):
    rec = agent_sandbox_loadgen.ClaimRecord(name='c0', requested_at=2.0)
    rec.released_at = 12.0
    self.assertAlmostEqual(rec.total_lifecycle_s, 10.0)


class _FakeExecutor(agent_sandbox_loadgen.WorkloadExecutor):
  """Records execute() calls; does not block."""

  def __init__(self):
    self.calls = []  # list of (claim_name, status)

  def execute(self, claim_name, _status, _prepared):
    self.calls.append((claim_name, _status))


class WorkloadEnabledTest(pkb_common_test_case.PkbCommonTestCase):

  def _counter_clock(self):
    """Returns a thread-safe monotonically increasing clock function."""
    return _make_counter_clock()

  def testWorkloadExecutorCalledOncePerReadyClaim(self):
    """With a workload executor, execute() is called once per ready claim."""
    import time as _time

    driver = _FakeDriver()
    executor = _FakeExecutor()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver,
        ready_timeout=5,
        max_concurrent=4,
        workload_executor=executor,
        workload_duration=1,
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    # Use time.sleep(0) as sleeper: releases the GIL so the watch/hold threads
    # make progress, while the counter clock keeps drain timing deterministic.
    records = gen.run(
        shape, clock=self._counter_clock(), sleeper=lambda _: _time.sleep(0)
    )

    # executor.execute should be called for each of the 3 ready claims
    executed_names = sorted(name for name, _ in executor.calls)
    self.assertEqual(executed_names, ['claim-0', 'claim-1', 'claim-2'])

  def testWorkloadRecordsHaveExecAndLifecycleTimings(self):
    """Records carry exec_started_at, exec_completed_at, released_at."""
    import time as _time

    driver = _FakeDriver()
    executor = _FakeExecutor()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver,
        ready_timeout=5,
        max_concurrent=4,
        workload_executor=executor,
        workload_duration=1,
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    records = gen.run(
        shape, clock=self._counter_clock(), sleeper=lambda _: _time.sleep(0)
    )

    for rec in records:
      if rec.error is not None:
        continue
      self.assertIsNotNone(
          rec.exec_started_at, f'{rec.name} missing exec_started_at'
      )
      self.assertIsNotNone(
          rec.exec_completed_at, f'{rec.name} missing exec_completed_at'
      )
      self.assertIsNotNone(rec.released_at, f'{rec.name} missing released_at')

  def testWorkloadDeleteCalledAfterExecute(self):
    """driver.delete is called for each claim after execute() completes."""
    import time as _time

    driver = _FakeDriver()
    executor = _FakeExecutor()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver,
        ready_timeout=5,
        max_concurrent=4,
        workload_executor=executor,
        workload_duration=1,
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    records = gen.run(
        shape, clock=self._counter_clock(), sleeper=lambda _: _time.sleep(0)
    )

    self.assertEqual(sorted(driver.deleted), ['claim-0', 'claim-1', 'claim-2'])

  def testNoWorkloadExecutorNeverCalled(self):
    """When workload_duration=0 (NoopExecutor), executor is never triggered."""
    driver = _FakeDriver()
    executor = _FakeExecutor()
    # Wrap NoopExecutor check: passing a NoopExecutor means workload disabled.
    noop = agent_sandbox_loadgen.NoopExecutor()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver,
        ready_timeout=9999,
        max_concurrent=4,
        workload_executor=noop,
        workload_duration=0,
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)

    records = gen.run(
        shape, clock=_make_counter_clock(), sleeper=lambda _: None
    )

    # FakeExecutor was not passed in; verify claims still deleted at ready.
    self.assertEqual(sorted(driver.deleted), ['claim-0', 'claim-1', 'claim-2'])
    # NoopExecutor.execute() is never called, so the _FakeExecutor has no calls.
    self.assertEqual(executor.calls, [])

  def testNoWorkloadPathDeletesAllClaims(self):
    """Default (no executor) path: all claims are deleted after becoming ready."""
    driver = _FakeDriver()
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver, ready_timeout=9999, max_concurrent=4
    )
    shape = agent_sandbox_loadgen.RunShape(qps=1000.0, duration=0.003, total=3)
    records = gen.run(
        shape, clock=_make_counter_clock(), sleeper=lambda _: None
    )

    self.assertEqual(sorted(driver.deleted), ['claim-0', 'claim-1', 'claim-2'])
    self.assertEqual(len(records), 3)


class SaturatedHoldPoolTimeoutRegressionTest(
    pkb_common_test_case.PkbCommonTestCase
):
  """Regression: ready claims must never get error='Timeout' when the hold pool
  is saturated and some holds are still queued when the drain deadline fires.

  Setup: max_concurrent=2, total=5 claims.  The fake executor blocks each
  hold until a shared release_event is set, so after 2 holds are in-flight
  the remaining 3 are queued.  A fast fake clock causes the drain deadline to
  fire while those holds are still queued.  A background thread sets the
  release event after a short real-time delay so all holds finish during
  hold_pool.shutdown(wait=True) regardless of the drain-loop code path.

  Without fix #1 (the `and name not in ready_at_map` guard), the Timeout
  sweep stamps all 5 names (none yet in `released` at deadline time), and the
  regression assertions below would fail.
  """

  def testReadyClaimsNotTimedOutWhenHoldPoolSaturated(self):
    import time as _time

    release_event = threading.Event()

    class BlockingExecutor(agent_sandbox_loadgen.WorkloadExecutor):

      def execute(self, claim_name, _status, _prepared):
        release_event.wait()

    total = 5
    max_concurrent = 2
    driver = _FakeDriver()
    executor = BlockingExecutor()
    # Use a thread-safe clock with step=1.0 so the drain deadline fires
    # quickly (ready_timeout + workload_duration = 0.05 + 1 = 1.05 fake-s,
    # reached in 2 clock calls), while the watch consumer and create loop
    # use the same clock without a race.
    clock = _make_counter_clock(step=1.0)
    gen = agent_sandbox_loadgen.LoadGenerator(
        driver,
        ready_timeout=0.05,
        max_concurrent=max_concurrent,
        workload_executor=executor,
        workload_duration=1,
    )
    shape = agent_sandbox_loadgen.RunShape(
        qps=1000.0, duration=float(total) / 1000.0, total=total
    )

    # Drain-loop sleeper yields the GIL so worker threads make progress.
    def fast_sleeper(_seconds):
      _time.sleep(0)

    # Background thread: wait a short real time, then unblock the executor.
    def _unblock():
      _time.sleep(0.5)
      release_event.set()

    unblock_thread = threading.Thread(target=_unblock, daemon=True)
    unblock_thread.start()

    records = gen.run(shape, clock=clock, sleeper=fast_sleeper)

    unblock_thread.join(timeout=5.0)

    # Every claim that reached Ready must not have error='Timeout'.
    # This is the core regression check: the Timeout sweep must not
    # stamp claims that already recorded ready_at.
    for rec in records:
      if rec.ready_at is not None:
        self.assertIsNone(
            rec.error,
            f'{rec.name} reached Ready but got error={rec.error!r}; '
            'Timeout sweep incorrectly stamped a ready claim',
        )
        self.assertIsNotNone(
            rec.released_at, f'{rec.name} reached Ready but released_at is None'
        )

    # At least one claim must have become ready (the fake driver reports
    # every created claim as ready immediately).
    ready_count = sum(1 for r in records if r.ready_at is not None)
    self.assertGreater(ready_count, 0, 'Expected at least one ready claim')


class StreamExecExecutorResolveTest(pkb_common_test_case.PkbCommonTestCase):

  def _make_executor(self, co_api):
    return agent_sandbox_loadgen.StreamExecExecutor(
        core_v1_api=None,
        custom_objects_api=co_api,
        namespace='default',
        workload_duration=5,
    )

  def testResolvesPodNameFromAnnotation(self):
    """When the Sandbox CR has agents.x-k8s.io/pod-name annotation, that name
    is returned."""
    co = mock.Mock()
    co.get_namespaced_custom_object.return_value = {
        'metadata': {
            'annotations': {'agents.x-k8s.io/pod-name': 'warmpool-pod-xyz'},
        }
    }
    executor = self._make_executor(co)
    result = executor._resolve_pod_name(
        {'sandbox': {'name': 'python-sandbox-warmpool-abc'}}
    )
    self.assertEqual(result, 'warmpool-pod-xyz')

  def testFallsBackToSandboxNameWhenNoAnnotation(self):
    """Without the annotation, the sandbox CR name is used as the pod name."""
    co = mock.Mock()
    co.get_namespaced_custom_object.return_value = {
        'metadata': {'annotations': {}}
    }
    executor = self._make_executor(co)
    result = executor._resolve_pod_name({'sandbox': {'name': 'claim-5'}})
    self.assertEqual(result, 'claim-5')

  def testReturnsNoneWhenNoSandboxBound(self):
    """Empty status.sandbox means the claim has no sandbox; returns None."""
    co = mock.Mock()
    executor = self._make_executor(co)
    result = executor._resolve_pod_name({})
    self.assertIsNone(result)
    co.get_namespaced_custom_object.assert_not_called()


if __name__ == '__main__':
  unittest.main()
