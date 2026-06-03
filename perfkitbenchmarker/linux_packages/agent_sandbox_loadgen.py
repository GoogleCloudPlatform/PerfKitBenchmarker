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


"""Host-side load generator for the agent_sandbox benchmark.

Creates SandboxClaim custom resources at a target QPS and measures the time
from claim creation to the claim reporting a Ready status condition.

Design: a single shared Watch stream (stream_claim_events) records readiness
from status events rather than per-claim polling, so we never flood the
apiserver with individual status GETs under concurrency.
"""

import dataclasses
import logging
import threading
import time
from concurrent import futures
from typing import Any, Optional, cast


# Default connection pool size for kubernetes ApiClient instances. Callers that
# know their concurrency level should pass an explicit value instead.
_CONNECTION_POOL_MAXSIZE = 64

# 429 retry parameters for create().
_CREATE_MAX_RETRIES = 5
_CREATE_RETRY_AFTER_DEFAULT = 1.0  # seconds
_CREATE_RETRY_AFTER_CAP = 5.0  # seconds


@dataclasses.dataclass(frozen=True)
class RunShape:
  """A fully resolved load shape: rate, window, and total claim count."""

  qps: float
  duration: float
  total: int


def resolve_run_shape(qps=None, duration=None, total=None):
  """Resolves any two of qps, duration, total into a complete RunShape.

  Args:
    qps: Target claims per second.
    duration: Submission window in seconds.
    total: Total number of claims to submit.

  Returns:
    A RunShape with all three fields populated.

  Raises:
    ValueError: If fewer than two values are provided.
  """
  if qps is not None and duration is not None and total is not None:
    # All three provided; respect them as-is.
    pass
  elif qps is not None and duration is not None:
    total = int(round(qps * duration))
  elif qps is not None and total is not None:
    duration = total / qps
  elif duration is not None and total is not None:
    qps = total / duration
  else:
    raise ValueError('Provide at least two of qps, duration, total.')
  return RunShape(qps=float(qps), duration=float(duration), total=int(total))


CLAIM_API_GROUP = 'extensions.agents.x-k8s.io'
CLAIM_API_VERSION = 'v1beta1'
CLAIM_PLURAL = 'sandboxclaims'
CLAIM_KIND = 'SandboxClaim'


def _load_kube_config():
  """Loads kubeconfig for the current PKB run. Isolated for test mocking."""
  from kubernetes import config  # pylint: disable=import-error,no-name-in-module

  config.load_kube_config()


def _register_bearer_token_auth(cfg):
  """Registers the exec-plugin bearer token under the key the client expects.

  kubernetes-client>=36 auth_settings() looks for the token under the
  'BearerToken' api_key, but load_kube_config() stores exec-plugin tokens
  under 'authorization'. Without this remap the Authorization header is never
  added to requests and every call goes out unauthenticated.
  """
  token = cfg.api_key.get('authorization', '')
  if token and 'BearerToken' not in cfg.api_key:
    if token.startswith('Bearer '):
      token = token[len('Bearer ') :]
    cfg.api_key['BearerToken'] = token
    cfg.api_key_prefix['BearerToken'] = 'Bearer'


def _new_api_client(connection_pool_maxsize=_CONNECTION_POOL_MAXSIZE):
  """Builds a Kubernetes ApiClient with a sized connection pool and working
  bearer-token auth.

  Every API client in this module needs the same two things, so they are
  applied once here rather than in each constructor:
    - connection_pool_maxsize, so concurrent requests don't serialize behind
      a small urllib3 pool.
    - the exec-plugin bearer-token remap (see _register_bearer_token_auth).
  """
  from kubernetes import client  # pylint: disable=import-error,no-name-in-module

  cfg = client.Configuration.get_default_copy()
  cfg.connection_pool_maxsize = connection_pool_maxsize
  _register_bearer_token_auth(cfg)
  return client.ApiClient(cfg)


def _make_custom_objects_api(connection_pool_maxsize=_CONNECTION_POOL_MAXSIZE):
  """Builds a CustomObjectsApi client with a sized connection pool."""
  from kubernetes import client  # pylint: disable=import-error,no-name-in-module

  return client.CustomObjectsApi(_new_api_client(connection_pool_maxsize))


def _make_core_v1_api(connection_pool_maxsize=_CONNECTION_POOL_MAXSIZE):
  """Builds a CoreV1Api client with a sized connection pool (for pod exec)."""
  from kubernetes import client  # pylint: disable=import-error,no-name-in-module

  return client.CoreV1Api(_new_api_client(connection_pool_maxsize))


@dataclasses.dataclass
class ClaimRecord:
  """Timing record for one SandboxClaim."""

  name: str
  requested_at: float
  ready_at: Optional[float] = None
  error: Optional[str] = None
  warm_served: Optional[bool] = None
  exec_started_at: Optional[float] = None
  exec_completed_at: Optional[float] = None
  released_at: Optional[float] = None

  @property
  def startup_time_s(self):
    if self.ready_at is None:
      return None
    return self.ready_at - self.requested_at

  @property
  def exec_duration_s(self):
    if self.exec_started_at is None or self.exec_completed_at is None:
      return None
    return self.exec_completed_at - self.exec_started_at

  @property
  def total_lifecycle_s(self):
    if self.released_at is None:
      return None
    return self.released_at - self.requested_at


def _busy_loop_code(workload_duration):
  """Python source for a CPU busy-loop that runs ~workload_duration seconds."""
  return (
      'import time\n'
      'start = time.monotonic()\n'
      'count = 0\n'
      f'while time.monotonic() - start < {workload_duration}:\n'
      '    _ = (count * 2) + (count ** 2)\n'
      '    count += 1\n'
      'print(f"iterations={count}")\n'
  )


class WorkloadExecutor:
  """Runs a workload inside a Ready sandbox and blocks until it completes."""

  def prepare(self, _claim_name, _status):
    """Called eagerly in the watch consumer when the claim becomes Ready.

    Returns any pre-fetched data that execute() needs. The return value is
    passed through as the `prepared` argument to execute(). Default: None."""
    return None

  def execute(self, _claim_name, _status, _prepared):
    raise NotImplementedError


class NoopExecutor(WorkloadExecutor):
  """No workload: used when workload_duration == 0 (immediate release)."""

  def execute(self, _claim_name, _status, _prepared):
    return


class StreamExecExecutor(WorkloadExecutor):
  """Execs the workload into the sandbox pod via the Kubernetes API server
  (connect_get_namespaced_pod_exec). Works from any host with kubeconfig."""

  def __init__(
      self,
      core_v1_api,
      custom_objects_api,
      namespace,
      workload_duration,
      sandbox_group='agents.x-k8s.io',
      sandbox_version='v1beta1',
      sandbox_plural='sandboxes',
  ):
    self._core = core_v1_api
    self._co = custom_objects_api
    self._namespace = namespace
    self._workload_duration = workload_duration
    self._sandbox_group = sandbox_group
    self._sandbox_version = sandbox_version
    self._sandbox_plural = sandbox_plural

  def _resolve_pod_name(self, status):
    """Ready claim -> sandbox pod name. status.sandbox.name is the Sandbox CR
    name; the actual pod is that name UNLESS a warm-pool pod was adopted, in
    which case the pod name is in the Sandbox's agents.x-k8s.io/pod-name
    annotation (per the controller's resolvePodName)."""
    sandbox = status.get('sandbox') or {}
    sandbox_name = sandbox.get('name')
    if not sandbox_name:
      return None
    sb = self._co.get_namespaced_custom_object(
        self._sandbox_group,
        self._sandbox_version,
        self._namespace,
        self._sandbox_plural,
        sandbox_name,
    )
    annotations = (sb.get('metadata') or {}).get('annotations') or {}
    return annotations.get('agents.x-k8s.io/pod-name') or sandbox_name

  def prepare(self, claim_name, status):
    """Resolves the pod name while the Sandbox CR is guaranteed to exist."""
    return self._resolve_pod_name(status)

  def execute(self, claim_name, _status, prepared):
    from kubernetes.stream import stream  # pylint: disable=import-error,no-name-in-module

    pod_name = prepared
    if not pod_name:
      raise RuntimeError(f'no sandbox pod bound for claim {claim_name}')
    ws = stream(
        self._core.connect_get_namespaced_pod_exec,
        pod_name,
        self._namespace,
        command=['python3', '-c', _busy_loop_code(self._workload_duration)],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _request_timeout=self._workload_duration + 60,
        _preload_content=False,
    )
    try:
      ws.run_forever(timeout=self._workload_duration + 60)
    finally:
      ws.close()


class ClaimDriver:
  """Creates, watches, and deletes SandboxClaim custom resources."""

  def __init__(
      self,
      namespace,
      template_name,
      warmpool_name=None,
      group=CLAIM_API_GROUP,
      version=CLAIM_API_VERSION,
      plural=CLAIM_PLURAL,
      claim_ttl_seconds=None,
      max_concurrent=64,
  ):
    _load_kube_config()
    pool_size = max_concurrent + 50
    self._api = _make_custom_objects_api(pool_size)
    self._delete_api = _make_custom_objects_api(pool_size)
    self._namespace = namespace
    self._template_name = template_name
    self._warmpool_name = warmpool_name
    self._group = group
    self._version = version
    self._plural = plural
    self._claim_ttl_seconds = claim_ttl_seconds or 0

  def _body(self, name):
    lifecycle: dict[str, Any] = {'shutdownPolicy': 'Delete'}
    if self._claim_ttl_seconds:
      lifecycle['ttlSecondsAfterFinished'] = self._claim_ttl_seconds
    spec = {
        'sandboxTemplateRef': {'name': self._template_name},
        'lifecycle': lifecycle,
    }
    if self._warmpool_name:
      spec['warmpool'] = self._warmpool_name
    return {
        'apiVersion': f'{self._group}/{self._version}',
        'kind': CLAIM_KIND,
        'metadata': {'name': name},
        'spec': spec,
    }

  def create(self, name, sleeper=time.sleep):
    """Creates a SandboxClaim, retrying on 429 up to _CREATE_MAX_RETRIES."""
    from kubernetes.client.rest import ApiException  # pylint: disable=import-error,no-name-in-module

    attempt = 0
    while True:
      try:
        self._api.create_namespaced_custom_object(
            self._group,
            self._version,
            self._namespace,
            self._plural,
            body=self._body(name),
        )
        return
      except ApiException as exc:
        if exc.status == 429 and attempt < _CREATE_MAX_RETRIES:
          attempt += 1
          retry_after = _CREATE_RETRY_AFTER_DEFAULT
          try:
            if exc.headers and exc.headers.get('Retry-After'):
              retry_after = float(exc.headers['Retry-After'])
          except (TypeError, ValueError):
            pass
          retry_after = min(retry_after, _CREATE_RETRY_AFTER_CAP)
          logging.warning(
              'Claim %s create got 429; retry %d/%d after %.1fs',
              name,
              attempt,
              _CREATE_MAX_RETRIES,
              retry_after,
          )
          sleeper(retry_after)
        else:
          raise

  def is_ready(self, status):
    for condition in status.get('conditions', []):
      if condition.get('type') == 'Ready' and condition.get('status') == 'True':
        return True
    return False

  def served_warm(self, status):
    """Warm-served iff the bound sandbox name carries the warm pool prefix.

    Cold-provisioned sandboxes are named after the claim; warm-served ones
    are named after the warm pool. Returns None if no sandbox is bound yet.
    """
    sandbox = status.get('sandbox') or {}
    name = sandbox.get('name')
    if not name:
      return None
    return bool(self._warmpool_name) and name.startswith(self._warmpool_name)

  def stream_claim_events(self, stop_event, timeout_seconds=60):
    """Generates {'name': str, 'status': dict} dicts from a Watch stream.

    Re-establishes the watch if it times out, until stop_event is set.
    Resumes from the last seen resourceVersion so no events are missed across
    reconnects (BOOKMARK events advance the cursor without being yielded).
    Isolated as a method so tests can substitute a fake generator.

    Args:
      stop_event: threading.Event; when set the generator stops after the
        current watch iteration completes.
      timeout_seconds: Per-watch-call timeout forwarded to the Watch stream.

    Yields:
      {'name': str, 'status': dict} for every SandboxClaim event.
    """
    from kubernetes import watch as kwatch  # pylint: disable=import-error,no-name-in-module
    from kubernetes.client.rest import ApiException  # pylint: disable=import-error,no-name-in-module

    resource_version = None
    while not stop_event.is_set():
      w = kwatch.Watch()
      try:
        stream_kwargs = dict(
            timeout_seconds=timeout_seconds,
            allow_watch_bookmarks=True,
        )
        if resource_version is not None:
          stream_kwargs['resource_version'] = resource_version
        for _raw_event in w.stream(
            self._api.list_namespaced_custom_object,
            self._group,
            self._version,
            self._namespace,
            self._plural,
            **stream_kwargs,
        ):
          event: dict[str, Any] = cast(dict[str, Any], _raw_event)
          if stop_event.is_set():
            w.stop()
            return
          obj = event['object']
          rv = (obj.get('metadata') or {}).get('resourceVersion')
          if rv:
            resource_version = rv
          if event.get('type') == 'BOOKMARK':
            continue
          if event.get('type') == 'ERROR':
            code = (
                obj.get('code')
                if isinstance(obj, dict)
                else getattr(obj, 'code', None)
            )
            logging.warning('Watch stream ERROR event (code=%s): %s', code, obj)
            resource_version = None
            w.stop()
            break
          name = obj['metadata']['name']
          status = obj.get('status', {}) or {}
          yield {'name': name, 'status': status}
      except ApiException as exc:
        if stop_event.is_set():
          return
        if exc.status == 410:
          logging.warning(
              'Watch resourceVersion too old (410 Gone); re-listing: %s', exc
          )
          resource_version = None
        else:
          logging.warning('Watch stream API error (will reconnect): %s', exc)
          time.sleep(1)
      except Exception as exc:  # noqa: BLE001  watch reconnect on any error
        if stop_event.is_set():
          return
        logging.warning('Watch stream error (will reconnect): %s', exc)
        time.sleep(1)

  def delete(self, name):
    self._delete_api.delete_namespaced_custom_object(
        self._group, self._version, self._namespace, self._plural, name
    )

  @property
  def custom_objects_api(self):
    return self._api


class LoadGenerator:
  """Submits claims at a target QPS; records readiness via a Watch stream.

  Architecture:
    - A background thread consumes stream_claim_events() and records ready_at
      timestamps in shared maps (no per-claim API polling).
    - A bounded ThreadPoolExecutor submits creates only (one API call per task,
      no blocking wait inside the task).
    - After all creates are submitted, the run() method drains locally by
      polling the in-memory maps until all claims are accounted for or the
      ready_timeout elapses.
    - Cleanup deletes all created claims best-effort after the run.
  """

  def __init__(
      self,
      driver,
      ready_timeout,
      max_concurrent,
      workload_executor=None,
      workload_duration=0,
  ):
    self._driver = driver
    self._ready_timeout = ready_timeout
    self._max_concurrent = max_concurrent
    self._workload_duration = workload_duration
    self._executor = workload_executor or NoopExecutor()
    self._lock = threading.Lock()
    self._in_flight = 0
    self._peak = 0
    self._clock = time.monotonic
    self._hold_pool = None

  @property
  def peak_concurrency(self):
    return self._peak

  def _watch_consumer(
      self,
      stop_event,
      ready_at_map,
      warm_map,
      created_set,
      released,
      exec_started_map,
      exec_completed_map,
      released_at_map,
  ):
    """Background thread: consume watch events and record readiness."""
    for event in self._driver.stream_claim_events(stop_event):
      name = event['name']
      status = event['status']
      if not self._driver.is_ready(status):
        continue
      with self._lock:
        if name not in created_set:
          continue
        if name in ready_at_map:
          continue  # already recorded
        ready_at_map[name] = self._clock()
        warm_map[name] = self._driver.served_warm(status)
      try:
        prepared = self._executor.prepare(name, status)
      except Exception as exc:  # noqa: BLE001
        logging.warning(
            'Failed to prepare workload for claim %s: %s', name, exc
        )
        prepared = None
      hold_pool = self._hold_pool
      if hold_pool is not None:
        hold_pool.submit(
            self._hold_task,
            name,
            prepared,
            exec_started_map,
            exec_completed_map,
            released_at_map,
            released,
        )

  def _hold_task(
      self,
      name,
      prepared,
      exec_started_map,
      exec_completed_map,
      released_at_map,
      released,
  ):
    """Worker thread: run workload then delete the claim."""
    exec_started = self._clock()
    try:
      self._executor.execute(name, None, prepared)
    except Exception as exc:  # noqa: BLE001  workload failure must not abort the run
      logging.warning('Workload exec failed for claim %s: %s', name, exc)
      exec_started = None  # leave exec timings unset so a bad exec does not skew exec_duration
    finally:
      exec_completed = self._clock() if exec_started is not None else None
      try:
        self._driver.delete(name)
      except Exception as exc:  # noqa: BLE001  best-effort release
        logging.warning('Failed to release claim %s: %s', name, exc)
      with self._lock:
        exec_started_map[name] = exec_started
        exec_completed_map[name] = exec_completed
        released_at_map[name] = self._clock()
        released.add(name)
        self._in_flight -= 1

  def run(self, shape, clock=time.monotonic, sleeper=time.sleep):
    """Run the load shape, returning a list of ClaimRecord in submission order.

    Args:
      shape: RunShape describing qps, duration, and total claims.
      clock: Callable returning a float timestamp (injectable for tests).
      sleeper: Callable(seconds) for pacing sleeps (injectable for tests).

    Returns:
      List of ClaimRecord, one per submitted claim index, in order.
    """
    self._clock = clock

    requested_at_map = {}
    ready_at_map = {}
    warm_map = {}
    errors_map = {}
    created_set = set()
    released = set()
    exec_started_map = {}
    exec_completed_map = {}
    released_at_map = {}
    submitted_names = []

    stop_event = threading.Event()

    # Initialize hold_pool before starting watch_thread so that a Ready event
    # from a pre-existing claim cannot race against self._hold_pool being None.
    hold_pool = futures.ThreadPoolExecutor(
        max_workers=self._max_concurrent, thread_name_prefix='claim-hold'
    )
    self._hold_pool = hold_pool

    watch_thread = threading.Thread(
        target=self._watch_consumer,
        args=(
            stop_event,
            ready_at_map,
            warm_map,
            created_set,
            released,
            exec_started_map,
            exec_completed_map,
            released_at_map,
        ),
        daemon=True,
        name='claim-watch-consumer',
    )
    watch_thread.start()

    def _create_task(name):
      # Record requested_at BEFORE create() so the watch consumer always finds
      # the entry even if a Ready event arrives before create() returns.
      with self._lock:
        requested_at_map[name] = clock()
      try:
        self._driver.create(name)
        with self._lock:
          created_set.add(name)
          self._in_flight += 1
          self._peak = max(self._peak, self._in_flight)
      except Exception as exc:  # noqa: BLE001  record and continue
        logging.warning(
            'Claim %s create failed: %s: %s', name, type(exc).__name__, exc
        )
        with self._lock:
          errors_map[name] = type(exc).__name__

    start = clock()
    last_create_time = start
    create_futs = []

    try:
      with futures.ThreadPoolExecutor(max_workers=self._max_concurrent) as pool:
        for idx in range(shape.total):
          name = f'claim-{idx}'
          submitted_names.append(name)
          deadline = start + (idx / shape.qps)
          now = clock()
          if now < deadline:
            sleeper(deadline - now)
          last_create_time = clock()
          create_futs.append(pool.submit(_create_task, name))
        # Wait for all create tasks to complete before starting drain.
        for fut in create_futs:
          fut.result()

      # Drain: wait for accounts of all successfully created claims, or until
      # the deadline elapses since the last create. When workload is enabled,
      # "accounted" means released_at is set (hold finished) or errored. When
      # workload is disabled, "accounted" means ready_at set or errored.
      drain_window = self._ready_timeout + self._workload_duration
      drain_deadline = last_create_time + drain_window
      while True:
        now = clock()
        with self._lock:
          accounted = len(released_at_map) + sum(
              1 for n in created_set if n in errors_map
          )
          outstanding = len(created_set) - accounted
        if outstanding <= 0:
          break
        if now >= drain_deadline:
          # Mark remaining created-but-never-Ready claims as Timeout.
          # Claims that already reached Ready (name in ready_at_map) must
          # never be Timeout'd: their startup_time is already captured and
          # their hold will finish in the finally block below.
          with self._lock:
            for name in list(created_set):
              if (
                  name not in released
                  and name not in errors_map
                  and name not in ready_at_map
              ):
                errors_map[name] = 'Timeout'
          break
        sleeper(0.1)

    finally:
      # Always stop the watch thread and delete any created-but-not-released
      # claims. Runs on normal completion AND on KeyboardInterrupt/exception
      # so Ctrl-C or crashes do not leak SandboxClaims (and their pods).
      stop_event.set()
      # Join the watch thread first so any in-progress _watch_consumer
      # iteration finishes submitting hold tasks before we shut down the pool.
      watch_thread.join(timeout=2.0)
      if watch_thread.is_alive():
        logging.warning(
            'Watch consumer thread did not exit within 2s; '
            'proceeding with hold pool shutdown.'
        )
      # Drain all in-flight hold tasks before nulling the pool reference so
      # that a still-running watch consumer can still submit tasks.
      hold_pool.shutdown(wait=True)
      self._hold_pool = None

      with self._lock:
        stragglers = [n for n in created_set if n not in released]
      for name in stragglers:
        try:
          self._driver.delete(name)
        except Exception as exc:  # noqa: BLE001  best-effort cleanup
          logging.warning('Failed to delete claim %s: %s', name, exc)

    # Build ordered result list.
    records = []
    for name in submitted_names:
      released_at = released_at_map.get(name)
      error = errors_map.get(name)
      # A claim that completed its hold (released_at set) is a success.
      # If a stale 'Timeout' error was recorded before the hold finished,
      # drop it so released_at is authoritative. Other error types (e.g.
      # create failures, exec errors) are kept regardless.
      if released_at is not None and error == 'Timeout':
        error = None
      rec = ClaimRecord(
          name=name,
          requested_at=requested_at_map.get(name, 0.0),
          ready_at=ready_at_map.get(name),
          error=error,
          warm_served=warm_map.get(name),
          exec_started_at=exec_started_map.get(name),
          exec_completed_at=exec_completed_map.get(name),
          released_at=released_at,
      )
      records.append(rec)
    return records
