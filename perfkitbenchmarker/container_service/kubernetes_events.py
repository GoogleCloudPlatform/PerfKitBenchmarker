"""Classes related to k8s events."""

import calendar
import dataclasses
import datetime
import logging
import multiprocessing
from multiprocessing import synchronize
import re
import time
from typing import Any, Callable

from perfkitbenchmarker import errors
from perfkitbenchmarker import events


class KubernetesEventPoller:
  """Wrapper which polls for Kubernetes events."""

  def __init__(self, get_events_fn: Callable[[], set['KubernetesEvent']]):
    self.get_events_fn = get_events_fn
    self.polled_events: set['KubernetesEvent'] = set()
    self.stop_polling = multiprocessing.Event()
    self.event_queue: multiprocessing.Queue = multiprocessing.Queue()
    self.event_poller = multiprocessing.Process(
        target=self._PollForEvents,
        args=((
            self.event_queue,
            self.stop_polling,
        )),
    )
    self.event_poller.daemon = True

  def _PollForEvents(
      self,
      queue: multiprocessing.Queue,
      stop_polling: synchronize.Event,
  ):
    """Polls for events & (ideally asynchronously) waits to poll again.

    Results are appended to the queue. Timeouts are ignored.

    Args:
      queue: The queue to append events to.
      stop_polling: Stop polling when set.
    """
    while True:
      try:
        k8s_events = self.get_events_fn()
        logging.info(
            'From async get events process, got %s events', len(k8s_events)
        )
        for event in k8s_events:
          queue.put(event)
      except errors.VmUtil.IssueCommandTimeoutError:
        logging.info(
            'Async get events command timed out. This may result in missing'
            ' events, but is not a reason to fail the benchmark.'
        )
        pass
      start_sleep_time = time.time()
      while time.time() - start_sleep_time < 60 * 40:
        time.sleep(1)
        if stop_polling.is_set():
          return

  def StartPolling(self):
    """Starts polling for events."""
    self.event_poller.start()

    # Stop polling events even if the resource is not deleted.
    def _StopPollingConnected(unused1, **kwargs):
      del unused1, kwargs
      self.StopPolling()

    events.benchmark_end.connect(_StopPollingConnected, weak=False)

  def StopPolling(self):
    """Stops polling for events, joining the poller process."""
    logging.info('Stopping event poller')
    self.stop_polling.set()
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    if self.event_poller.is_alive():
      self.event_poller.join(timeout=30)
    if self.event_poller.is_alive():
      logging.warning(
          'Event poller process did not join in 30 seconds; killing it.'
      )
      self.event_poller.kill()
      self.event_poller.join(timeout=30)

  def GetEvents(self) -> set['KubernetesEvent']:
    """Gets the events for the cluster, including previously polled events."""
    k8s_events = self.get_events_fn()
    self.polled_events.update(k8s_events)
    while not self.event_queue.empty():
      self.polled_events.add(self.event_queue.get())
    return self.polled_events


@dataclasses.dataclass(eq=True, frozen=True)
class KubernetesEventResource:
  """Holder for Kubernetes event involved objects."""

  kind: str
  name: str | None

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEventResource':
    """Parse Kubernetes Event YAML output."""
    return cls(kind=yaml_data['kind'], name=yaml_data.get('name'))


@dataclasses.dataclass(eq=True, frozen=True)
class KubernetesEvent:
  """Holder for Kubernetes event data."""

  resource: KubernetesEventResource
  message: str
  # Reason is actually more of a machine readable message.
  reason: str | None
  # Examples: Normal, Warning, Error
  type: str
  timestamp: float

  @classmethod
  def FromDict(cls, yaml_data: dict[str, Any]) -> 'KubernetesEvent | None':
    """Parse Kubernetes Event YAML output."""
    if 'message' not in yaml_data:
      return None
    try:
      # There are multiple timestamps. They should be equivalent.
      raw_timestamp = yaml_data.get('lastTimestamp') or yaml_data.get(
          'eventTime'
      )
      assert raw_timestamp
      # Python 3.10 cannot handle Z as utc in ISO 8601 timestamps
      python_3_10_compatible_timestamp = re.sub('Z$', '+00:00', raw_timestamp)
      timestamp = calendar.timegm(
          datetime.datetime.fromisoformat(
              python_3_10_compatible_timestamp
          ).timetuple()
      )
      return cls(
          message=yaml_data['message'],
          reason=yaml_data.get('reason'),
          resource=KubernetesEventResource.FromDict(
              yaml_data['involvedObject']
          ),
          type=yaml_data['type'],
          timestamp=timestamp,
      )
    except (AssertionError, KeyError) as e:
      logging.exception(
          'Tried parsing event: %s but ran into error: %s', yaml_data, e
      )
      return None
