"""Runners for end-to-end latency benchmark scenario."""

import asyncio
import json
import logging
import multiprocessing as mp
import os
import random
import sys
import traceback
from typing import Any, Dict, Optional, Set

from perfkitbenchmarker.scripts.messaging_service_scripts.common import client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common import log_utils
from perfkitbenchmarker.scripts.messaging_service_scripts.common import runners
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import main_process
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import protocol

RECEIVER_PULL_TIME_MARGIN = 0.5  # seconds


# setting dummy root logger, before flags are parsed
logger = logging.getLogger('')


def nanoseconds_to_milliseconds(ns: int) -> int:
  return ns // 1_000_000


class EndToEndLatencyRunner(runners.BaseRunner):
  """Runner for end-to-end latency measurement."""

  MAIN_PINNED_CPUS: Optional[Set[int]] = None
  PUBLISHER_PINNED_CPUS: Optional[Set[int]] = None
  RECEIVER_PINNED_CPUS: Optional[Set[int]] = None
  RECEIVER_WORKER = main_process.ReceiverWorker

  @classmethod
  def on_startup(cls):
    global logger
    mp.set_start_method('spawn')
    available_cpus = os.sched_getaffinity(0)
    publisher_cpus_required = main_process.PublisherWorker.CPUS_REQUIRED
    receiver_cpus_required = cls.RECEIVER_WORKER.CPUS_REQUIRED
    total_cpus_required = 1 + publisher_cpus_required + receiver_cpus_required
    if len(available_cpus) < total_cpus_required:
      print(
          "Not enough CPU cores to assign to workers, they won't be pinned.",
          file=sys.stderr,
      )
      return
    shuffled_cpus = list(available_cpus)
    random.shuffle(shuffled_cpus)
    main_process_cpus = {shuffled_cpus[0]}
    publisher_cpus = set(shuffled_cpus[1 : publisher_cpus_required + 1])
    receiver_cpus = set(
        shuffled_cpus[publisher_cpus_required + 1 : total_cpus_required]
    )
    os.sched_setaffinity(0, main_process_cpus)
    cls.MAIN_PINNED_CPUS = main_process_cpus
    cls.PUBLISHER_PINNED_CPUS = publisher_cpus
    cls.RECEIVER_PINNED_CPUS = receiver_cpus
    logger = log_utils.get_logger(__name__, 'main.log')

  def __init__(self, client_: client.BaseMessagingServiceClient):
    super().__init__(client_)
    self._published_timestamps = []
    self._receive_timestamps = []
    self._ack_timestamps = []
    self._publisher = None
    self._receiver = None

  def _record_message_published(self, ack_publish: protocol.AckPublish) -> None:
    self._published_timestamps[ack_publish.seq] = ack_publish.publish_timestamp

  def _record_message_reception(
      self, reception_report: protocol.ReceptionReport
  ) -> None:
    self._receive_timestamps[reception_report.seq] = (
        reception_report.receive_timestamp
    )
    self._ack_timestamps[reception_report.seq] = reception_report.ack_timestamp

  def _compute_metrics(self, number_of_messages: int) -> Dict[str, Any]:
    e2e_pull_latencies = [None] * number_of_messages
    e2e_ack_latencies = [None] * number_of_messages
    failure_counter = 0
    i = 0
    for published_timestamp, receive_timestamp, ack_timestamp in zip(
        self._published_timestamps,
        self._receive_timestamps,
        self._ack_timestamps,
    ):
      if None in (published_timestamp, receive_timestamp, ack_timestamp):
        failure_counter += 1
        continue
      e2e_pull_latencies[i] = nanoseconds_to_milliseconds(
          receive_timestamp - published_timestamp
      )
      e2e_ack_latencies[i] = nanoseconds_to_milliseconds(
          ack_timestamp - published_timestamp
      )
      i += 1
    e2e_pull_latencies = e2e_pull_latencies[:i]
    e2e_ack_latencies = e2e_ack_latencies[:i]
    pull_metrics = self._get_summary_statistics(
        'e2e_latency', e2e_pull_latencies, number_of_messages, failure_counter
    )
    acknowledge_metrics = self._get_summary_statistics(
        'e2e_acknowledge_latency',
        e2e_ack_latencies,
        number_of_messages,
        failure_counter,
    )
    metrics = {**pull_metrics, **acknowledge_metrics}
    return metrics

  def run_phase(self, number_of_messages: int, message_size: int):
    """Runs an end-to-end latency benchmark.

    Latency here is measured starting from the moment just before publishing the
    message from one end until just after the message is read back on the other
    end. Each end is actually a worker child process which communicates with
    this main process via the multiprocessing standard library sending back
    messages which contain the actual timestamps of the moments of interest.
    Then this process computes the latencies.

    Args:
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the benchmark. It
        specifies the number of characters in those messages.

    Returns:
      Dictionary produced by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    return asyncio.run(self._async_run_phase(number_of_messages))

  async def _wait_until_received(self, seq: int) -> protocol.ReceptionReport:
    while True:
      report = await self._receiver.receive()
      self._record_message_reception(report)
      if report.seq == seq:
        return report

  async def _async_run_phase(self, number_of_messages: int) -> Dict[str, Any]:
    """Actual coroutine implementing run phase logic.

    Not re-entrant!

    Args:
      number_of_messages: Number of messages.

    Returns:
      A dict of metrics.
    """
    self._publisher = main_process.PublisherWorker(self.PUBLISHER_PINNED_CPUS)
    self._receiver = self.RECEIVER_WORKER(self.RECEIVER_PINNED_CPUS)
    self._published_timestamps = [None] * number_of_messages
    self._receive_timestamps = [None] * number_of_messages
    self._ack_timestamps = [None] * number_of_messages
    try:
      await asyncio.gather(self._publisher.start(), self._receiver.start())
      for i in range(number_of_messages):
        try:
          await self._receiver.start_consumption(seq=i)
          # Give time for the receiver to actually start pulling.
          # RECEIVER_PULL_TIME_MARGIN should be big enough for the receiver to
          # have started pulling after being commanded to do so, while small
          # enough to make this test run on a reasonable time.
          await asyncio.sleep(RECEIVER_PULL_TIME_MARGIN)
          self._record_message_published(await self._publisher.publish(i))
          await self._wait_until_received(i)
        except errors.EndToEnd.PublisherFailedOperationError:
          await self._receiver.purge_messages()
        except errors.EndToEnd.ReceiverFailedOperationError:
          await asyncio.sleep(self.RECEIVER_WORKER.PURGE_TIMEOUT)
    except Exception:  # pylint: disable=broad-except
      traceback.print_exc()
    finally:
      await asyncio.gather(self._publisher.stop(), self._receiver.stop())

    metrics = self._compute_metrics(number_of_messages)
    print(json.dumps(metrics))
    return metrics


class StreamingPullEndToEndLatencyRunner(EndToEndLatencyRunner):
  """Runner for end-to-end latency measurement using StreamingPull."""

  RECEIVER_WORKER = main_process.StreamingPullReceiverWorker
