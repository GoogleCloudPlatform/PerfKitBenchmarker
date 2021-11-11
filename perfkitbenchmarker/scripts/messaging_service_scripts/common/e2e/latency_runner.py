"""Runners for end-to-end latency benchmark scenario."""

import asyncio
import json
import multiprocessing as mp
import os
import random
import sys
from typing import Optional, Set

from perfkitbenchmarker.scripts.messaging_service_scripts.common import errors
from perfkitbenchmarker.scripts.messaging_service_scripts.common import runners
from perfkitbenchmarker.scripts.messaging_service_scripts.common.e2e import main_process

RECEIVER_PULL_TIME_MARGIN = 0.5  # seconds


def nanoseconds_to_milliseconds(ns: int) -> int:
  return ns // 1_000_000


class EndToEndLatencyRunner(runners.BaseRunner):
  """Runner for end-to-end latency measurement."""

  MAIN_PINNED_CPUS: Optional[Set[int]] = None
  PUBLISHER_PINNED_CPUS: Optional[Set[int]] = None
  RECEIVER_PINNED_CPUS: Optional[Set[int]] = None

  @classmethod
  def on_startup(cls):
    mp.set_start_method('spawn')
    available_cpus = os.sched_getaffinity(0)
    publisher_cpus_required = main_process.PublisherWorker.CPUS_REQUIRED
    receiver_cpus_required = main_process.ReceiverWorker.CPUS_REQUIRED
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
    publisher_cpus = set(shuffled_cpus[1:publisher_cpus_required + 1])
    receiver_cpus = set(
        shuffled_cpus[publisher_cpus_required + 1: total_cpus_required])
    os.sched_setaffinity(0, main_process_cpus)
    cls.MAIN_PINNED_CPUS = main_process_cpus
    cls.PUBLISHER_PINNED_CPUS = publisher_cpus
    cls.RECEIVER_PINNED_CPUS = receiver_cpus

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
    return asyncio.run(self._async_run_phase(number_of_messages, message_size))

  async def _async_run_phase(self, number_of_messages: int, _: int):
    """Actual coroutine implementing run phase logic."""

    publisher = main_process.PublisherWorker(self.PUBLISHER_PINNED_CPUS)
    receiver = main_process.ReceiverWorker(self.RECEIVER_PINNED_CPUS)
    e2e_pull_latencies = []
    e2e_ack_latencies = []
    failure_counter = 0

    try:
      await asyncio.wait([publisher.start(), receiver.start()])
      for _ in range(number_of_messages):
        try:
          await receiver.start_consumption()
          # Give time for the receiver to actually start pulling.
          # RECEIVER_PULL_TIME_MARGIN should be big enough for the receiver to
          # have started pulling after being commanded to do so, while small
          # enough to make this test run on a reasonable time.
          await asyncio.sleep(RECEIVER_PULL_TIME_MARGIN)
          publish_timestamp = await publisher.publish()
          receive_timestamp, ack_timestamp = await receiver.receive()
          e2e_pull_latencies.append(
              nanoseconds_to_milliseconds(receive_timestamp -
                                          publish_timestamp))
          e2e_ack_latencies.append(
              nanoseconds_to_milliseconds(ack_timestamp - publish_timestamp))
        except errors.EndToEnd.SubprocessFailedOperationError:
          failure_counter += 1
    except Exception:  # pylint: disable=broad-except
      failure_counter += number_of_messages - len(e2e_pull_latencies)
    finally:
      await asyncio.wait([publisher.stop(), receiver.stop()])

    # getting summary statistics
    pull_metrics = self._get_summary_statistics('e2e_latency',
                                                e2e_pull_latencies,
                                                number_of_messages,
                                                failure_counter)
    acknowledge_metrics = self._get_summary_statistics(
        'e2e_acknowledge_latency', e2e_ack_latencies, number_of_messages,
        failure_counter)

    # merging metrics dictionaries
    metrics = {**pull_metrics, **acknowledge_metrics}

    print(json.dumps(metrics))
    return metrics
