"""Messaging Service interface class.

This generic interface creates the base class that we need to benchmark
different messaging services.
"""
# pylint: disable=broad-except
import abc
import json
import logging
import random
import string
import time
from typing import Any, Dict, List

from absl import flags
import numpy as np

TIMEOUT = 10
MESSAGE_CHARACTERS = string.ascii_letters + string.digits
GET_TIME_IN_MILLISECONDS = lambda: time.time() * 1000
UNIT_OF_TIME = 'milliseconds'

FLAGS = flags.FLAGS

flags.DEFINE_enum('benchmark_scenario',
                  'pull_latency',
                  ['pull_latency', 'end_to_end_latency'],
                  help='The scenario to benchmark.')
flags.DEFINE_integer('number_of_messages',
                     100,
                     help='Number of messages to send on benchmark.')
flags.DEFINE_integer('message_size',
                     10,
                     help='Number of characters to have in a message. '
                     "Ex: 1: 'A', 2: 'AA', ...")


class MessagingServiceClient:
  """Generic MessagingServiceClient Class.

  This is a base class to all messaging service interfaces - GCP Cloud PubSub,
  AWS SQS... Common functions are defined here, and the specific ones on their
  own implementation (gcp_pubsub_client.py).
  """

  def _get_summary_statistics(self, scenario: str, results: List[float],
                              number_of_messages: int) -> Dict[str, Any]:
    """Getting statistics based on results from the benchmark."""
    metrics_data = {}
    common_metadata = {}

    latency_mean = np.mean(results)
    latency_mean_without_cold_start = np.mean(results[len(results) // 2:])
    latency_percentage_received = 100 * (len(results) / number_of_messages)

    metrics_data[scenario + '_mean'] = {
        'value': latency_mean,
        'unit': UNIT_OF_TIME,
        'metadata': {'samples': results}
    }
    metrics_data[scenario + '_mean_without_cold_start'] = {
        'value': latency_mean_without_cold_start,
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p50'] = {
        'value': np.percentile(results, 50),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p99'] = {
        'value': np.percentile(results, 99),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p99_9'] = {
        'value': np.percentile(results, 99.9),
        'unit': UNIT_OF_TIME,
        'metadata': common_metadata
    }
    metrics_data[scenario + '_percentage_received'] = {
        'value': latency_percentage_received,
        'unit': '%',
        'metadata': common_metadata
    }
    return metrics_data

  def _generate_random_message(self, message_size: int) -> bytes:
    message = ''.join(
        random.choice(MESSAGE_CHARACTERS) for _ in range(message_size))
    return message.encode('utf-8')

  @abc.abstractmethod
  def _publish_message(self, message_payload: bytes) -> Any:
    """Publishes a single message to the messaging service.

    Args:
      message_payload: Message in bytes, created by '_generate_random_message'.
      This message will be the one that we publish/pull from the messaging
      service.

    Returns:
      Return response to publish a message from the provider. For GCP PubSub
      we make a call to '.publish' that publishes the message and we return
      the result from this call.
    """

  @abc.abstractmethod
  def _pull_message(self) -> Any:
    """Pulls a single message from the messaging service.

    Returns:
      Return response to pull a message from the provider. For GCP PubSub
      we make a call to '.pull' that pulls a message and we return
      the result from this call.
    """

  @abc.abstractmethod
  def _acknowledges_received_message(self, response: Any) -> None:
    """Acknowledges that the pulled message was received.

    It tries to acknowledge the message that was pulled with _pull_message.

    Args:
      response: Response from _pull_message.
    """

  def measure_publish_and_pull_latency(self, number_of_messages: int,
                                       message_size: int) -> Dict[str, Any]:
    """Handle measurements of latency to publish and then pull messages.

    This function publish messages, and then pull them. It measures the latency
    between a call to publish the message, and the message being successfully
    published. We wait for the publish message call to be completed (with a
    blocking call) before attempting to publish the next message. When the
    publish message call is completed the message was successfully published.
    After "number_of_messages" messages were published we try to pull them
    from the messaging service. It measures the latency between a call to pull
    the message, and the message being successfully received on the client VM.
    We wait for a message to be pulled before attempting to pull the next one
    (blocking call). If a publish (or pull) message fails we will keep trying to
    publish 'number_of_messages' messages, we are generating a stat for success
    rate that shows the percentage of requests that didn't failed.

    Args:
      number_of_messages: Number of messages to publish and pull.
      message_size: Size of the messages that are published/pulled.
        It specifies the number of characters in those messages.

    Returns:
      Dictionary with metric_name (mean_latency, p50_latency...) as key and the
      results from the benchmark as the value:
        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    publish_latencies = []
    pull_latencies = []
    acknowledge_latencies = []

    # publishing 'number_of_messages' messages
    for _ in range(number_of_messages):
      message_payload = self._generate_random_message(message_size)
      start_time = GET_TIME_IN_MILLISECONDS()
      # Publishing a message and waiting for completion
      try:
        self._publish_message(message_payload)
        end_time = GET_TIME_IN_MILLISECONDS()
        publish_latencies.append(end_time - start_time)
      except Exception as e:
        logging.warning('Failed to publish message: %r', e)

    # attempt to pull 'number_of_messages' messages
    for _ in range(number_of_messages):
      start_time = GET_TIME_IN_MILLISECONDS()
      try:
        response = self._pull_message()
        end_time = GET_TIME_IN_MILLISECONDS()
        pull_latencies.append(end_time - start_time)
        self._acknowledges_received_message(response)
        end_time = GET_TIME_IN_MILLISECONDS()
        acknowledge_latencies.append(end_time - start_time)
      except Exception as e:
        logging.warning('Failed to pull and acknowledge message: %r', e)

    # getting metrics for publish, pull, and acknowledge latencies
    publish_metrics = self._get_summary_statistics('publish_latency',
                                                   publish_latencies,
                                                   number_of_messages)
    pull_metrics = self._get_summary_statistics('pull_latency', pull_latencies,
                                                number_of_messages)

    acknowledge_metrics = self._get_summary_statistics(
        'pull_and_acknowledge_latency', acknowledge_latencies,
        number_of_messages)

    # merging dictionaries
    metrics = {**publish_metrics, **pull_metrics, **acknowledge_metrics}

    print(json.dumps(metrics))
    return metrics

  def measure_end_to_end_latency(self, number_of_messages: int,
                                 message_size: int) -> Dict[str, Any]:
    """Measures end to end latency.

    Latency to publish and pull message.
    This function publishes a single message and then it tries to pull it as
    soon as it's available to be pulled. It measures the latency between a call
    to publish the message and pulling the message - both operations are done
    sequentially by the same client VM. It uses the _publish_message and
    _pull_message cloud specific implementations.

    Args:
      number_of_messages: Number of messages to publish and pull.
      message_size: Size of the messages that are published/pulled.
        It specifies the number of characters in those messages.

    Returns:
      Dictionary with metric_name (mean_latency, p50_latency...) as key and the
      results from the benchmark as the value:
        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    # TODO(user): revisit if we really want to measure EndToEndLatency.
    end_to_end_latencies = []
    end_to_end_acknowledge_latencies = []

    # publishing and then pulling a single message
    for _ in range(number_of_messages):
      message_payload = self._generate_random_message(message_size)
      start_time = GET_TIME_IN_MILLISECONDS()
      try:
        # Publishing a message and waiting for completion
        self._publish_message(message_payload)
        # Pulling the message and waiting for completion
        response = self._pull_message()
        end_time = GET_TIME_IN_MILLISECONDS()
        end_to_end_latencies.append(end_time - start_time)
        self._acknowledges_received_message(response)
        end_time = GET_TIME_IN_MILLISECONDS()
        end_to_end_acknowledge_latencies.append(end_time - start_time)
      except Exception as e:
        logging.warning('Failed to publish and then pull message: %r', e)
        raise Exception(e)

    # getting end_to_end metrics: (publish + pull), and (publish + pull + ack)
    end_to_end_metrics = self._get_summary_statistics('end_to_end_latency',
                                                      end_to_end_latencies,
                                                      number_of_messages)
    acknowledge_metrics = self._get_summary_statistics(
        'end_to_end_acknowledge_latency', end_to_end_acknowledge_latencies,
        number_of_messages)

    # merging dictionaries
    metrics = {**end_to_end_metrics, **acknowledge_metrics}

    print(json.dumps(metrics))
    return metrics

  def run_phase(self, benchmark_scenario: str, number_of_messages: int,
                message_size: int) -> Dict[str, Any]:
    """Runs a given benchmark based on the benchmark_messaging_service Flag.

    Args:
      benchmark_scenario: Specifies which benchmark scenario to run.
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the
        benchmark. It specifies the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    if benchmark_scenario == 'pull_latency':
      return self.measure_publish_and_pull_latency(number_of_messages,
                                                   message_size)
    elif benchmark_scenario == 'end_to_end_latency':
      return self.measure_end_to_end_latency(number_of_messages, message_size)
