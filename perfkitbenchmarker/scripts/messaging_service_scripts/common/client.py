"""Messaging Service interface class.

This generic interface creates the base class that we need to benchmark
different messaging services.
"""
# pylint: disable=bare-except
import abc
import json
import random
import string
import time
from typing import Any, Dict, List, Type, TypeVar

from absl import flags
import numpy as np

TIMEOUT = 10
MESSAGE_CHARACTERS = string.ascii_letters + string.digits
GET_TIME_IN_MILLISECONDS = lambda: time.time() * 1000
UNIT_OF_TIME = 'milliseconds'

FLAGS = flags.FLAGS

# Workaround for forward declaration
BaseMessagingServiceClientT = TypeVar(
    'BaseMessagingServiceClientT', bound='BaseMessagingServiceClient')


class BaseMessagingServiceClient(abc.ABC):
  """Generic BaseMessagingServiceClient Class.

  This is a base class to all messaging service interfaces - GCP Cloud PubSub,
  AWS SQS... Common functions are defined here, and the specific ones on their
  own implementation (gcp_pubsub_client.py).
  """

  def _get_summary_statistics(self, scenario: str, results: List[float],
                              number_of_messages: int,
                              failure_counter: int) -> Dict[str, Any]:
    """Getting statistics based on results from the benchmark."""
    metrics_data = {}
    common_metadata = {}

    latency_mean = np.mean(results)
    latency_mean_without_cold_start = np.mean(results[len(results) // 2:])
    latency_percentage_received = 100 * (len(results) / number_of_messages)

    metrics_data[scenario + '_failure_counter'] = {
        'value': failure_counter,
        'unit': '',
        'metadata': common_metadata
    }
    metrics_data[scenario + '_mean'] = {
        'value': latency_mean,
        'unit': UNIT_OF_TIME,
        'metadata': {
            'samples': results
        }
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

  def _generate_random_message(self, message_size: int) -> str:
    message = ''.join(
        random.choice(MESSAGE_CHARACTERS) for _ in range(message_size))
    return message

  @classmethod
  @abc.abstractmethod
  def from_flags(
      cls: Type[BaseMessagingServiceClientT]
  ) -> BaseMessagingServiceClientT:
    """Gets an actual instance based upon the FLAGS values."""
    pass

  @abc.abstractmethod
  def _publish_message(self, message_payload: str) -> Any:
    """Publishes a single message to the messaging service.

    Args:
      message_payload: Message, created by '_generate_random_message'. This
        message will be the one that we publish/pull from the messaging service.

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
  def _acknowledge_received_message(self, response: Any):
    """Acknowledge that the pulled message was received.

    It tries to acknowledge the message that was pulled with _pull_message.

    Args:
      response: Response from _pull_message.
    """

  def close(self):
    """Closes the underlying client objects.

    Optional override. By default it does nothing.
    """

  def _publish_messages(self, number_of_messages: int,
                        message_size: int) -> Dict[str, Any]:
    """Publish messages on messaging service and measure single publish latency.

    This function attempts to publish messages to a messaging service in a
    single stream to compute and report average stats for a single message
    publish. It measures the latency between a call to publish the message, and
    the message being successfully published. We wait for the publish message
    call to be completed (with a blocking call) before attempting to publish
    the next message. When the publish message call is completed the message
    was successfully published. In case of any failure when publishing a message
    we ignore it and proceed to attempt to publish the next message (success
    rate is one of the statistics generated by '_get_summary_statistics').
    Publish failure should be very rare in normal conditions.

    Args:
      number_of_messages: Number of messages to publish.
      message_size: Size of the messages that are being published. It specifies
        the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    publish_latencies = []
    failure_counter = 0

    # publishing 'number_of_messages' messages
    for _ in range(number_of_messages):
      message_payload = self._generate_random_message(message_size)
      start_time = GET_TIME_IN_MILLISECONDS()
      # Publishing a message and waiting for completion
      try:
        self._publish_message(message_payload)
        end_time = GET_TIME_IN_MILLISECONDS()
        publish_latencies.append(end_time - start_time)
      except:
        failure_counter += 1

    # getting metrics for publish, pull, and acknowledge latencies
    publish_metrics = self._get_summary_statistics('publish_latency',
                                                   publish_latencies,
                                                   number_of_messages,
                                                   failure_counter)
    print(json.dumps(publish_metrics))
    return publish_metrics

  def _pull_messages(self, number_of_messages: int) -> Dict[str, Any]:
    """Pull messages from messaging service and measure single pull latency.

    This function attempts to pull messages from a messaging service in a
    single stream to compute and report average stats for a single message
    pull. It measures the latency between a call to pull the message, and the
    message being successfully received on the client VM. We wait for a message
    to be pulled before attempting to pull the next one (blocking call). We also
    measure the latency between a call to pull the message, and a completed call
    to acknowledge that the message was received. In case of any failure when
    pulling a message we ignore it and proceed to attempt to pull the
    next message (success rate is one of the statistics generated by
    '_get_summary_statistics'). If some messages failed to publish, we expect
    their pull operation to fail as well. Pull failure should be very rare in
    normal conditions.

    Args:
      number_of_messages: Number of messages to pull.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }
    """
    pull_latencies = []
    acknowledge_latencies = []
    failure_counter = 0

    # attempt to pull 'number_of_messages' messages
    for _ in range(number_of_messages):
      start_time = GET_TIME_IN_MILLISECONDS()
      try:
        response = self._pull_message()
        pull_end_time = GET_TIME_IN_MILLISECONDS()
        self._acknowledge_received_message(response)
        acknowledge_end_time = GET_TIME_IN_MILLISECONDS()
        pull_latencies.append(pull_end_time - start_time)
        acknowledge_latencies.append(acknowledge_end_time - start_time)
      except:
        failure_counter += 1

    # getting summary statistics
    pull_metrics = self._get_summary_statistics('pull_latency', pull_latencies,
                                                number_of_messages,
                                                failure_counter)
    acknowledge_metrics = self._get_summary_statistics(
        'pull_and_acknowledge_latency', acknowledge_latencies,
        number_of_messages, failure_counter)

    # merging metrics dictionaries
    metrics = {**pull_metrics, **acknowledge_metrics}

    print(json.dumps(metrics))
    return metrics

  def run_phase(self, benchmark_scenario: str, number_of_messages: int,
                message_size: int) -> Dict[str, Any]:
    """Runs a given benchmark based on the benchmark_messaging_service Flag.

    Args:
      benchmark_scenario: Specifies which benchmark scenario to run.
      number_of_messages: Number of messages to use on the benchmark.
      message_size: Size of the messages that will be used on the benchmark. It
        specifies the number of characters in those messages.

    Returns:
      Dictionary produce by the benchmark with metric_name (mean_latency,
      p50_latency...) as key and the results from the benchmark as the value:

        data = {
          'mean_latency': 0.3423443...
          ...
        }

    Raises:
      Exception: Raised if benchmark_scenario is unknown.
    """
    if benchmark_scenario == 'publish_latency':
      return self._publish_messages(number_of_messages, message_size)
    elif benchmark_scenario == 'pull_latency':
      return self._pull_messages(number_of_messages)
    else:
      raise Exception('Unknown benchmark scenario.')
