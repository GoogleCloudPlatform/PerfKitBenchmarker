"""Messaging Service interface class.

This generic interface creates the base class that we need to benchmark
different messaging services.
"""

import abc
import json
from typing import Any, Dict, List

from absl import flags
import numpy as np

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

  def _GetSummaryStatistics(self, scenario: str, results: List[float],
                            number_of_messages: int) -> Dict[str, Any]:
    """Getting statistics based on results from the benchmark."""
    metrics_data = {}
    common_metadata = {}

    latency_mean = np.mean(results)
    latency_mean_without_cold_start = np.mean(results[len(results) // 2:])
    latency_percentage_received = 100 * (len(results) / number_of_messages)

    metrics_data[scenario + '_mean'] = {
        'value': latency_mean,
        'unit': 'seconds',
        'metadata': {'samples': results}
    }
    metrics_data[scenario + '_mean_without_cold_start'] = {
        'value': latency_mean_without_cold_start,
        'unit': 'seconds',
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p50'] = {
        'value': np.percentile(results, 50),
        'unit': 'seconds',
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p99'] = {
        'value': np.percentile(results, 99),
        'unit': 'seconds',
        'metadata': common_metadata
    }
    metrics_data[scenario + '_p99_9'] = {
        'value': np.percentile(results, 99.9),
        'unit': 'seconds',
        'metadata': common_metadata
    }
    metrics_data[scenario + '_percentage_received'] = {
        'value': latency_percentage_received,
        'unit': '%',
        'metadata': common_metadata
    }
    return metrics_data

  @abc.abstractmethod
  def PublishMessages(
      self,
      number_of_messages: int,
      message_size: int) -> None:
    """Publish messages on messaging service and measure latency.

    This function attempts to publish messages. It measures the latency
    between a call to publish the message, and the message being successfully
    published. We wait for the publish message call to be completed (with a
    blocking call) before attempting to publish the next message. When the
    publish message call is completed the message was successfully published.

    Args:
      number_of_messages: Number of messages to publish.
      message_size: Size of the messages that are being published.
        It specifies the number of characters in those messages.
    """

  @abc.abstractmethod
  def PullMessages(
      self,
      number_of_messages: int) -> None:
    """Pull messages from messaging service and measure latency.

    This function pulls messages from the messaging service. It measures the
    latency between a call to pull the message, and the message being
    successfully received on the client VM. We wait for a message to be pulled
    before attempting to pull the next one (blocking call).

    Args:
      number_of_messages: Number of messages to pull.
    """

  @abc.abstractmethod
  def ConsecutivePublishPullMessages(self, number_of_messages: int,
                                     message_size: int):
    """Publish and pull message, measuring end to end latency.

    This function publishes a message and then it tries to pull it as soon as
    it's available to be pulled. It measures the latency between a call to
    publish the message and pulling the message - both operations are done
    sequentially by the same client VM.

    Args:
      number_of_messages: Number of messages to publish and pull.
      message_size: Size of the messages that are published/pulled. It specifies
        the number of characters in those messages.
    """

  def MeasurePublishAndPullLatency(self, number_of_messages: int,
                                   message_size: int) -> Dict[str, Any]:
    """Handle measurements of latency to pull messages.

    This function calls the cloud specific implementation to pull messages
    and it creates a dictionary containing all the results.

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

    # publishing 'number_of_messages' messages
    publish_latencies = self.PublishMessages(
        number_of_messages,
        message_size)

    # pulling 'number_of_messages' messages
    pull_latencies = self.PullMessages(number_of_messages)

    # getting metrics and adding to 'data' Dict
    publish_metrics = self._GetSummaryStatistics('publish_latency',
                                                 publish_latencies,
                                                 number_of_messages)
    pull_metrics = self._GetSummaryStatistics('pull_latency', pull_latencies,
                                              number_of_messages)
    # merging dictionaries
    publish_metrics.update(pull_metrics)

    print(json.dumps(publish_metrics))
    return publish_metrics

  def MeasureEndToEndLatency(self, number_of_messages: int,
                             message_size: int) -> Dict[str, Any]:
    """Measures end to end latency.

    Latency to publish and pull message.

    This function calls the cloud specific implementation to publish and pull
    messages - right after they're available to be pulled - and it creates a
    dictionary containing all the results.

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

    # publishing and pulling 'number_of_messages' messages
    # and measuring their latency | run phase
    end_to_end_latencies = self.ConsecutivePublishPullMessages(
        number_of_messages,
        message_size)

    # getting metrics and adding to our 'data' Dict
    end_to_end_metrics = self._GetSummaryStatistics('end_to_end_latency',
                                                    end_to_end_latencies,
                                                    number_of_messages)

    print(json.dumps(end_to_end_metrics))
    return end_to_end_metrics

  def RunPhase(self,
               benchmark_scenario: str,
               number_of_messages: int,
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
      return self.MeasurePublishAndPullLatency(number_of_messages, message_size)
    elif benchmark_scenario == 'end_to_end_latency':
      return self.MeasureEndToEndLatency(number_of_messages, message_size)
