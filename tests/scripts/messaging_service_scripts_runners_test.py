"""Tests for scripts/messaging_service_scripts/common/runners.py."""

import datetime
import typing
import unittest

from absl.testing import parameterized
import freezegun
import mock
from perfkitbenchmarker.scripts.messaging_service_scripts.common import runners

FAKE_DATETIME = datetime.datetime(2021, 6, 14)
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10
UNIT_OF_TIME = 'milliseconds'
FAILURE_COUNTER = 10

METRICS = [
    0.20989608764648438, 0.2431643009185791, 0.14051604270935059,
    0.08317422866821289, 0.11351299285888672, 0.17781305313110352,
    0.037261247634887695, 0.030757904052734375, 0.042165279388427734,
    0.036507606506347656
]

AGGREGATE_PUBLISH_METRICS = {
    'publish_latency_mean': {
        'value': 0.11147687435150147,
        'unit': UNIT_OF_TIME,
        'metadata': {
            'samples': METRICS
        }
    },
    'publish_latency_mean_without_cold_start': {
        'value': 0.06490101814270019,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'publish_latency_p50': {
        'value': 0.0983436107635498,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'publish_latency_p99': {
        'value': 0.2401701617240906,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'publish_latency_p99_9': {
        'value': 0.2428648869991303,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'publish_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
    'publish_latency_failure_counter': {
        'value': 10,
        'unit': '',
        'metadata': {}
    },
}

AGGREGATE_PULL_METRICS = {
    'pull_latency_mean': {
        'value': 0.11147687435150147,
        'unit': UNIT_OF_TIME,
        'metadata': {
            'samples': METRICS
        }
    },
    'pull_latency_mean_without_cold_start': {
        'value': 0.06490101814270019,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'pull_latency_p50': {
        'value': 0.0983436107635498,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'pull_latency_p99': {
        'value': 0.2401701617240906,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'pull_latency_p99_9': {
        'value': 0.2428648869991303,
        'unit': UNIT_OF_TIME,
        'metadata': {}
    },
    'pull_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
    'pull_latency_failure_counter': {
        'value': 10,
        'unit': '',
        'metadata': {}
    },
}


@freezegun.freeze_time(FAKE_DATETIME)
class MessagingServiceScriptsRunnersTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('Publish', 'publish_latency', AGGREGATE_PUBLISH_METRICS),
      ('Pull', 'pull_latency', AGGREGATE_PULL_METRICS))
  def testGetSummaryStatistics(self, scenario, expected_samples):
    runner = runners.PublishLatencyRunner(mock.Mock())
    actual_samples = runner._get_summary_statistics(scenario, METRICS,
                                                    NUMBER_OF_MESSAGES,
                                                    FAILURE_COUNTER)

    for expected_sample_key in expected_samples:
      if expected_sample_key not in actual_samples:
        sample_not_found_message = (
            f'Expected sample:\n{expected_sample_key}\nnot found in actual samples:'
            f'\n{actual_samples}')
        raise Exception(sample_not_found_message)
      elif expected_samples[expected_sample_key] != actual_samples[
          expected_sample_key]:
        sample_doesnt_match_message = (
            f"Expected sample:\n{expected_samples[expected_sample_key]}\ndoesn't match actual sample:"
            f'\n{actual_samples[expected_sample_key]}')
        raise Exception(sample_doesnt_match_message)

  @mock.patch.object(
      runners.PublishLatencyRunner,
      '_get_summary_statistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testPublishMessages(self, summary_statistics_mock):

    runner = runners.PublishLatencyRunner(mock.Mock())
    results = runner.run_phase(NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    self.assertIsInstance(results, dict)

    # check if functions were called
    typing.cast(mock.MagicMock, runner.client.publish_message).assert_called()
    summary_statistics_mock.assert_called()
    self.assertEqual(
        typing.cast(mock.MagicMock, runner.client.publish_message).call_count,
        NUMBER_OF_MESSAGES)

  @mock.patch.object(
      runners.PublishLatencyRunner,
      '_get_summary_statistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testMeasurePublishMessagesException(self, summary_statistics_mock):
    client_mock = mock.Mock()
    client_mock.publish_message.side_effect = Exception('MockedException')
    runner = runners.PublishLatencyRunner(client_mock)
    results = runner.run_phase(NUMBER_OF_MESSAGES, MESSAGE_SIZE)
    self.assertEqual(results, {'mocked_dict': 'mocked_value'})

    # check if functions were called
    client_mock.publish_message.assert_called()
    summary_statistics_mock.assert_called_with('publish_latency', [],
                                               NUMBER_OF_MESSAGES,
                                               FAILURE_COUNTER)

  @mock.patch.object(
      runners.PullLatencyRunner,
      '_get_summary_statistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testPullMessages(self, summary_statistics_mock):

    runner = runners.PullLatencyRunner(mock.Mock())
    results = runner.run_phase(NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    self.assertIsInstance(results, dict)

    # check if functions were called
    typing.cast(mock.MagicMock, runner.client.pull_message).assert_called()
    typing.cast(mock.MagicMock,
                runner.client.acknowledge_received_message).assert_called()
    summary_statistics_mock.assert_called()
    self.assertEqual(
        typing.cast(mock.MagicMock, runner.client.pull_message).call_count,
        NUMBER_OF_MESSAGES)
    self.assertEqual(
        typing.cast(mock.MagicMock,
                    runner.client.acknowledge_received_message).call_count,
        NUMBER_OF_MESSAGES)

  @mock.patch.object(
      runners.PullLatencyRunner,
      '_get_summary_statistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testPullMessagesException(self, summary_statistics_mock):
    client_mock = mock.Mock()
    client_mock.pull_message.side_effect = Exception('MockedException')
    runner = runners.PullLatencyRunner(client_mock)
    results = runner.run_phase(NUMBER_OF_MESSAGES, MESSAGE_SIZE)
    self.assertEqual(results, {'mocked_dict': 'mocked_value'})

    # check if functions were called
    client_mock.pull_message.assert_called()
    summary_statistics_mock.assert_called_with('pull_and_acknowledge_latency',
                                               [], NUMBER_OF_MESSAGES,
                                               FAILURE_COUNTER)


if __name__ == '__main__':
  unittest.main()
