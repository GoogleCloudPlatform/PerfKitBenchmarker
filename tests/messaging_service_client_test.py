"""Tests for data/messaging_service/messaging_service_client.py."""

import datetime
import unittest

from absl.testing import parameterized
import freezegun
import mock
from perfkitbenchmarker.data.messaging_service.messaging_service_client import MessagingServiceClient


FAKE_DATETIME = datetime.datetime(2021, 6, 14)
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10

METRICS = [
    0.20989608764648438, 0.2431643009185791, 0.14051604270935059,
    0.08317422866821289, 0.11351299285888672, 0.17781305313110352,
    0.037261247634887695, 0.030757904052734375, 0.042165279388427734,
    0.036507606506347656
]

AGGREGATE_PUBLISH_METRICS = {
    'publish_latency_mean': {
        'value': 0.11147687435150147,
        'unit': 'seconds',
        'metadata': {
            'samples': METRICS
        }
    },
    'publish_latency_mean_without_cold_start': {
        'value': 0.06490101814270019,
        'unit': 'seconds',
        'metadata': {}
    },
    'publish_latency_p50': {
        'value': 0.0983436107635498,
        'unit': 'seconds',
        'metadata': {}
    },
    'publish_latency_p99': {
        'value': 0.2401701617240906,
        'unit': 'seconds',
        'metadata': {}
    },
    'publish_latency_p99_9': {
        'value': 0.2428648869991303,
        'unit': 'seconds',
        'metadata': {}
    },
    'publish_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
}

AGGREGATE_PULL_METRICS = {
    'pull_latency_mean': {
        'value': 0.11147687435150147,
        'unit': 'seconds',
        'metadata': {
            'samples': METRICS}
    },
    'pull_latency_mean_without_cold_start': {
        'value': 0.06490101814270019,
        'unit': 'seconds',
        'metadata': {}
    },
    'pull_latency_p50': {
        'value': 0.0983436107635498,
        'unit': 'seconds',
        'metadata': {}
    },
    'pull_latency_p99': {
        'value': 0.2401701617240906,
        'unit': 'seconds',
        'metadata': {}
    },
    'pull_latency_p99_9': {
        'value': 0.2428648869991303,
        'unit': 'seconds',
        'metadata': {}
    },
    'pull_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
}

AGGREGATE_END_TO_END_METRICS = {
    'end_to_end_latency_mean': {
        'value': 0.11147687435150147,
        'unit': 'seconds',
        'metadata': {
            'samples': METRICS
        }
    },
    'end_to_end_latency_mean_without_cold_start': {
        'value': 0.06490101814270019,
        'unit': 'seconds',
        'metadata': {}
    },
    'end_to_end_latency_p50': {
        'value': 0.0983436107635498,
        'unit': 'seconds',
        'metadata': {}
    },
    'end_to_end_latency_p99': {
        'value': 0.2401701617240906,
        'unit': 'seconds',
        'metadata': {}
    },
    'end_to_end_latency_p99_9': {
        'value': 0.2428648869991303,
        'unit': 'seconds',
        'metadata': {}
    },
    'end_to_end_latency_percentage_received': {
        'value': 100.0,
        'unit': '%',
        'metadata': {}
    },
}


@freezegun.freeze_time(FAKE_DATETIME)
class MessagingServiceClientTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.messaging_service = MessagingServiceClient()

  @parameterized.named_parameters(
      ('publish', 'publish_latency', AGGREGATE_PUBLISH_METRICS),
      ('pull', 'pull_latency', AGGREGATE_PULL_METRICS),
      ('end_to_end', 'end_to_end_latency', AGGREGATE_END_TO_END_METRICS))
  def testGetSummaryStatistics(self, scenario, expected_samples):
    actual_samples = self.messaging_service._GetSummaryStatistics(
        scenario, METRICS, NUMBER_OF_MESSAGES)

    for expected_sample_key in expected_samples:
      if expected_sample_key not in actual_samples:
        sample_not_found_message = (
            f'Expected sample:\n{expected_sample_key}\nnot found in actual samples:'
            f'\n{actual_samples}')
        raise Exception(sample_not_found_message)
      elif expected_samples[expected_sample_key] != actual_samples[
          expected_sample_key]:
        sample_doesnt_match_message = (
            f"Expected sample:\n{expected_sample_key}\ndoesn't match actual sample:"
            f'\n{actual_samples[expected_sample_key]}')
        raise Exception(sample_doesnt_match_message)

  @mock.patch.object(MessagingServiceClient, 'PublishMessages')
  @mock.patch.object(MessagingServiceClient, 'PullMessages')
  @mock.patch.object(
      MessagingServiceClient,
      '_GetSummaryStatistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testMeasurePublishAndPullLatency(self, publish_mock, pull_mock,
                                       summary_statistics_mock):

    results = self.messaging_service.MeasurePublishAndPullLatency(
        NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    self.assertIsInstance(results, dict)

    # check if functions were called
    publish_mock.assert_called()
    pull_mock.assert_called()
    summary_statistics_mock.assert_called()

  @mock.patch.object(MessagingServiceClient, 'ConsecutivePublishPullMessages')
  @mock.patch.object(
      MessagingServiceClient,
      '_GetSummaryStatistics',
      return_value={'mocked_dict': 'mocked_value'})
  def testMeasureEndToEndLatency(self, publish_and_pull_mock,
                                 summary_statistics_mock):

    results = self.messaging_service.MeasureEndToEndLatency(
        NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    self.assertIsInstance(results, dict)

    # check if functions were called
    publish_and_pull_mock.assert_called()
    summary_statistics_mock.assert_called()

  @mock.patch.object(MessagingServiceClient, 'MeasurePublishAndPullLatency')
  def testRunPhasePullScenario(self, pull_mock):

    self.messaging_service.RunPhase(
        'pull_latency', NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    # check if functions were called
    pull_mock.assert_called()

  @mock.patch.object(MessagingServiceClient, 'MeasureEndToEndLatency')
  def testRunPhaseEndToEndScenario(self, end_to_end_mock):

    self.messaging_service.RunPhase(
        'end_to_end_latency', NUMBER_OF_MESSAGES, MESSAGE_SIZE)

    # check if functions were called
    end_to_end_mock.assert_called()


if __name__ == '__main__':
  unittest.main()
