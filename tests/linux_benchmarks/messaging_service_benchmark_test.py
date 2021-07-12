"""Tests for pubsub_benchmark."""

import datetime
import json
import os
import unittest

from absl.testing import parameterized
import freezegun
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks import messaging_service_benchmark

FAKE_DATETIME = datetime.datetime(2021, 6, 14)
NUMBER_OF_MESSAGES = 10
MESSAGE_SIZE = 10
CLOUD = 'GCP'
UNIT_OF_TIME = 'milliseconds'

_AGGREGATE_SAMPLES = [
    sample.Sample(
        metric='publish_latency_mean',
        value=0.11147687435150147,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'samples': [
                0.20989608764648438, 0.2431643009185791, 0.14051604270935059,
                0.08317422866821289, 0.11351299285888672, 0.17781305313110352,
                0.037261247634887695, 0.030757904052734375,
                0.042165279388427734, 0.036507606506347656
            ],
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='publish_latency_mean_without_cold_start',
        value=0.06490101814270019,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='publish_latency_p50',
        value=0.0983436107635498,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='publish_latency_p99',
        value=0.2401701617240906,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='publish_latency_p999',
        value=0.2428648869991303,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='publish_latency_percentage_received',
        value=100.0,
        unit='%',
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_mean',
        value=1.384722399711609,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'samples': [
                2.4855735301971436,
                1.0239264965057373,
                1.0291249752044678,
                1.0388901233673096,
                1.0497450828552246,
                1.6815855503082275,
                1.1416656970977783,
                1.032665729522705,
                1.032970666885376,
                2.331076145172119
            ],
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_mean_without_cold_start',
        value=1.4439927577972411,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_p50',
        value=1.044317603111267,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_p99',
        value=2.4716687655448912,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_p999',
        value=2.4841830537319187,
        unit=UNIT_OF_TIME,
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
    sample.Sample(
        metric='pull_latency_percentage_received',
        value=100.0,
        unit='%',
        timestamp=1623628800.0,
        metadata={
            'number_of_messages': NUMBER_OF_MESSAGES,
            'message_size': MESSAGE_SIZE,
            'cloud': CLOUD
        }),
]


@freezegun.freeze_time(FAKE_DATETIME)
class MessagingServiceBenchmarkTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'messaging_service_output.json')
    with open(path) as fp:
      self.contents = json.loads(fp.read())

  @parameterized.named_parameters(('aggregate_samples', _AGGREGATE_SAMPLES))
  def testCreateSamples(self, expected_samples):
    actual_samples = messaging_service_benchmark._CreateSamples(
        self.contents, NUMBER_OF_MESSAGES, MESSAGE_SIZE, CLOUD)

    for expected_sample in expected_samples:
      if expected_sample not in actual_samples:
        sample_not_found_message = (
            f'Expected sample:\n{expected_sample}\nnot found in actual samples:'
            f'\n{actual_samples}')
        raise Exception(sample_not_found_message)

if __name__ == '__main__':
  unittest.main()
