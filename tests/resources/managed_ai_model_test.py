"""Tests for managed_ai_model."""

import time
from typing import Any
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources import managed_ai_model
from tests import pkb_common_test_case

_ZONE = 'us-west-1a'


class ManagedAiModelImplementation(managed_ai_model.BaseManagedAiModel):
  CLOUD = 'TEST'

  def GetRegionFromZone(self, zone: str) -> str:
    return zone + '-region'

  def _SendPrompt(
      self, prompt: str, max_tokens: int, temperature: float, **kwargs: Any
  ) -> list[str]:
    return [prompt]

  def _Create(self) -> None:
    pass

  def _Delete(self) -> None:
    pass


class ManagedAiModelTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.time_mock = self.enter_context(
        mock.patch.object(time, 'time', side_effect=range(100, 200))
    )
    self.enter_context(flagsaver.flagsaver(zone=[_ZONE]))

  def testUsesRegion(self):
    ai_model = ManagedAiModelImplementation()
    self.assertEqual(ai_model.region, 'us-west-1a-region')

  def testThrowsRegionUnset(self):
    self.enter_context(flagsaver.flagsaver(zone=None))
    with self.assertRaises(errors.Setup.InvalidConfigurationError):
      ManagedAiModelImplementation()

  def testSamplesAddedForSendingPrompt(self):
    ai_model = ManagedAiModelImplementation()
    self.assertEmpty(ai_model.GetSamples())
    for i in range(1, 5):
      ai_model.SendPrompt('who?', 10, 1.0)
      self.assertLen(ai_model.GetSamples(), i)

  def testSampleTimedSendingPrompt(self):
    ai_model = ManagedAiModelImplementation()
    ai_model.SendPrompt('who?', 10, 1.0)
    samples = ai_model.GetSamples()
    self.assertEqual(
        samples[0],
        sample.Sample(
            'response_time_0',
            1,
            'seconds',
            {'region': 'us-west-1a-region'},
            timestamp=102,
        ),
    )


if __name__ == '__main__':
  unittest.main()
