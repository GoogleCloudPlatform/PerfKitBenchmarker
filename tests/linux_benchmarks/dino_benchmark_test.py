"""Tests for dino_benchmark."""

import os
import unittest

from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import dino_benchmark
from tests import pkb_common_test_case


class DinoTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super(DinoTestCase, self).setUp()
    path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'dino_output.txt'
    )
    with open(path) as fp:
      self.contents = fp.read()
    self.samples = dino_benchmark.MakeSamplesFromOutput(self.contents)

  def testTrainResults(self):
    self.assertLen(self.samples, 3)

    self.assertEqual(self.samples[0].metric, 'total_time')
    self.assertAlmostEqual(self.samples[0].value, 1263.0)
    self.assertEqual(self.samples[0].unit, 's')
    self.assertEqual(self.samples[0].metadata['epoch'], 98)
    self.assertEqual(self.samples[0].metadata['total_epochs'], 100)
    self.assertAlmostEqual(self.samples[0].metadata['total_time'], 1263)
    self.assertAlmostEqual(self.samples[0].metadata['iterable_time'], 0.505057)

    self.assertEqual(self.samples[1].metric, 'total_time')
    self.assertAlmostEqual(self.samples[1].value, 1259.0)
    self.assertEqual(self.samples[1].unit, 's')
    self.assertEqual(self.samples[1].metadata['epoch'], 99)
    self.assertEqual(self.samples[1].metadata['total_epochs'], 100)
    self.assertAlmostEqual(self.samples[1].metadata['total_time'], 1259)
    self.assertAlmostEqual(self.samples[1].metadata['iterable_time'], 0.50352)

  def TestTrainingTime(self):
    self.assertEqual(self.samples[2].metric, 'training_time')
    self.assertAlmostEqual(self.samples[2].value, 127540)
    self.assertEqual(self.samples[2].unit, 's')


if __name__ == '__main__':
  unittest.main()
