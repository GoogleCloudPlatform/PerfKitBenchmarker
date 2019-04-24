# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the Tensorflow benchmark."""
import unittest
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_benchmarks import tensorflow_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class TensorflowBenchmarkBatchSizesTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(TensorflowBenchmarkBatchSizesTestCase, self).setUp()
    FLAGS.tf_batch_sizes = [99]

  def testFlagOverridesDefaultBatchSize(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', tensorflow_benchmark.CPU)
    self.assertEqual([99], batch_size)


class TensorflowBenchmarkDefaultBatchSizesTestCase(
    pkb_common_test_case.PkbCommonTestCase):

  def testUnknownGpuTypeReturnsDefaultBatchSize(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', 'unknown_gpu_type')
    self.assertEqual([64], batch_size)

  def testUnknownModelReturnsDefaultBatchSize(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'unknown_model', tensorflow_benchmark.CPU)
    self.assertEqual([64], batch_size)

  def testCpuAlexnetDefault(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', tensorflow_benchmark.CPU)
    self.assertEqual([512], batch_size)

  def testCpuInception3Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'inception3', tensorflow_benchmark.CPU)
    self.assertEqual([64], batch_size)

  def testCpuResnet50Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet50', tensorflow_benchmark.CPU)
    self.assertEqual([64], batch_size)

  def testCpuResnet152Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet152', tensorflow_benchmark.CPU)
    self.assertEqual([32], batch_size)

  def testCpuVgg16Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'vgg16', tensorflow_benchmark.CPU)
    self.assertEqual([32], batch_size)

  def testK80AlexnetDefault(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', tensorflow_benchmark.NVIDIA_TESLA_K80)
    self.assertEqual([512], batch_size)

  def testK80Inception3Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'inception3', tensorflow_benchmark.NVIDIA_TESLA_K80)
    self.assertEqual([64], batch_size)

  def testK80Resnet50Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet50', tensorflow_benchmark.NVIDIA_TESLA_K80)
    self.assertEqual([64], batch_size)

  def testK80Resnet152Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet152', tensorflow_benchmark.NVIDIA_TESLA_K80)
    self.assertEqual([32], batch_size)

  def testK80Vgg16Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'vgg16', tensorflow_benchmark.NVIDIA_TESLA_K80)
    self.assertEqual([32], batch_size)

  def testP100AlexnetDefault(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', tensorflow_benchmark.NVIDIA_TESLA_P100)
    self.assertEqual([512], batch_size)

  def testP100Inception3Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'inception3', tensorflow_benchmark.NVIDIA_TESLA_P100)
    self.assertEqual([256], batch_size)

  def testP100Resnet50Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet50', tensorflow_benchmark.NVIDIA_TESLA_P100)
    self.assertEqual([256], batch_size)

  def testP100Resnet152Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet152', tensorflow_benchmark.NVIDIA_TESLA_P100)
    self.assertEqual([128], batch_size)

  def testP100Vgg16Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'vgg16', tensorflow_benchmark.NVIDIA_TESLA_P100)
    self.assertEqual([128], batch_size)

  def testV100AlexnetDefault(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'alexnet', tensorflow_benchmark.NVIDIA_TESLA_V100)
    self.assertEqual([512], batch_size)

  def testV100Inception3Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'inception3', tensorflow_benchmark.NVIDIA_TESLA_V100)
    self.assertEqual([256], batch_size)

  def testV100Resnet50Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet50', tensorflow_benchmark.NVIDIA_TESLA_V100)
    self.assertEqual([256], batch_size)

  def testV100Resnet152Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'resnet152', tensorflow_benchmark.NVIDIA_TESLA_V100)
    self.assertEqual([128], batch_size)

  def testV100Vgg16Default(self):
    batch_size = tensorflow_benchmark._GetBatchSizes(
        'vgg16', tensorflow_benchmark.NVIDIA_TESLA_V100)
    self.assertEqual([128], batch_size)


if __name__ == '__main__':
  unittest.main()
