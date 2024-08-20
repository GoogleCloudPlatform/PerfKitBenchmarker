# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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


import unittest
from unittest import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import ai_model_create_benchmark
from perfkitbenchmarker.resources import managed_ai_model
from tests import pkb_common_test_case


class AiModelCreateBenchmarkTest(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):

  def setUp(self):
    super().setUp()
    self.bm_spec = mock.create_autospec(benchmark_spec.BenchmarkSpec)
    self.bm_spec.ai_model = mock.create_autospec(
        managed_ai_model.BaseManagedAiModel
    )

  def testBenchmarkPassesForOneModel(self):
    self.bm_spec.ai_model.ListExistingEndpoints.return_value = [
        'model1',
    ]
    ai_model_create_benchmark.Run(self.bm_spec)

  def testBenchmarkFailsIfMoreModelsFound(self):
    self.bm_spec.ai_model.ListExistingEndpoints.return_value = [
        'model1',
        'model2',
    ]
    with self.assertRaises(errors.Benchmarks.PrepareException):
      ai_model_create_benchmark.Run(self.bm_spec)


if __name__ == '__main__':
  unittest.main()
