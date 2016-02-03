# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for perfkitbenchmarker.context."""

import unittest

import mock

from perfkitbenchmarker import context
from perfkitbenchmarker import vm_util


class ThreadLocalBenchmarkSpecTestCase(unittest.TestCase):

  def setUp(self):
    # Reset the current benchmark spec.
    self.addCleanup(context.SetThreadBenchmarkSpec, None)

  def testSetGet(self):
    benchmark_spec = mock.MagicMock()
    context.SetThreadBenchmarkSpec(benchmark_spec)
    self.assertEqual(benchmark_spec, context.GetThreadBenchmarkSpec())

  def testPropagation(self):
    benchmark_spec = mock.MagicMock()
    context.SetThreadBenchmarkSpec(benchmark_spec)

    def _DoWork(_):
      self.assertEqual(benchmark_spec, context.GetThreadBenchmarkSpec())
      new_benchmark_spec = mock.MagicMock()
      context.SetThreadBenchmarkSpec(new_benchmark_spec)
      self.assertNotEqual(benchmark_spec, context.GetThreadBenchmarkSpec())
      self.assertEqual(new_benchmark_spec,
                       context.GetThreadBenchmarkSpec())

    vm_util.RunThreaded(_DoWork, range(10))


if __name__ == '__main__':
  unittest.main()
