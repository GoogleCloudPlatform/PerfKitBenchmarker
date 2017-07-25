# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for beam_benchmark_helper."""

import unittest

from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import dpb_service


class BeamBenchmarkHelperTestCase(unittest.TestCase):
  def test_runner_option_override_non_dataflow(self):
    # This is documenting the current behavior - when we add an EMR
    # service_type, this test should change.
    actual_options = []
    beam_benchmark_helper.AddRunnerOptionMvnArgument(
        dpb_service.EMR, actual_options, None)
    self.assertListEqual([], actual_options)


  def test_runner_option_override_dataflow(self):
    actual_options = []
    beam_benchmark_helper.AddRunnerOptionMvnArgument(
        dpb_service.DATAFLOW, actual_options, None)
    self.assertListEqual(['"--runner=TestDataflowRunner"'], actual_options)


  def test_runner_option_override_use_override(self):
    testOptionVal = "--runner=TestVal"
    actual_options = []
    beam_benchmark_helper.AddRunnerOptionMvnArgument(
        dpb_service.DATAFLOW, actual_options, testOptionVal)
    self.assertListEqual([testOptionVal], actual_options)


  def test_runner_option_override_empty_override(self):
    testOptionVal = ""
    actual_options = []
    beam_benchmark_helper.AddRunnerOptionMvnArgument(
        dpb_service.DATAFLOW, actual_options, testOptionVal)
    self.assertListEqual([], actual_options)


  def test_runner_profile_override_dataflow(self):
    actual_mvn_command = []
    beam_benchmark_helper.AddRunnerProfileMvnArgument(
        dpb_service.DATAFLOW, actual_mvn_command, None)
    self.assertListEqual(['-Pdataflow-runner'], actual_mvn_command)


  def test_runner_profile_override_non_dataflow(self):
    # This is documenting the current behavior - when we add an EMR
    # service_type, this test should change.
    actual_mvn_command = []
    beam_benchmark_helper.AddRunnerProfileMvnArgument(
        dpb_service.EMR, actual_mvn_command, None)
    self.assertListEqual([], actual_mvn_command)


  def test_runner_profile_override_use_override(self):
    testOptionVal = "testval"
    actual_mvn_command = []
    beam_benchmark_helper.AddRunnerProfileMvnArgument(
        dpb_service.DATAFLOW, actual_mvn_command, testOptionVal)
    self.assertListEqual(['-P' + testOptionVal], actual_mvn_command)


  def test_runner_profile_override_empty_override(self):
    testOptionVal = ""
    actual_mvn_command = []
    beam_benchmark_helper.AddRunnerProfileMvnArgument(
        dpb_service.DATAFLOW, actual_mvn_command, testOptionVal)
    self.assertListEqual([], actual_mvn_command)


if __name__ == '__main__':
  unittest.main()
