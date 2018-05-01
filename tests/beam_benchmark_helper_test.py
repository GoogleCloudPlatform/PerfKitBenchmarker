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


class BeamBenchmarkHelperTestCase(unittest.TestCase):

  def test_runner_option_override_use_override(self):
    testOptionVal = "TestVal"
    actual_options = []
    beam_benchmark_helper.AddRunnerPipelineOption(actual_options, None, testOptionVal)
    self.assertListEqual(["--runner=" + testOptionVal], actual_options)

  def test_runner_option_override_empty_override(self):
    testOptionVal = ""
    actual_options = []
    beam_benchmark_helper.AddRunnerPipelineOption(actual_options, None, testOptionVal)
    self.assertListEqual([], actual_options)

  def test_dataflow_runner_name_added(self):
    testOptionVal = "dataflow"
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, testOptionVal)
    self.assertListEqual(["-DintegrationTestRunner=" + testOptionVal], actual_command)

  def test_direct_runner_name_added(self):
    testOptionVal = "direct"
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, testOptionVal)
    self.assertListEqual(["-DintegrationTestRunner=" + testOptionVal], actual_command)


  def test_runner_name_empty(self):
    testOptionVal = ""
    actual_command = []
    beam_benchmark_helper.AddRunnerArgument(actual_command, testOptionVal)
    self.assertListEqual([], actual_command)

  def test_extra_property_empty_property(self):
    testOptionVal = ""
    actual_command = []
    beam_benchmark_helper.AddExtraProperties(actual_command, testOptionVal)
    self.assertListEqual([], actual_command)

  def test_extra_property_single_property(self):
    testOptionVal = "[key=value]"
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command, testOptionVal)
    self.assertListEqual(["-Dkey=value"], actual_mvn_command)

  def test_extra_property_single_property_quoted(self):
    testOptionVal = "[\"key=value\"]"
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command, testOptionVal)
    self.assertListEqual(["-Dkey=value"], actual_mvn_command)

  def test_extra_property_multiple_properties(self):
    testOptionVal = "[\"key=value\", \"key2=value2\"]"
    actual_mvn_command = []
    beam_benchmark_helper.AddExtraProperties(actual_mvn_command, testOptionVal)
    self.assertListEqual(["-Dkey=value", "-Dkey2=value2"], actual_mvn_command)

  def test_integrationPipelineOptions_rejection(self):
    testOptionVal = "[\"integrationTestPipelineOptions=...\"]"
    actual_mvn_command = []
    with self.assertRaises(ValueError):
      beam_benchmark_helper.AddExtraProperties(actual_mvn_command, testOptionVal)

  def test_hdfs_filesystem_addition(self):
    testOptionVal = "hdfs"
    actual_command = []
    beam_benchmark_helper.AddFilesystemArgument(actual_command, testOptionVal)
    self.assertListEqual(["-Dfilesystem=hdfs"], actual_command)

  def test_empty_filesystem(self):
    testOptionVal = ""
    actual_command = []
    beam_benchmark_helper.AddFilesystemArgument(actual_command, testOptionVal)
    self.assertListEqual([], actual_command)

if __name__ == '__main__':
  unittest.main()
