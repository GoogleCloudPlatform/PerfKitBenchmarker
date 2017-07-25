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

"""Tests for beam_integration_benchmark."""

import unittest

from perfkitbenchmarker import beam_pipeline_options


class BeamArgsOptionsTestCase(unittest.TestCase):
  def testNoFlagsPassed(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        None, None, [], [])
    self.assertListEqual(options_list, [])


  def testAllFlagsPassed(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        "--itargone=anarg,--itargtwo=anotherarg",
        "[\"--project=testProj\","
        "\"--gcpTempLocation=gs://test-bucket/staging\"]",
        [{"postgresUsername": "postgres"}, {"postgresPassword": "mypass"}],
        [{"name": "aTestVal", "type": "TestValue", "value": "this_is_a_test"},
         {"name": "testier", "type": "TestValue", "value": "another_test"}]
    )

    self.assertListEqual(options_list,
                         ["\"--itargone=anarg\"",
                          "\"--itargtwo=anotherarg\"",
                          "\"--project=testProj\"",
                          "\"--gcpTempLocation=gs://test-bucket/staging\"",
                          "\"--aTestVal=this_is_a_test\"",
                          "\"--testier=another_test\"",
                          "\"--postgresUsername=postgres\"",
                          "\"--postgresPassword=mypass\""])


  def testItOptionsWithSpaces(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        None,
        "[\"--project=testProj\", "
        "\"--gcpTempLocation=gs://test-bucket/staging\"]",
        [],
        [])

    self.assertListEqual(options_list,
                         ["\"--project=testProj\"",
                          "\"--gcpTempLocation=gs://test-bucket/staging\""])


  def dynamicPipelineOptions(self):
    beam_pipeline_options.EvaluateDynamicPipelineOptions()


if __name__ == '__main__':
  unittest.main()
