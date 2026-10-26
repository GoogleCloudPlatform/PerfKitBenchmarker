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

import mock
from perfkitbenchmarker import beam_pipeline_options
from perfkitbenchmarker import errors
from perfkitbenchmarker.resources.container_service import kubernetes_commands


class BeamArgsOptionsTestCase(unittest.TestCase):

  def testNoFlagsPassed(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        None, None, [], []
    )
    self.assertListEqual(options_list, [])

  def testAllFlagsPassed(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        "--itargone=anarg,--itargtwo=anotherarg",
        '["--project=testProj","--gcpTempLocation=gs://test-bucket/staging"]',
        [{"postgresUsername": "postgres"}, {"postgresPassword": "mypass"}],
        [
            {
                "name": "aTestVal",
                "type": "TestValue",
                "value": "this_is_a_test",
            },
            {"name": "testier", "type": "TestValue", "value": "another_test"},
        ],
    )

    self.assertListEqual(
        options_list,
        [
            '"--itargone=anarg"',
            '"--itargtwo=anotherarg"',
            '"--project=testProj"',
            '"--gcpTempLocation=gs://test-bucket/staging"',
            '"--aTestVal=this_is_a_test"',
            '"--testier=another_test"',
            '"--postgresUsername=postgres"',
            '"--postgresPassword=mypass"',
        ],
    )

  def testItOptionsWithSpaces(self):
    options_list = beam_pipeline_options.GenerateAllPipelineOptions(
        None,
        '["--project=testProj", "--gcpTempLocation=gs://test-bucket/staging"]',
        [],
        [],
    )

    self.assertListEqual(
        options_list,
        [
            '"--project=testProj"',
            '"--gcpTempLocation=gs://test-bucket/staging"',
        ],
    )

  def testDynamicPipelineOpionsWithFormat(self):
    dynamic_options = [
        {
            "name": "test_value_A",
            "type": "TestValue",
            "value": "a_value",
            "format": "other representation of {{TestValue}}",
        },
        {"name": "test_value_B", "type": "TestValue", "value": "b_value"},
    ]

    self.assertListEqual(
        beam_pipeline_options.EvaluateDynamicPipelineOptions(dynamic_options),
        [
            ("test_value_A", "other representation of a_value"),
            ("test_value_B", "b_value"),
        ],
    )

  def dynamicPipelineOptions(self):
    beam_pipeline_options.EvaluateDynamicPipelineOptions()


class DynamicPipelineOptionFailureTestCase(unittest.TestCase):
  """Tests dynamic pipeline options whose values cannot be resolved."""

  # pylint: disable=invalid-name

  @mock.patch.object(
      kubernetes_commands, "GetWithWaitForContents", return_value=""
  )
  def testNodePortIpNotFound(self, _):
    """Raises a config error when no pod IP address is available."""
    dynamic_options = [
        {"name": "node_port", "type": "NodePortIp", "podLabel": "app=test"}
    ]

    with self.assertRaises(errors.Config.InvalidValue) as raised:
      beam_pipeline_options.EvaluateDynamicPipelineOptions(dynamic_options)

    self.assertIn("NodePort IP address", str(raised.exception))

  @mock.patch.object(
      kubernetes_commands, "GetWithWaitForContents", return_value=""
  )
  def testLoadBalancerIpNotFound(self, _):
    """Raises a config error when no load balancer IP address is available."""
    dynamic_options = [{
        "name": "load_balancer",
        "type": "LoadBalancerIp",
        "serviceName": "test-service",
    }]

    with self.assertRaises(errors.Config.InvalidValue) as raised:
      beam_pipeline_options.EvaluateDynamicPipelineOptions(dynamic_options)

    self.assertIn("LoadBalancer IP address", str(raised.exception))


if __name__ == "__main__":
  unittest.main()
