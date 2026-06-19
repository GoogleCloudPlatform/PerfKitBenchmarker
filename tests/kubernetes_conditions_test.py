# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Unit tests for the kubernetes_conditions module."""

import json
import unittest
from unittest import mock
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_conditions
from tests import pkb_common_test_case


class KubernetesConditionsTest(pkb_common_test_case.PkbCommonTestCase):

  def testTimestampConvert(self):
    epoch_time = kubernetes_conditions.ConvertToEpochTime(
        '1970-01-01T00:00:00Z'
    )
    self.assertEqual(epoch_time, 0)
    self.assertEqual(
        kubernetes_conditions.ConvertToEpochTime('1970-01-01T00:01:00Z'),
        60,
    )

  def testPodStatusConditions(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod123'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'PodReadyToStartContainers',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'Initialized',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'ContainersReady',
                        },
                    ]
                },
            },
            {
                'metadata': {'name': 'pod456'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'PodScheduled',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(stdout, '', 0),
        )
    )
    conditions = kubernetes_conditions.GetStatusConditionsForResourceType(
        'pod',
        frozenset(),
    )
    self.assertLen(conditions, 5)

  def testPodStatusConditionsInvalid(self):
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(
                json.dumps({
                    'items': [{
                        'metadata': {'name': 'pod123'},
                        'status': {
                            'conditions': [{
                                'lastTransitionTime': None,
                                'status': 'True',
                                'message': (
                                    'Image docker.io is backed by'
                                    ' image streaming.'
                                ),
                                'type': 'ImageStreaming',
                            }]
                        },
                    }]
                }),
                '',
                0,
            ),
        )
    )
    conditions = kubernetes_conditions.GetStatusConditionsForResourceType(
        'pod',
        frozenset(),
    )
    self.assertEmpty(conditions)

  def testPodStatusConditionsWithIgnoredResources(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {'name': 'pod123'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'PodReadyToStartContainers',
                        },
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'Initialized',
                        },
                    ]
                },
            },
            {
                'metadata': {'name': 'pod456'},
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T18:51:17Z',
                            'status': 'True',
                            'type': 'PodScheduled',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(stdout, '', 0),
        )
    )
    conditions = kubernetes_conditions.GetStatusConditionsForResourceType(
        'pod',
        resources_to_ignore=frozenset(['pod456']),
    )
    self.assertLen(conditions, 2)

  def testStatusConditionsWithInstanceType(self):
    stdout = json.dumps({
        'items': [
            {
                'metadata': {
                    'name': 'node123',
                    'labels': {
                        'node.kubernetes.io/instance-type': 'n2-standard-4',
                    },
                },
                'status': {
                    'conditions': [
                        {
                            'lastProbeTime': None,
                            'lastTransitionTime': '1970-01-01T00:01:19Z',
                            'status': 'True',
                            'type': 'Ready',
                        },
                    ]
                },
            },
        ]
    })
    self.enter_context(
        mock.patch.object(
            kubectl,
            'RunKubectlCommand',
            return_value=(stdout, '', 0),
        )
    )
    conditions = kubernetes_conditions.GetStatusConditionsForResourceType(
        'node',
        frozenset(),
    )
    self.assertLen(conditions, 1)
    self.assertEqual(
        conditions[0].metadata,
        {'machine_type': 'n2-standard-4'},
    )


if __name__ == '__main__':
  unittest.main()
