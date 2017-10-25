# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.google_container_engine."""

# pylint: disable=not-context-manager

import unittest
import contextlib2
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import google_container_engine
from perfkitbenchmarker.providers.gcp import util
from tests import mock_flags

_COMPONENT = 'test_component'
_RUN_URI = 'fake-urn-uri'


@contextlib2.contextmanager
def patch_critical_objects(stdout='', stderr='', return_code=0):
  with contextlib2.ExitStack() as stack:
    flags = mock_flags.MockFlags()
    flags.gcloud_path = 'gcloud'
    flags.run_uri = _RUN_URI

    stack.enter_context(mock_flags.PatchFlags(flags))
    stack.enter_context(mock.patch('__builtin__.open'))
    stack.enter_context(mock.patch(vm_util.__name__ + '.PrependTempDir'))
    stack.enter_context(mock.patch(vm_util.__name__ + '.NamedTemporaryFile'))
    stack.enter_context(
        mock.patch(
            util.__name__ + '.GetDefaultProject', return_value='fakeproject'))

    retval = (stdout, stderr, return_code)
    issue_command = stack.enter_context(
        mock.patch(vm_util.__name__ + '.IssueCommand', return_value=retval))
    yield issue_command


def create_container_engine_spec():
  container_engine_spec = benchmark_config_spec._ContainerClusterSpec(
      'NAME', **{
          'cloud': 'GCP',
          'os_type': 'debian',
          'vm_spec': {
              'GCP': {
                  'machine_type': 'fake-machine-type'
              },
          },
          'vm_count': 2,
      })
  return container_engine_spec


class GoogleContainerEngineTestCase(unittest.TestCase):

  def testCreate(self):
    spec = create_container_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_container_engine.GkeCluster(spec)
      cluster._Create()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters create', command_string)
      self.assertIn('--num-nodes 2', command_string)
      self.assertIn('--machine-type fake-machine-type', command_string)

  def testPostCreate(self):
    spec = create_container_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_container_engine.GkeCluster(spec)
      cluster._PostCreate()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(
          'gcloud container clusters get-credentials pkb-{0}'.format(_RUN_URI),
          command_string)
      self.assertIn('KUBECONFIG', issue_command.call_args[1]['env'])

  def testDelete(self):
    spec = create_container_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_container_engine.GkeCluster(spec)
      cluster._Delete()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn('gcloud container clusters delete pkb-{0}'.format(_RUN_URI),
                    command_string)

  def testExists(self):
    spec = create_container_engine_spec()
    with patch_critical_objects() as issue_command:
      cluster = google_container_engine.GkeCluster(spec)
      cluster._Exists()
      command_string = ' '.join(issue_command.call_args[0][0])

      self.assertEqual(issue_command.call_count, 1)
      self.assertIn(
          'gcloud container clusters describe pkb-{0}'.format(_RUN_URI),
          command_string)


if __name__ == '__main__':
  unittest.main()
