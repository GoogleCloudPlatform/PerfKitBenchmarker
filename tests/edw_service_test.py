# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for edw_service.py."""

import builtins
import copy
import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import edw_service
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import pkb_common_test_case

_CLUSTER_PARAMETER_GROUP = 'fake_redshift_cluster_parameter_group'
_CLUSTER_SUBNET_GROUP = 'fake_redshift_cluster_subnet_group'
_PKB_CLUSTER = 'pkb-cluster'
_PKB_CLUSTER_DATABASE = 'pkb-database'
_REDSHIFT_NODE_TYPE = 'dc2.large'
_USERNAME = 'pkb-username'
_PASSWORD = 'pkb-password'
_TEST_RUN_URI = 'fakeru'

_AWS_ZONE_US_EAST_1A = 'us-east-1a'

CSV_CONTENT = """Question ID,DB ID,Question,Evidence,Difficulty,Ground Truth SQL,Table Referenced,cuj_tag
6,call_center,Which 3 customer segments have the most conversations,,Simple,SELECT 1;,"[call_center.transcript, retail_banking.client]",ca_tag
7,ecomm,How many sessions with a cart in 2023?,,Simple,SELECT 2;,ecomm.events,ca_tag
"""

_BASE_REDSHIFT_SPEC = {
    'cluster_identifier': _PKB_CLUSTER,
    'db': _PKB_CLUSTER_DATABASE,
    'user': _USERNAME,
    'password': _PASSWORD,
    'node_type': _REDSHIFT_NODE_TYPE,
    'node_count': 1,
}

FLAGS = flags.FLAGS


class ClientVm:
  """A fake VM class that can proxies a remote command to execute query."""

  def RemoteCommand(self, command):
    """Returns sample output for executing a query."""
    pass


class PreparedClientVm:

  def Install(self, package_name):
    if package_name != 'pip':
      raise RuntimeError

  def RemoteCommand(self, command):
    pass


class FakeEdwService(edw_service.EdwService):
  """A fake Edw Service class."""

  def _Create(self):
    pass

  def _Delete(self):
    pass


class EdwServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    FLAGS.run_uri = _TEST_RUN_URI
    FLAGS.zones = [_AWS_ZONE_US_EAST_1A]

  def testIsUserManaged(self):
    kwargs = copy.copy(
        {'cluster_identifier': _PKB_CLUSTER, 'db': _PKB_CLUSTER_DATABASE}
    )
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertTrue(edw_local.IsUserManaged(spec))

  def testIsPkbManaged(self):
    kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertFalse(edw_local.IsUserManaged(spec))

  def testUserManagedGetClusterIdentifier(self):
    kwargs = copy.copy(
        {'cluster_identifier': _PKB_CLUSTER, 'db': _PKB_CLUSTER_DATABASE}
    )
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertEqual(_PKB_CLUSTER, edw_local.GetClusterIdentifier(spec))
    self.assertEqual(_PKB_CLUSTER, edw_local.cluster_identifier)

  def testPkbManagedGetClusterIdentifier(self):
    kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    edw_local = FakeEdwService(spec)
    self.assertEqual(
        'pkb-' + FLAGS.run_uri, edw_local.GetClusterIdentifier(spec)
    )
    self.assertEqual('pkb-' + FLAGS.run_uri, edw_local.cluster_identifier)

  @parameterized.named_parameters(
      dict(
          testcase_name='_empty_dict',
          cols={},
          expected=[],
      ),
      dict(
          testcase_name='_single_column',
          cols={'col1': ['val1', 'val2']},
          expected=[{'col1': 'val1'}, {'col1': 'val2'}],
      ),
      dict(
          testcase_name='_multiple_columns',
          cols={'col1': ['val1', 'val2'], 'col2': ['val3', 'val4']},
          expected=[
              {'col1': 'val1', 'col2': 'val3'},
              {'col1': 'val2', 'col2': 'val4'},
          ],
      ),
      dict(
          testcase_name='_uneven_columns',
          cols={'col1': ['val1', 'val2'], 'col2': ['val3', 'val4', 'val5']},
          expected=[
              {'col1': 'val1', 'col2': 'val3'},
              {'col1': 'val2', 'col2': 'val4'},
          ],
      ),
  )
  def testColsToRows(self, cols=None, expected=None):
    self.assertEqual(
        expected,
        edw_service.EdwService.ColsToRows(cols),
        msg=(
            f'Expected {expected} but got'
            f' {edw_service.EdwService.ColsToRows(cols)}'
        ),
    )

  @flagsaver.flagsaver(
      conversational_analytics_questions_file='test_questions.csv'
  )
  def testInitLoadsQuestionsFromResource(self):
    mock_resource_path = self.enter_context(
        mock.patch.object(
            edw_service.data,
            'ResourcePath',
            autospec=True,
            return_value='/fake/path/test_questions.csv',
        )
    )

    mock_open = mock.mock_open(read_data=CSV_CONTENT)
    with mock.patch.object(builtins, 'open', mock_open):
      kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
      spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
      service = FakeEdwService(spec)

    with self.subTest('FileRead'):
      mock_resource_path.assert_called_once_with('test_questions.csv')
      mock_open.assert_called_once_with(
          '/fake/path/test_questions.csv', 'r', encoding='utf-8'
      )

    with self.subTest('QuestionsContent'):
      questions = service.GetConversationalAnalyticsQuestionList()
      self.assertLen(questions, 2)

      self.assertEqual(
          questions[0].question,
          'Which 3 customer segments have the most conversations',
      )
      self.assertEqual(questions[0].db_id, 'call_center')
      self.assertEqual(questions[0].ground_truth_sql, 'SELECT 1;')

      self.assertEqual(
          questions[1].question, 'How many sessions with a cart in 2023?'
      )
      self.assertEqual(questions[1].db_id, 'ecomm')
      self.assertEqual(questions[1].ground_truth_sql, 'SELECT 2;')

  def testGetConversationalAnalyticsQuestionListNotPrepared(self):
    kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    service = FakeEdwService(spec)
    with self.assertRaisesRegex(
        ValueError, 'Conversational analytics questions not loaded.'
    ):
      service.GetConversationalAnalyticsQuestionList()

  @flagsaver.flagsaver(
      conversational_analytics_questions_file='test_questions_empty.csv'
  )
  def testInitLoadsQuestionsEmptyFileThrowsError(self):
    self.enter_context(
        mock.patch.object(
            edw_service.data,
            'ResourcePath',
            autospec=True,
            return_value='/fake/path/test_questions_empty.csv',
        )
    )
    mock_open = mock.mock_open(
        read_data=(
            'Question ID,DB ID,Question,Evidence,Difficulty,Ground Truth'
            ' SQL,Table Referenced,cuj_tag\n'
        )
    )
    with mock.patch.object(builtins, 'open', mock_open):
      kwargs = copy.copy({'db': _PKB_CLUSTER_DATABASE})
      spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
      with self.assertRaisesRegex(
          edw_service.errors.Benchmarks.RunError,
          'Loaded 0 questions from /fake/path/test_questions_empty.csv.',
      ):
        FakeEdwService(spec)


if __name__ == '__main__':
  unittest.main()
