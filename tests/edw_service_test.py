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
import json
from typing import Any
import unittest
from unittest import mock
from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import edw_ai_extract_lib
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import vm_util
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
        edw_service.EdwService.ColsToRows(cols),  # pyrefly: ignore[bad-argument-type]
        msg=(
            # pyrefly: ignore[bad-argument-type]
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

  def testBaseConversationalAnalyticsClientInterfaceGetMetadata(self):
    class TestCaClientInterface(
        edw_service.BaseConversationalAnalyticsClientInterface
    ):

      @property
      def fetches_results_immediately(self) -> bool:
        return True

      @property
      def ca_client_name(self) -> str:
        return 'test_ca_client'

    ca_client = TestCaClientInterface()
    self.assertEqual(
        ca_client.GetMetadata(),
        {
            'fetches_results_immediately': 'True',
            'ca_client': 'test_ca_client',
        },
    )

  def testTryExecuteQueryCatchesRemoteCommandError(self) -> None:
    class TestClientInterface(edw_service.EdwClientInterface):

      def ExecuteQuery(self, query_name: str, print_results: bool = False):
        raise edw_service.errors.VirtualMachine.RemoteCommandError(
            'Syntax error in AI generated SQL'
        )

    # Arrange
    client = TestClientInterface()

    # Act
    execution_time, details = client.TryExecuteQuery('test_query')

    # Assert
    self.assertEqual(execution_time, -1.0)
    self.assertEqual(details, {})

  def testTryExecuteQueryRaisesOtherExceptions(self) -> None:
    class TestClientInterface(edw_service.EdwClientInterface):

      def ExecuteQuery(self, query_name: str, print_results: bool = False):
        raise RuntimeError('Unexpected failure')

    # Arrange
    client = TestClientInterface()

    # Act / Assert
    with self.assertRaises(RuntimeError):
      client.TryExecuteQuery('test_query')

  def testGetAiExtractClientInterfaceUnsupportedService(self) -> None:
    spec = benchmark_config_spec._EdwServiceSpec(
        'NAME', db=_PKB_CLUSTER_DATABASE
    )
    service = FakeEdwService(spec)
    with self.assertRaisesRegex(NotImplementedError, 'AI extract'):
      service.GetAiExtractClientInterface()


class _FakeAiExtractSchema:
  """Minimal schema stub satisfying AiExtractSchema."""

  def NestedTemplate(self) -> dict[str, Any]:
    return {'name': 'string'}

  def GeminiResponseSchema(self) -> dict[str, Any]:
    return {'type': 'OBJECT'}

  def SnowflakeResponseFormat(self) -> dict[str, Any]:
    return {'schema': {'type': 'object'}}

  def SnowflakeResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    return dict(response)

  def DatabricksSchema(self) -> dict[str, Any]:
    return {'name': 'string'}

  def DatabricksResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    return dict(response)


class _FakeSqlQueryClient(edw_service.EdwClientInterface):
  """Fake SQL query client recording executed statements."""

  def __init__(self, fetched_rows: dict[str, list[Any]]):
    super().__init__()
    self.client_vm = ClientVm()
    self.provisioned_with = None
    self.prepared_with = None
    self.queries: list[tuple[str, bool]] = []
    self._fetched_rows = fetched_rows

  def SetProvisionedAttributes(self, benchmark_spec: Any) -> None:
    self.provisioned_with = benchmark_spec

  def Prepare(self, package_name: str) -> None:
    self.prepared_with = package_name

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    self.queries.append((query_name, print_results))
    if query_name.endswith('_extract.sql'):
      return 12.5, {'query_id': 'qid-extract'}
    if query_name.endswith('_fetch.sql'):
      return 1.0, {'query_id': 'qid-fetch', 'query_results': self._fetched_rows}
    return 0.1, {'query_results': {}}


class _FakeSqlAiExtractClient(edw_service.BaseSqlAiExtractClientInterface):
  """Concrete BaseSqlAiExtractClientInterface for unit testing."""

  system = 'fake_sql'
  ERROR_KEY = 'error'

  def RenderExtractSql(
      self,
      results_table: str,
      documents: edw_ai_extract_lib.AiExtractDocuments,
      schema: edw_ai_extract_lib.AiExtractSchema,
  ) -> str:
    del schema
    return f'CREATE TABLE {results_table} AS SELECT {len(documents.doc_ids)};'

  def RenderFetchSql(self, results_table: str) -> str:
    return f'SELECT DOC_ID, EXTRACTION FROM {results_table};'

  def ResponseToPrediction(
      self,
      schema: edw_ai_extract_lib.AiExtractSchema,
      response: dict[str, Any],
  ) -> dict[str, Any]:
    return schema.SnowflakeResponseToCanonical(response)

  def GetMetadata(self) -> dict[str, str]:
    return {
        'extract_model': edw_ai_extract_lib.AI_EXTRACT_PLATFORM_DEFAULT_MODEL
    }


class BaseSqlAiExtractClientInterfaceTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    self.remote_files: dict[str, str] = {}
    self.enter_context(
        mock.patch.object(
            vm_util,
            'CreateRemoteFile',
            autospec=True,
            side_effect=lambda vm, text, path, write_mode='w': (
                self.remote_files.__setitem__(path, text)
            ),
        )
    )

  def testDelegatesSetupToQueryClient(self):
    # Arrange
    query_client = _FakeSqlQueryClient({})
    client = _FakeSqlAiExtractClient(query_client, 'run1')

    # Act
    client.SetProvisionedAttributes('fake_spec')
    client.Prepare('edw_common')

    # Assert
    self.assertFalse(client.emits_confidence)
    self.assertEqual(query_client.provisioned_with, 'fake_spec')
    self.assertEqual(query_client.prepared_with, 'edw_common')

  def testExtractAndCleanupLifecycle(self):
    # Arrange
    rows = {
        'DOC_ID': ['0001'],
        'EXTRACTION': [
            json.dumps({'error': None, 'response': {'name': 'Jane'}})
        ],
    }
    query_client = _FakeSqlQueryClient(rows)
    client = _FakeSqlAiExtractClient(query_client, 'run1')
    paystub_docs = edw_ai_extract_lib.AiExtractDocuments(
        'paystub', 'gs://b/paystub/', {'0001': 'documents/0001.pdf'}
    )
    w2_docs = edw_ai_extract_lib.AiExtractDocuments(
        'w2', 'gs://b/w2/', {'0001': 'documents/0001.pdf'}
    )
    schema = _FakeAiExtractSchema()

    # Act
    first = client.Extract(paystub_docs, schema)
    client.Extract(paystub_docs, schema)
    client.Extract(w2_docs, schema)
    client.Cleanup()
    client.Cleanup()  # Second call should be a no-op.

    # Assert
    with self.subTest('extract_result'):
      self.assertEqual(first.wall_time_sec, 12.5)
      self.assertEqual(first.usage_metadata, {'query_id': 'qid-extract'})
      self.assertEqual(first.document_results[0].prediction, {'name': 'Jane'})
    with self.subTest('executed_queries'):
      self.assertEqual(
          query_client.queries,
          [
              ('ai_extract_results_run1_paystub_1_extract.sql', False),
              ('ai_extract_results_run1_paystub_1_fetch.sql', True),
              ('ai_extract_results_run1_paystub_2_extract.sql', False),
              ('ai_extract_results_run1_paystub_2_fetch.sql', True),
              ('ai_extract_results_run1_w2_1_extract.sql', False),
              ('ai_extract_results_run1_w2_1_fetch.sql', True),
              ('drop_ai_extract_results_run1_paystub_1.sql', False),
              ('drop_ai_extract_results_run1_paystub_2.sql', False),
              ('drop_ai_extract_results_run1_w2_1.sql', False),
          ],
      )
      self.assertEqual(
          self.remote_files['drop_ai_extract_results_run1_paystub_1.sql'],
          'DROP TABLE IF EXISTS ai_extract_results_run1_paystub_1;\n',
      )

  def testExtractChecksCoverage(self):
    # Arrange
    query_client = _FakeSqlQueryClient({'DOC_ID': [], 'EXTRACTION': []})
    client = _FakeSqlAiExtractClient(query_client, 'run1')
    docs = edw_ai_extract_lib.AiExtractDocuments(
        'paystub', 'gs://b/paystub/', {'0001': 'documents/0001.pdf'}
    )

    # Act / Assert
    with self.assertRaisesRegex(ValueError, "missing=\\['0001'\\]"):
      client.Extract(docs, _FakeAiExtractSchema())


class ConcreteClaudeClientInterface(
    edw_service.BaseClaudeConversationalAnalyticsClientInterface
):
  """Fake Claude client interface for testing."""

  fetches_results_immediately = True


class BaseClaudeConversationalAnalyticsClientInterfaceTest(
    pkb_common_test_case.PkbCommonTestCase
):

  def setUp(self):
    super().setUp()
    self.client = ConcreteClaudeClientInterface()

  def testParseConversationalAnalyticsResultsIncludingCost(self):
    results = {
        'query_wall_time_in_secs': 5.0,
        'details': {
            'job_id': 'job_123',
            'query_results': {
                'text_response': 'The total sales is $1000.',
                'generated_sql': 'SELECT sum(sales) FROM t',
                'retrieved_data': [['1000']],
                'total_cost_usd': 0.05,
                'usage': {'input_tokens': 100},
            },
        },
    }
    execution_time, metadata = self.client._ParseConversationalAnalyticsResults(
        results, 'test query'
    )
    self.assertEqual(execution_time, 5.0)
    self.assertEqual(metadata['total_cost_usd'], 0.05)
    self.assertEqual(metadata['usage'], {'input_tokens': 100})

  def testGetMetadataReportsClaudeCaClient(self):
    self.assertEqual(self.client.GetMetadata()['ca_client'], 'claude')


if __name__ == '__main__':
  unittest.main()
