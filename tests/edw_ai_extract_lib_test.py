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
"""Unit tests for EDW AI extraction document locations and coverage checks."""

import json
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from perfkitbenchmarker import edw_ai_extract_lib

_DOCS = edw_ai_extract_lib.AiExtractDocuments(
    dataset_id='paystub',
    storage_prefix='gs://bucket/bqmmdlab/paystub/',
    paths={
        '0001': 'documents/0001.pdf',
        '0002': 'documents/0002.pdf',
    },
)


def _Record(
    doc_id: str,
    prediction: dict[str, Any],
    confidence: dict[str, list[str]] | None = None,
) -> edw_ai_extract_lib.AiExtractDocumentResult:
  return edw_ai_extract_lib.AiExtractDocumentResult(
      doc_id=doc_id,
      error=None,
      raw_output='',
      prediction=prediction,
      confidence=confidence,
      usage={},
  )


def _SnowflakeToCanonical(response: dict[str, Any]) -> dict[str, Any]:
  if set(response) - {'name', 'item'}:
    raise ValueError(f'unexpected keys: {sorted(response)}')
  item = response.get('item', {})
  if not isinstance(item, dict) or set(item) - {'amount'}:
    raise ValueError(f'unexpected item: {item!r}')
  return {
      'name': response.get('name'),
      'item': [{'amount': v} for v in item.get('amount', [])],
  }


def _DatabricksToCanonical(response: dict[str, Any]) -> dict[str, Any]:
  if set(response) - {'name', 'item'}:
    raise ValueError(f'unexpected keys: {sorted(response)}')
  return {
      'name': response['name']['value'],
      'item': [{'amount': row['amount']['value']} for row in response['item']],
  }


class AiExtractDocumentsTest(absltest.TestCase):

  def testLocations(self):
    with self.subTest('doc_ids'):
      self.assertEqual(_DOCS.doc_ids, ['0001', '0002'])
    with self.subTest('gcs_uri'):
      self.assertEqual(
          _DOCS.GcsUri('0001'),
          'gs://bucket/bqmmdlab/paystub/documents/0001.pdf',
      )
    with self.subTest('bucket_path'):
      self.assertEqual(
          _DOCS.BucketPath('0001'), 'bqmmdlab/paystub/documents/0001.pdf'
      )
    with self.subTest('storage_folder'):
      self.assertEqual(_DOCS.StorageFolder(), 'paystub')
    with self.subTest('folder_relative_path'):
      self.assertEqual(
          _DOCS.FolderRelativePath('0001'), 'paystub/documents/0001.pdf'
      )

  def testStorageFolderPreservesCase(self):
    docs = edw_ai_extract_lib.AiExtractDocuments(
        dataset_id='bank_statement_2l',
        storage_prefix='gs://bucket/bqmmdlab/bank_statement_2L/',
        paths={'0001': 'documents/0001.pdf'},
    )
    with self.subTest('storage_folder'):
      self.assertEqual(docs.StorageFolder(), 'bank_statement_2L')
    with self.subTest('folder_relative_path'):
      self.assertEqual(
          docs.FolderRelativePath('0001'),
          'bank_statement_2L/documents/0001.pdf',
      )

  def testBucketRootHasNoFolder(self):
    docs = edw_ai_extract_lib.AiExtractDocuments(
        'd', 'gs://bucket/', {'1': 'a.pdf'}
    )
    with self.subTest('bucket_path'):
      self.assertEqual(docs.BucketPath('1'), 'a.pdf')
    with self.subTest('storage_folder_raises'):
      with self.assertRaisesRegex(ValueError, 'has no folder'):
        docs.StorageFolder()

  def testFailedResultHasNoPrediction(self):
    with self.assertRaisesRegex(ValueError, 'no prediction'):
      edw_ai_extract_lib.AiExtractDocumentResult(
          '1', 'boom', '', {'a': 1}, None, {}
      )


class ParseAiExtractRowsTest(absltest.TestCase):

  def testParseRowsEmpty(self):
    self.assertEqual(
        edw_ai_extract_lib.ParseAiExtractRows(
            {}, _SnowflakeToCanonical, 'error'
        ),
        [],
    )

  def testParseRowsSnowflake(self):
    # Arrange
    rows = {
        'DOC_ID': ['0001', '0002', '0003', '0004'],
        'EXTRACTION': [
            json.dumps({
                'error': None,
                'response': {'name': 'Jane', 'item': {'amount': ['1', '2']}},
            }),
            json.dumps({'error': 'doc too long', 'response': None}),
            json.dumps({'error': None, 'response': {'bogus': 'x'}}),
            None,
        ],
    }

    # Act
    results = edw_ai_extract_lib.ParseAiExtractRows(
        rows, _SnowflakeToCanonical, 'error'
    )

    # Assert
    with self.subTest('valid_response'):
      self.assertIsNone(results[0].error)
      self.assertEqual(
          results[0].prediction,
          {'name': 'Jane', 'item': [{'amount': '1'}, {'amount': '2'}]},
      )
    with self.subTest('platform_error'):
      self.assertEqual(results[1].error, 'doc too long')
    with self.subTest('unexpected_shape'):
      self.assertIn('unexpected response shape', str(results[2].error))
    with self.subTest('null_extraction'):
      self.assertEqual(results[3].error, 'null extraction result')
      self.assertEqual(results[3].raw_output, '')

  def testParseRowsNullResponseIsErrorNotEmptyPrediction(self):
    # Arrange
    rows = {
        'DOC_ID': ['0001', '0002'],
        'EXTRACTION': [
            json.dumps({'error': None, 'response': None}),
            json.dumps({}),
        ],
    }

    # Act
    results = edw_ai_extract_lib.ParseAiExtractRows(
        rows, _SnowflakeToCanonical, 'error'
    )

    # Assert
    with self.subTest('explicit_null_response'):
      self.assertEqual(results[0].error, 'null response')
    with self.subTest('missing_response_key'):
      self.assertEqual(results[1].error, 'null response')
      self.assertEqual(results[1].raw_output, '{}')

  def testParseRowsDatabricks(self):
    # Arrange
    rows = {
        'DOC_ID': ['0001'],
        'EXTRACTION': [
            json.dumps({
                'error_message': None,
                'response': {
                    'name': {'value': 'Jane'},
                    'item': [{'amount': {'value': 5}}],
                },
            })
        ],
    }

    # Act
    results = edw_ai_extract_lib.ParseAiExtractRows(
        rows, _DatabricksToCanonical, 'error_message'
    )

    # Assert
    self.assertEqual(
        results[0].prediction, {'name': 'Jane', 'item': [{'amount': 5}]}
    )


class CheckAiExtractCoverageTest(parameterized.TestCase):

  def testValidCoverage(self):
    edw_ai_extract_lib.CheckAiExtractCoverage([_Record('1', {})], ['1'])

  @parameterized.named_parameters(
      ('missing', [_Record('1', {})], ['1', '2'], r"missing=\['2'\]"),
      (
          'duplicates',
          [_Record('1', {}), _Record('1', {})],
          ['1'],
          'duplicates=1',
      ),
      ('extra', [_Record('1', {}), _Record('2', {})], ['1'], r"extra=\['2'\]"),
  )
  def testInvalidCoverageRaises(self, results, doc_ids, expected_regex):
    with self.assertRaisesRegex(ValueError, expected_regex):
      edw_ai_extract_lib.CheckAiExtractCoverage(results, doc_ids)


if __name__ == '__main__':
  absltest.main()
