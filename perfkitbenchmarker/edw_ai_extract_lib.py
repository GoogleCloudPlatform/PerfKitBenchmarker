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
"""Types and helpers for EDW AI (entity) extraction benchmarks."""

from collections.abc import Callable, Sequence
import dataclasses
import json
import posixpath
from typing import Any, Protocol

# Neither Snowflake AI_EXTRACT nor Databricks ai_extract exposes a model
# parameter; this is recorded as their model in benchmark metadata.
AI_EXTRACT_PLATFORM_DEFAULT_MODEL = 'platform_default'


class AiExtractSchema(Protocol):
  """An entity extraction schema, rendered in each platform's native form.

  Each AI extract client uses only its own platform's renderer(s) and response
  converter; a converter maps the platform's native response to the nested,
  platform-neutral prediction and raises ValueError on an unexpected shape.
  """

  def NestedTemplate(self) -> dict[str, Any]:
    ...

  def GeminiResponseSchema(self) -> dict[str, Any]:
    ...

  def SnowflakeResponseFormat(self) -> dict[str, Any]:
    ...

  def SnowflakeResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    ...

  def DatabricksSchema(self) -> dict[str, Any]:
    ...

  def DatabricksResponseToCanonical(
      self, response: dict[str, Any]
  ) -> dict[str, Any]:
    ...


@dataclasses.dataclass(frozen=True)
class AiExtractDocuments:
  """Documents for one dataset to extract, located by GCS source of truth.

  Each platform maps a document to its own copy (e.g. a Snowflake stage rooted
  at the bucket root, or a Databricks volume holding one folder per dataset).

  Attributes:
    dataset_id: ID of the dataset the documents belong to (e.g. 'w2').
    storage_prefix: 'gs://<bucket>/<prefix>/' of the dataset's folder (e.g.
      'gs://bucket/bqmmdlab/w2/').
    paths: doc_id -> document path relative to storage_prefix, in request
      order (e.g. {'0001': 'documents/0001.pdf', '0002':
      'documents/0002.pdf'}).
  """

  dataset_id: str
  storage_prefix: str
  paths: dict[str, str]

  @property
  def doc_ids(self) -> list[str]:
    """Return document IDs in request order."""
    return list(self.paths)

  def GcsUri(self, doc_id: str) -> str:
    """Return the gs:// URI of a document."""
    return self.storage_prefix + self.paths[doc_id]

  def BucketPath(self, doc_id: str) -> str:
    """Return the object path in its bucket (without 'gs://<bucket>/')."""
    return self.GcsUri(doc_id).split('/', 3)[3]

  def StorageFolder(self) -> str:
    """Return the name of the documents' folder in the bucket.

    Returns:
      E.g. 'bank_statement_2L' for 'gs://bucket/bqmmdlab/bank_statement_2L/'.

    Raises:
      ValueError: If storage_prefix is a bucket root (no folder).
    """
    if self.storage_prefix.count('/') <= 3:  # 'gs://bucket/'
      raise ValueError(f'storage_prefix {self.storage_prefix!r} has no folder')
    return posixpath.basename(self.storage_prefix.rstrip('/'))

  def FolderRelativePath(self, doc_id: str) -> str:
    """Return the path of a document under its folder's parent directory.

    Args:
      doc_id: ID of the document.

    Returns:
      E.g. 'paystub/documents/0001.pdf' for
      'gs://bucket/bqmmdlab/paystub/documents/0001.pdf'.
    """
    return f'{self.StorageFolder()}/{self.paths[doc_id]}'


@dataclasses.dataclass(frozen=True)
class AiExtractDocumentResult:
  """One document's extraction result.

  Attributes:
    doc_id: Document ID.
    error: Error message iff extraction failed for the document.
    raw_output: Verbatim platform output, for debugging.
    prediction: Nested prediction conforming to the schema (empty iff error).
    confidence: Dotted leaf path -> confidence labels, from platforms that emit
      confidence; None otherwise.
    usage: Platform-specific per-document usage (tokens, latencies).
  """

  doc_id: str
  error: str | None
  raw_output: str
  prediction: dict[str, Any]
  confidence: dict[str, list[str]] | None
  usage: dict[str, Any]

  def __post_init__(self):
    if self.error is not None and self.prediction:
      raise ValueError(f'Failed document {self.doc_id} must have no prediction')


@dataclasses.dataclass(frozen=True)
class AiExtractResult:
  """Result of extracting a set of documents.

  Attributes:
    wall_time_sec: Wall time of the extraction itself (excludes result fetch).
    document_results: One result per requested document.
    usage_metadata: Run-level platform usage (token totals / query IDs).
    extra_metrics: Additional platform-specific (name, value, unit) metrics.
  """

  wall_time_sec: float
  document_results: list[AiExtractDocumentResult]
  usage_metadata: dict[str, Any]
  extra_metrics: list[tuple[str, float, str]]


def CheckAiExtractCoverage(
    results: Sequence[AiExtractDocumentResult], doc_ids: Sequence[str]
) -> None:
  """Raise ValueError unless results cover exactly the requested documents."""
  got = [r.doc_id for r in results]
  if sorted(got) != sorted(doc_ids):
    raise ValueError(
        "AI extract results don't cover exactly the requested documents: "
        f'missing={sorted(set(doc_ids) - set(got))[:10]} '
        f'extra={sorted(set(got) - set(doc_ids))[:10]} '
        f'duplicates={len(got) - len(set(got))}'
    )


def _MakeDocumentResult(
    doc_id: str,
    raw_output: str,
    error: str | None,
    prediction: dict[str, Any],
) -> AiExtractDocumentResult:
  return AiExtractDocumentResult(
      doc_id=doc_id,
      error=error,
      raw_output=raw_output,
      prediction=prediction,
      confidence=None,
      usage={},
  )


def ParseAiExtractRows(
    query_results: dict[str, list[Any]],
    to_prediction: Callable[[dict[str, Any]], dict[str, Any]],
    error_key: str,
) -> list[AiExtractDocumentResult]:
  """Convert fetched (DOC_ID, EXTRACTION JSON) rows to document results.

  A row whose extraction is null, whose platform result reports an error or
  has no response, or whose response has an unexpected shape, becomes a failed
  document result.

  Args:
    query_results: Columnar query results with DOC_ID and EXTRACTION columns
      (or empty when the query returned zero rows).
    to_prediction: Native response object -> prediction (raises ValueError on
      unexpected shapes).
    error_key: Name of the platform's per-row error member.

  Returns:
    Document results in row order.
  """
  if not query_results:
    return []
  results = []
  for doc_id, extraction in zip(
      query_results['DOC_ID'], query_results['EXTRACTION'], strict=True
  ):
    if extraction is None:
      results.append(
          _MakeDocumentResult(doc_id, '', 'null extraction result', {})
      )
      continue
    result = json.loads(extraction)
    if result.get(error_key):
      results.append(
          _MakeDocumentResult(doc_id, extraction, str(result[error_key]), {})
      )
      continue
    if result.get('response') is None:
      results.append(
          _MakeDocumentResult(doc_id, extraction, 'null response', {})
      )
      continue
    try:
      prediction = to_prediction(result['response'])
    except ValueError as e:
      results.append(
          _MakeDocumentResult(
              doc_id, extraction, f'unexpected response shape: {e}', {}
          )
      )
      continue
    results.append(_MakeDocumentResult(doc_id, extraction, None, prediction))
  return results
