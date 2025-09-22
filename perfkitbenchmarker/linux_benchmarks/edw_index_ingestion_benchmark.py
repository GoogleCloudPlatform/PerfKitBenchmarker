# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Benchmark for evaluating search index performance during data ingestion.

Measure the performance of creating and using a search index on an
Enterprise Data Warehouse (EDW) system while data is being actively ingested.

This benchmark collects the following key metrics:
- Time to build the initial search index
- Latency of search queries after initial indexing, during ingestion, and after
final indexing completes
- Time to complete the index after all data is ingested

Overview of benchmark:
* Create/recreate a table
* Load n copies of the user-provided init dataset into table from cloud storage
* Create a text search index on all supported columns of the new table
* Wait for the service to report 100% completion on the index
* Run 5 text search queries against the provided
<edw_search_ingestion_search_keyword> search keyword
* Start three concurrent subprocesses:
  * One subprocesses continuously ingests new data into the table.
  * Another subprocesses continuously runs search queries against the table.
  * A third one continuously fetches index completion metrics.
* Continue benchmarking until the table reaches
<edw_search_ingestion_target_row_count> total rows
* Once the target row count is reached, stop data ingestion and querying and
wait for the index to reach 100% completion
* Run 5 final search queries against the table after the index completes
"""

from collections.abc import Iterable
import dataclasses
import enum
import logging
import multiprocessing
import random
import threading
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker.configs import benchmark_config_spec as pkb_benchmark_config_spec
from perfkitbenchmarker.providers.gcp import bigquery
from perfkitbenchmarker.providers.snowflake import snowflake


BENCHMARK_NAME = "edw_index_ingestion_benchmark"


class _Steps(enum.Enum):
  INITIAL_LOAD = "INITIAL_LOAD"
  INITIAL_INDEX_CREATION = "INITIAL_INDEX_CREATION"
  INITIAL_INDEX_WAIT = "INITIAL_INDEX_WAIT"
  INITIAL_SEARCH = "INITIAL_SEARCH"
  MAIN = "MAIN"
  FINAL_INDEX_WAIT = "FINAL_INDEX_WAIT"
  FINAL_SEARCH = "FINAL_SEARCH"


BENCHMARK_CONFIG = """
edw_index_ingestion_benchmark:
  description: Edw search index benchmark
  edw_service:
    type: bigquery
    cluster_identifier: _cluster_id_
  vm_groups:
    client:
      vm_spec: *default_dual_core
"""

INDEXING_TIMEOUT_SEC = 21600

_TARGET_ROW_COUNT = flags.DEFINE_integer(
    "edw_search_ingestion_target_row_count",
    1,
    "Target number of rows ingested for index ingestion benchmark",
)

_LOAD_INTERVAL_SEC = flags.DEFINE_integer(
    "edw_search_ingestion_load_interval_sec",
    60,
    "The time in seconds to wait between data insertion calls during live data"
    " ingestion. Does not apply to init dataset loading.",
)

_QUERY_INTERVAL_SEC = flags.DEFINE_integer(
    "edw_search_ingestion_query_interval_sec",
    29,
    "The time in seconds to wait between search queries during live data"
    " ingestion.",
)

_INIT_DATASET_COPIES = flags.DEFINE_integer(
    "edw_search_ingestion_init_dataset_copies",
    2,
    "The number of copies of the init dataset to insert into the starter table"
    " for edw search index benchmarks.",
)

_SNOWFLAKE_INGESTION_WAREHOUSE = flags.DEFINE_string(
    "snowflake_ingestion_warehouse",
    None,
    "Separate warehouse to send ingestion queries, per their recommendation:"
    " https://docs.snowflake.com/en/user-guide/data-load-considerations-plan#dedicating-separate-warehouses-to-load-and-query-operations."
    " If unset, will just use the same warehouse set with"
    " --snowflake_warehouse.",
)

_BENCHMARK_STEPS = flags.DEFINE_list(
    "edw_search_ingestion_steps",
    [s.value for s in _Steps],
    "Comma-separated list of benchmark steps to run. By default it will just "
    " run all steps. Here's the list of steps in order of execution and what"
    " they do:\n"
    "  INITIAL_LOAD: Load n copies of the user-provided init dataset into\n"
    "    table from cloud storage.\n"
    "  INITIAL_INDEX_CREATION: Create a text search index on relevant columns\n"
    "    of the new table. This step is mandatory.\n"
    "  INITIAL_INDEX_WAIT: Wait for the service to report 100% completion on\n"
    "    the index.\n"
    "  INITIAL_SEARCH: Run each defined search query 5 times against the\n"
    "    provided <edw_search_ingestion_search_keyword> search keyword.\n"
    "  MAIN: Start 3 concurrent subprocesses: One subprocesses continuously\n"
    "    ingests new data into the table. Another subprocess continuously\n"
    "    runs search queries against the table. A third one continuosly\n"
    "    fetches index completion metrics. This step is mandatory.\n"
    "  FINAL_INDEX_WAIT: Once the target row count is reached, stop data\n"
    "    ingestion and querying and wait for the index to reach 100%\n"
    "    completion.\n"
    "  FINAL_SEARCH: Run each defined search query 5 times against the table\n"
    "    after the index completes.",
)

_SEARCH_QUERIES = flags.DEFINE_list(
    "edw_search_ingestion_queries",
    [],
    "Comma separated list of search queries to run. Each query passed has to be"
    " in the format 'name:term', where name is a human-friendly name to be"
    " added to the exported samples' metadata and term is the actual search"
    " query that will be passed down to the corresponding EDW text search"
    " function. If more than one query is passed then we will have the"
    " following behavior: INITIAL_SEARCH and FINAL_SEARCH will submit 5 queries"
    " for each search query passed. The queries submitted on MAIN will be"
    " picked randomly (uniform distribution).",
)

FLAGS = flags.FLAGS


@dataclasses.dataclass
class _SearchQuery:
  name: str
  term: str


def _ParseSearchQueries(search_queries: list[str]) -> list[_SearchQuery]:
  """Parses a list of search queries from the format 'name:term'."""
  parsed_queries = []
  for query in search_queries:
    parts = query.split(":", 1)
    if len(parts) != 2:
      raise ValueError(
          f"Invalid search query format: {query}. Expected 'name:term'."
      )
    parsed_queries.append(_SearchQuery(name=parts[0], term=parts[1]))
  return parsed_queries


@flags.validator(
    _BENCHMARK_STEPS,
    "Invalid step(s) provided to --edw_search_ingestion_steps. Must be one or "
    "more of: "
    + ", ".join([s.value for s in _Steps])
    + " and must include INITIAL_INDEX_CREATION and MAIN.",
)
def _CheckEdwSearchIngestionSteps(steps: list[str]) -> bool:
  """Checks that all provided steps are valid."""
  return (
      all(step in [s.value for s in _Steps] for step in steps)
      and _Steps.INITIAL_INDEX_CREATION.value in steps
      and _Steps.MAIN.value in steps
  )


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the benchmark config.

  Args:
    user_config: A dictionary of the user's command line flags.

  Returns:
    The benchmark configuration.
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(
    benchmark_config_spec: pkb_benchmark_config_spec.BenchmarkConfigSpec,
) -> None:
  """Checks if the required flags are passed.

  Args:
    benchmark_config_spec: The bencshmark configuration.
  """
  edw_service_type: str = benchmark_config_spec.edw_service.type  # pytype: disable=attribute-error
  if _SNOWFLAKE_INGESTION_WAREHOUSE.value and not edw_service_type.startswith(
      "snowflake"
  ):
    raise errors.Config.InvalidValue(
        "--snowflake_ingestion_warehouse is only valid for Snowflake EDW"
        " services."
    )
  if not _SEARCH_QUERIES.value:
    raise errors.Config.InvalidValue(
        "edw_index_ingestion_benchmark requires --edw_search_ingestion_queries"
        " flag to be set."
    )
  try:
    _ParseSearchQueries(_SEARCH_QUERIES.value)
  except ValueError:
    raise errors.Config.InvalidValue(
        "--edw_search_ingestion_queries does not follow the required format:"
        " 'label1:term1,...,labeln:termn'."
    ) from None


def Prepare(spec: benchmark_spec.BenchmarkSpec):
  """Install script execution environment on the client vm.

  Args:
    spec: The benchmark specification. Contains all data that is required to run
      the benchmark.
  """
  spec.always_call_cleanup = True
  edw_service_instance: edw_service.EdwService = spec.edw_service

  edw_service_instance.GetClientInterface().SetProvisionedAttributes(spec)
  edw_service_instance.GetClientInterface().Prepare("edw_common")


# MARK: Helpers
def _ExecuteDataLoad(
    service: edw_service.EdwService,
    table_name: str,
    data_path: str,
    target_row_count: int,
    interval: float,
    ingestion_finished: threading.Event,
    ingestion_warehouse: str | None,
    bench_meta: dict[str, Any] | None = None,
) -> list[sample.Sample]:
  """Executes data insertion queries in a loop.

  Intended to be run in a child process. Continuously
  loads data from `data_path` into `table_name` at a given `interval` by
  spawning new processes for each load operation. It stops when the table
  reaches `target_row_count`.

  Args:
    service: The EdwService instance to use for executing queries.
    table_name: The name of the table to insert data into.
    data_path: The path to the data to be loaded.
    target_row_count: The target number of rows to ingest.
    interval: The time in seconds to wait between data insertion calls.
    ingestion_finished: A threading.Event-like object to signal when the loading
      process is finished.
    ingestion_warehouse: The name of the warehouse to use for ingestion queries
      (Snowflake only).
    bench_meta: Metadata to add to the collected samples.

  Returns:
    A list of sample.Sample objects representing the execution time of each
    load query.
  """
  try:
    logging.info("Starting data loading loop")
    cur_process = multiprocessing.current_process()
    assert cur_process.name != "MainProcess", (
        "Expected to run this function on its own subprocess, since it may try"
        " to re-configure the EDW service instance (to change Snowflake's"
        " warehouse), which would otherwise affect other functions of the"
        " benchmark undesirably."
    )
    if ingestion_warehouse:
      assert isinstance(service, snowflake.Snowflake)
      service.SetWarehouse(ingestion_warehouse)
    if bench_meta is None:
      bench_meta = {}
    samples = []
    i = 0
    current_rows, _ = service.GetTableRowCount(table_name)
    while current_rows < target_row_count:
      ingestion_start_time = time.monotonic()
      execution_time, metadata = service.InsertSearchData(table_name, data_path)
      reported_rows, _ = service.GetTableRowCount(table_name)
      current_rows = reported_rows
      metadata["current_rows"] = reported_rows
      metadata["load_iter"] = i
      samples.append(
          sample.Sample(
              "load_query_execution_time",
              execution_time,
              "seconds",
              metadata | bench_meta,
          )
      )
      ingestion_end_time = time.monotonic()
      elapsed = ingestion_end_time - ingestion_start_time
      sleep_time = interval - elapsed
      if sleep_time > 0:
        time.sleep(sleep_time)
    logging.info("Data loading complete.")
    return samples
  finally:
    ingestion_finished.set()


@dataclasses.dataclass
class _IndexSearchQuerySubmitter:
  """Helper class to submit a sequence of index search queries.

  Attributes:
    edw_service_instance: The EdwService instance to use for executing queries.
    table_name: The name of the table to query.
    index_name: The name of the search index to use.
  """

  edw_service_instance: edw_service.EdwService
  table_name: str
  index_name: str

  def ExecuteSearchQueryNTimes(
      self,
      search_query: _SearchQuery,
      n: int,
      cooldown_sec: float = 0,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries n times.

    Args:
      search_query: The search query to execute.
      n: The number of times to execute the query.
      cooldown_sec: The time in seconds to wait between queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """

    def _Generator():
      for _ in range(n):
        yield search_query
        time.sleep(cooldown_sec)

    return self._ExecuteIndexSearchQueriesFor(_Generator(), bench_meta)

  def ExecuteSearchQueryUntilEvent(
      self,
      search_queries: list[_SearchQuery],
      event: threading.Event,
      cooldown_sec: float,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries in a loop until an event is set.

    Args:
      search_queries: A list of _SearchQuery objects to randomly choose from.
      event: A threading.Event-like object to signal when to stop executing
        queries.
      cooldown_sec: The time in seconds to wait between queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """

    def _Generator():
      while not event.is_set():
        search_query = random.choice(search_queries)
        yield search_query
        time.sleep(cooldown_sec)

    return self._ExecuteIndexSearchQueriesFor(_Generator(), bench_meta)

  def _ExecuteIndexSearchQueriesFor(
      self,
      iterable: Iterable[_SearchQuery],
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries for each item in the iterable.

    Args:
      iterable: An iterable to control the number of queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """
    logging.info("Starting search queries.")
    samples = []
    if bench_meta is None:
      bench_meta = {}
    i = 0
    for search_query in iterable:
      query_meta = {
          "search_query_iter": i,
          "search_query_name": search_query.name,
      }
      samples.append(
          self._ExecuteIndexSearchQuery(
              search_query.term, bench_meta | query_meta
          )
      )
      i += 1
    return samples

  def _ExecuteIndexSearchQuery(
      self,
      query_text: str,
      bench_meta: dict[Any, Any],
  ) -> sample.Sample:
    """Executes a single text search query and returns the sample.

    Args:
      query_text: The text of the search query to execute.
      bench_meta: Metadata to add to the collected sample.

    Returns:
      A sample.Sample object representing the execution time of the query.
    """
    execution_time, metadata = self.edw_service_instance.TextSearchQuery(
        self.table_name, query_text, self.index_name
    )
    return sample.Sample(
        "query_execution_time",
        execution_time,
        "seconds",
        metadata | bench_meta,
    )


def _FetchQueryPercentageUntilEvent(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    ingestion_finished: threading.Event,
    bench_meta: dict[Any, Any] | None = None,
) -> list[sample.Sample]:
  # TODO(odiego): Review this fn
  """Fetches index completion percentage until ingestion finishes.

  Polls the index status and records a sample the first time each coverage
  percentage is seen. Stops when the event is set.

  Args:
    service: The EdwService instance to use for checking index status.
    table_name: The name of the table containing the index.
    index_name: The name of the index to monitor.
    ingestion_finished: A threading.Event-like object to signal when to stop.
    bench_meta: Metadata to add to the collected samples.

  Returns:
    A list of sample.Sample objects representing the time to reach each
    index coverage percentage.
  """
  if bench_meta is None:
    bench_meta = {}
  samples: list[sample.Sample] = []
  bench_meta = bench_meta.copy()
  while not ingestion_finished.is_set():
    percentage, query_meta = service.GetSearchIndexCompletionPercentage(
        table_name, index_name
    )
    samples.append(
        sample.Sample(
            "current_index_percentage",
            percentage,
            "percent",
            bench_meta | query_meta,
        )
    )
    time.sleep(30)
  return samples


def _WaitForIndexCompletion(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    start_time: float,
    timeout: float,
    bench_meta: dict[Any, Any] | None = None,
) -> list[sample.Sample]:
  """Waits for a search index to reach 100% coverage.

  Poll the index status and record a sample the first time each coverage
  percentage is seen. Stops when the index is fully built or when the
  timeout is reached.

  Args:
    service: The EdwService instance to use for checking index status.
    table_name: The name of the table containing the index.
    index_name: The name of the index to monitor.
    start_time: The time at which index creation was initiated.
    timeout: The maximum time in seconds to wait for completion.
    bench_meta: Metadata to add to the collected samples.

  Returns:
    A list of sample.Sample objects representing the time to reach each
    index coverage percentage.
  """
  if bench_meta is None:
    bench_meta = {}
  wait_results: dict[int, sample.Sample] = {}
  bench_meta = bench_meta.copy()
  bench_meta["index_completion_timeout"] = timeout
  while True:
    (percentage, query_meta) = service.GetSearchIndexCompletionPercentage(
        table_name, index_name
    )
    if percentage not in wait_results:
      sample_metadata = query_meta | bench_meta
      sample_metadata["index_percentage"] = percentage
      wait_results[percentage] = sample.Sample(
          "time_to_index_percentage",
          time.time() - start_time,
          "seconds",
          sample_metadata,
      )
    if 100 in wait_results:
      wait_results[-1] = sample.Sample(
          "index_build_completed_before_timeout", True, "boolean", bench_meta
      )
      break
    elif time.time() - start_time > timeout:
      wait_results[-1] = sample.Sample(
          "index_build_completed_before_timeout", False, "boolean", bench_meta
      )
      break
    time.sleep(10)
  return list(wait_results.values())


# MARK: Run
def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the Edw Index Ingestion benchmark.

  Measure the performance of text search queries against an indexed dataset with
  concurrent data ingestion. See this module's docstring for a benchmark
  overview.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  edw_service_instance: edw_service.EdwService = spec.edw_service
  samples: list[sample.Sample] = []
  search_queries = _ParseSearchQueries(_SEARCH_QUERIES.value)

  gen_metadata = {
      "edw_index_search_table": edw_service.EDW_SEARCH_TABLE_NAME.value,
      "edw_index_search_index": edw_service.EDW_SEARCH_INDEX_NAME.value,
      "edw_index_init_data_location": (
          edw_service.EDW_SEARCH_INIT_DATA_LOCATION.value
      ),
      "edw_index_interactive_data_location": (
          edw_service.EDW_SEARCH_DATA_LOCATION.value
      ),
      "edw_index_target_row_count": _TARGET_ROW_COUNT.value,
      "edw_index_init_dataset_copies": _INIT_DATASET_COPIES.value,
      "edw_index_ingestion_load_interval_sec": _LOAD_INTERVAL_SEC.value,
      "edw_index_ingestion_query_interval_sec": _QUERY_INTERVAL_SEC.value,
      "edw_index_steps": ",".join(_BENCHMARK_STEPS.value),
  }
  if isinstance(edw_service_instance, bigquery.Bigquery):
    gen_metadata["edw_index_table_partitioned"] = (
        bigquery.INITIALIZE_SEARCH_TABLE_PARTITIONED.value
    )

  if _Steps.INITIAL_LOAD.value in _BENCHMARK_STEPS.value:
    logging.info("Loading initial search data")
    edw_service_instance.DropSearchIndex(
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
    )
    edw_service_instance.InitializeSearchStarterTable(
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INIT_DATA_LOCATION.value,
    )
    logging.info("Inserting initial search data")
    for _ in range(_INIT_DATASET_COPIES.value):
      edw_service_instance.InsertSearchData(
          edw_service.EDW_SEARCH_TABLE_NAME.value,
          edw_service.EDW_SEARCH_INIT_DATA_LOCATION.value,
      )
    logging.info("Initial search data load complete")

  indexing_start_time = time.time()
  if _Steps.INITIAL_INDEX_CREATION.value in _BENCHMARK_STEPS.value:
    logging.info("Creating index")
    edw_service_instance.CreateSearchIndex(
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
    )

  if _Steps.INITIAL_INDEX_WAIT.value in _BENCHMARK_STEPS.value:
    current_step_meta = {
        "edw_index_current_step": _Steps.INITIAL_INDEX_WAIT.value
    }
    logging.info("Waiting for index to reach 100% coverage on init data")
    samples += _WaitForIndexCompletion(
        edw_service_instance,
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
        indexing_start_time,
        INDEXING_TIMEOUT_SEC,
        bench_meta=gen_metadata | current_step_meta,
    )
  logging.info("Initial dataset indexing stage complete")

  query_submitter = _IndexSearchQuerySubmitter(
      edw_service_instance,
      edw_service.EDW_SEARCH_TABLE_NAME.value,
      edw_service.EDW_SEARCH_INDEX_NAME.value,
  )

  if _Steps.INITIAL_SEARCH.value in _BENCHMARK_STEPS.value:
    current_step_meta = {"edw_index_current_step": _Steps.INITIAL_SEARCH.value}
    logging.info("Running preload search queries")
    for search_query in search_queries:
      samples += query_submitter.ExecuteSearchQueryNTimes(
          search_query,
          5,
          bench_meta=gen_metadata | current_step_meta,
      )

  if _Steps.MAIN.value in _BENCHMARK_STEPS.value:
    current_step_meta = {"edw_index_current_step": _Steps.MAIN.value}
    logging.info("Dispatching concurrent ingestion and search queries.")
    with multiprocessing.Manager() as manager:
      ingestion_finished = manager.Event()
      tasks = [
          (
              _ExecuteDataLoad,
              (
                  edw_service_instance,
                  edw_service.EDW_SEARCH_TABLE_NAME.value,
                  edw_service.EDW_SEARCH_DATA_LOCATION.value,
                  _TARGET_ROW_COUNT.value,
                  _LOAD_INTERVAL_SEC.value,
                  ingestion_finished,
                  _SNOWFLAKE_INGESTION_WAREHOUSE.value,
              ),
              {"bench_meta": gen_metadata | current_step_meta},
          ),
          (
              query_submitter.ExecuteSearchQueryUntilEvent,
              (search_queries, ingestion_finished, _QUERY_INTERVAL_SEC.value),
              {"bench_meta": gen_metadata | current_step_meta},
          ),
          (
              _FetchQueryPercentageUntilEvent,
              (
                  edw_service_instance,
                  edw_service.EDW_SEARCH_TABLE_NAME.value,
                  edw_service.EDW_SEARCH_INDEX_NAME.value,
                  ingestion_finished,
              ),
              {"bench_meta": gen_metadata | current_step_meta},
          ),
      ]
      (
          ingestion_samples,
          search_query_samples,
          index_percentage_samples,
      ) = background_tasks.RunParallelProcesses(
          tasks, background_tasks.MAX_CONCURRENT_THREADS
      )
    samples += ingestion_samples
    samples += search_query_samples
    samples += index_percentage_samples

  if _Steps.FINAL_INDEX_WAIT.value in _BENCHMARK_STEPS.value:
    current_step_meta = {
        "edw_index_current_step": _Steps.FINAL_INDEX_WAIT.value
    }
    logging.info("Waiting for index to reach 100% coverage on full dataset")
    after_load_index_start = time.time()
    samples += _WaitForIndexCompletion(
        edw_service_instance,
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
        after_load_index_start,
        INDEXING_TIMEOUT_SEC,
        bench_meta=gen_metadata | current_step_meta,
    )

  if _Steps.FINAL_SEARCH.value in _BENCHMARK_STEPS.value:
    current_step_meta = {"edw_index_current_step": _Steps.FINAL_SEARCH.value}
    logging.info("Running post load queries")
    for search_query in search_queries:
      samples += query_submitter.ExecuteSearchQueryNTimes(
          search_query,
          5,
          bench_meta=gen_metadata | current_step_meta,
      )

  return sorted(samples, key=lambda s: s.timestamp)


def Cleanup(spec: benchmark_spec.BenchmarkSpec):
  """Cleans up the benchmark resources.

  Args:
    spec: The benchmark specification.
  """
  spec.edw_service.Cleanup()
