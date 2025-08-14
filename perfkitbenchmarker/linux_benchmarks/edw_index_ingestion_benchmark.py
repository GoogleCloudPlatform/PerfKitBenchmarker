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
* Start two child processes, run simultaneously:
  * Every <edw_search_ingestion_load_interval_sec> seconds, dispatch a load
  query to load one additional copy of the user-provided live ingestion dataset
  into the table
  * Every <edw_search_ingestion_query_interval_sec> seconds, dispatch a test
  search query to evaluate search performance and index completion.
* Continue benchmarking until the table reaches
<edw_search_ingestion_target_row_count> total rows
* Once the target row count is reached, stop data ingestion and querying and
wait for the index to reach 100% completion
* Run 5 final search queries against the table after the index completes
"""


import logging
import multiprocessing
# Used as type annotation
# pylint: disable-next=unused-import
from multiprocessing import managers
# Need to import Event as a renamed member for type annotations
# see https://stackoverflow.com/questions/75630114/
# pylint: disable-next=g-importing-member
from multiprocessing.synchronize import Event as SyncEvent
import time
from typing import Any, Dict, TypeAlias

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import sample


BENCHMARK_NAME = "edw_index_ingestion_benchmark"

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

_TResultList: TypeAlias = "managers.ListProxy[sample.Sample]"

_TARGET_ROW_COUNT = flags.DEFINE_integer(
    "edw_search_ingestion_target_row_count",
    1,
    "Target number of rows ingested for index ingestion benchmark",
)

_SEARCH_KEYWORD = flags.DEFINE_string(
    "edw_search_ingestion_search_keyword",
    "ASIA",
    "Keyword to search for in index ingestion benchmark",
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

FLAGS = flags.FLAGS


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the benchmark config.

  Args:
    user_config: A dictionary of the user's command line flags.

  Returns:
    The benchmark configuration.
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


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
    results: _TResultList,
    synchro: SyncEvent,
    target_row_count: int,
    interval: float,
):
  """Executes data insertion queries in a loop.

  Intended to be run in a child process. Continuously
  loads data from `data_path` into `table_name` at a given `interval` by
  spawning new processes for each load operation. It stops when the table
  reaches `target_row_count` or when the `synchro` event is set.

  Args:
    service: The EdwService instance to use for executing queries.
    table_name: The name of the table to insert data into.
    data_path: The path to the data to be loaded.
    results: A multiprocessing manager list to append samples to.
    synchro: A multiprocessing.Event to signal when to stop the loading process.
    target_row_count: The target number of rows to ingest.
    interval: The time in seconds to wait between data insertion calls.
  """
  logging.info("Starting data loading loop")
  i = 0
  load_processes: list[multiprocessing.Process] = []
  current_rows: multiprocessing.Value = multiprocessing.Value("i", 0)
  current_rows.value, _ = service.GetTableRowCount(table_name)
  while current_rows.value < target_row_count and not synchro.is_set():
    next_run = multiprocessing.Process(
        target=_ExecuteDataLoadQuery,
        args=(
            service,
            table_name,
            data_path,
            results,
            current_rows,
            target_row_count,
            i,
        ),
    )
    load_processes.append(next_run)
    next_run.start()
    i += 1
    time.sleep(interval)
  for process in load_processes:
    process.join()
  logging.info("Data loading complete.")
  synchro.set()


def _ExecuteDataLoadQuery(
    service: edw_service.EdwService,
    table_name: str,
    data_path: str,
    results: _TResultList,
    current_rows: multiprocessing.Value,
    target_row_count: int,
    load_iter: int,
):
  """Executes a single data load query and records the result.

  Helper function for `_ExecuteDataLoad`, run in a child process. Inserts data
  from `data_path` into `table_name`, updates the shared `current_rows`
  value, and records the execution time as a `sample.Sample`.

  Args:
    service: The EdwService instance to use for executing the query.
    table_name: The name of the table to insert data into.
    data_path: The path to the data to be loaded.
    results: A multiprocessing manager list to append the resulting sample to.
    current_rows: A shared multiprocessing.Value holding the current row count.
    target_row_count: The target number of rows to ingest.
    load_iter: The iteration number of the load operation.
  """
  if current_rows.value >= target_row_count:
    return
  execution_time, metadata = service.InsertSearchData(table_name, data_path)
  reported_rows, _ = service.GetTableRowCount(table_name)
  current_rows.value = reported_rows
  metadata["current_rows"] = reported_rows
  metadata["load_iter"] = load_iter
  results.append(
      sample.Sample(
          "load_query_execution_time", execution_time, "seconds", metadata
      )
  )


def _ExecuteIndexQueries(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    search_query: str,
    results: _TResultList,
    num_queries: int = 1,
    interval: float = 0,
    bench_meta: Dict[str, Any] | None = None,
):
  """Execute a set number of index search queries.

  Args:
    service: The EdwService instance to use for executing queries.
    table_name: The name of the table to query.
    index_name: The name of the search index to use.
    search_query: The text of the search query to execute.
    results: A list-like object to append samples to.
    num_queries: The number of times to execute the query.
    interval: The time in seconds to wait between queries.
    bench_meta: Optional metadata to add to the collected samples.
  """
  logging.info("Starting search queries.")
  if bench_meta is None:
    bench_meta = {}
  for i in range(num_queries):
    query_meta = {"search_query_iter": i}
    _ExecuteIndexQuery(
        service,
        table_name,
        index_name,
        search_query,
        results,
        bench_meta | query_meta,
    )
    time.sleep(interval)


# TODO(jguertin): * Take list of search query keywords?
def _ExecuteIndexQueriesSynchro(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    search_query: str,
    results: _TResultList,
    synchro: SyncEvent,
    interval: float = 0,
    bench_meta: Dict[str, Any] | None = None,
):
  """Executes index search queries in a loop until signaled to stop.

  Intended to be run in a separate process. Continuously execute a search query
  at a given `interval` by spawning new processes. Stops when the `synchro`
  event is set.

  Args:
    service: The EdwService instance to use for executing queries.
    table_name: The name of the table to query.
    index_name: The name of the search index to use.
    search_query: The text of the search query to execute.
    results: A multiprocessing manager list to append samples to.
    synchro: A multiprocessing.Event that signals the process to stop.
    interval: The time in seconds to wait between queries.
    bench_meta: Optional metadata to add to the collected samples.
  """
  logging.info("Starting search queries.")
  if bench_meta is None:
    bench_meta = {}
  i = 0
  query_processes: list[multiprocessing.Process] = []
  while not synchro.is_set():
    query_meta = {"search_query_iter": i}
    query_process = multiprocessing.Process(
        target=_ExecuteIndexQuery,
        args=(
            service,
            table_name,
            index_name,
            search_query,
            results,
            bench_meta | query_meta,
        ),
    )
    query_processes.append(query_process)
    query_process.start()
    i += 1
    time.sleep(interval)

  for process in query_processes:
    process.join()
  logging.info("Search queries completed.")


def _ExecuteIndexQuery(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    query_text: str,
    results: _TResultList,
    bench_meta: dict[Any, Any],
) -> None:
  """Executes a single text search query and records the result.

  Args:
    service: The EdwService instance to use for executing the query.
    table_name: The name of the table to query.
    index_name: The name of the search index to use.
    query_text: The text of the search query to execute.
    results: A list-like object to append the resulting sample to.
    bench_meta: Metadata to add to the collected sample.
  """
  execution_time, metadata = service.TextSearchQuery(
      table_name, query_text, index_name
  )
  results.append(
      sample.Sample(
          "query_execution_time",
          execution_time,
          "seconds",
          metadata | bench_meta,
      )
  )


def _WaitForIndexCompletion(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    results_proxy: _TResultList,
    start_time: float,
    timeout: float,
    stage_label: str,
    bench_meta: dict[Any, Any],
) -> None:
  """Waits for a search index to reach 100% coverage.

  Poll the index status and record a sample the first time each coverage
  percentage is seen. Stops when the index is fully built or when the
  timeout is reached.

  Args:
    service: The EdwService instance to use for checking index status.
    table_name: The name of the table containing the index.
    index_name: The name of the index to monitor.
    results_proxy: A multiprocessing manager list to append samples to.
    start_time: The time at which index creation was initiated.
    timeout: The maximum time in seconds to wait for completion.
    stage_label: Label for the current benchmark stage (e.g. 'initialization').
    bench_meta: Metadata to add to the collected samples.
  """
  wait_results: dict[int, sample.Sample] = {}
  bench_meta = bench_meta.copy()
  bench_meta["index_completion_timeout"] = timeout
  bench_meta["benchmark_stage"] = stage_label
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
  results_proxy.extend(wait_results.values())


# MARK: Run
def Run(spec: benchmark_spec.BenchmarkSpec):
  """Runs the Edw Index Ingestion benchmark.

  Measure the performance of text search queries against an
  indexed dataset with concurrent data ingestion. Overview of the benchmark:

  1. Load an initial dataset into a table.
  2. Create a search index on the table and waits for it to build.
  3. Run a set of queries against the initial data.
  4. Start two concurrent processes:
     - One process continuously ingests new data into the table.
     - Another process continuously runs search queries against the table.
  5. Wait for both processes to complete.
  6. Wait for the index to cover the newly ingested data.
  7. Run a final set of queries.
  8. Collect and returns performance metrics from all stages.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """

  results: list[sample.Sample]

  search_table: str = edw_service.EDW_SEARCH_TABLE_NAME.value
  index_name: str = edw_service.EDW_SEARCH_INDEX_NAME.value
  init_data_loc: str = edw_service.EDW_SEARCH_INIT_DATA_LOCATION.value
  interactive_data_loc: str = edw_service.EDW_SEARCH_DATA_LOCATION.value
  row_target: int = _TARGET_ROW_COUNT.value
  init_copies: int = _INIT_DATASET_COPIES.value
  search_kw: str = _SEARCH_KEYWORD.value
  ingestion_interval: int = _LOAD_INTERVAL_SEC.value
  search_interval: int = _QUERY_INTERVAL_SEC.value

  with multiprocessing.Manager() as manager:
    results_proxy: _TResultList = manager.list()
    edw_service_instance: edw_service.EdwService = spec.edw_service
    logging.info("Loading initial search data")
    edw_service_instance.DropSearchIndex(search_table, index_name)
    edw_service_instance.InitializeSearchStarterTable(
        search_table, init_data_loc
    )

    logging.info("Inserting initial search data")
    for _ in range(init_copies):
      edw_service_instance.InsertSearchData(search_table, init_data_loc)
    logging.info("Initial search data load complete")

    logging.info("Creating index")
    indexing_start_time = time.time()
    edw_service_instance.CreateSearchIndex(search_table, index_name)
    logging.info("Waiting for index to reach 100% coverage on init data")
    _WaitForIndexCompletion(
        edw_service_instance,
        search_table,
        index_name,
        results_proxy,
        indexing_start_time,
        INDEXING_TIMEOUT_SEC,
        "initialization",
        {},
    )
    logging.info("Initial dataset indexing stage complete")

    logging.info("Running preload search queries")
    _ExecuteIndexQueries(
        edw_service_instance,
        search_table,
        index_name,
        search_kw,
        results_proxy,
        num_queries=5,
        bench_meta={"search_query_block": "preload"},
    )

    run_complete = multiprocessing.Event()
    data_load_process = multiprocessing.Process(
        target=_ExecuteDataLoad,
        args=(
            edw_service_instance,  # edw_service: edw_service.EdwService,
            search_table,  # table_name: str,
            interactive_data_loc,  # data_path: str,
            results_proxy,  # results: ListProxy[sample.Sample],
            run_complete,  # synchro: SyncEvent,
            row_target,  # target_row_count: int,
            ingestion_interval,  # interval: int,
        ),
    )

    search_query_process = multiprocessing.Process(
        target=_ExecuteIndexQueriesSynchro,
        args=(
            edw_service_instance,  # service: edw_service.EdwService,
            search_table,  # table_name: str,
            index_name,  # index_name: str,
            search_kw,  # search_query: str,
            results_proxy,  # results: _TResultList,
            run_complete,  # synchro: SyncEvent,
        ),
        kwargs={
            "interval": search_interval,
            "bench_meta": {"search_query_block": "ingestion"},
        },
    )

    logging.info("Starting data load")
    data_load_process.start()
    logging.info("Starting synchronous query dispatch")
    search_query_process.start()

    while data_load_process.is_alive() and search_query_process.is_alive():
      time.sleep(1)

    # Send finish signal if a process terminates without doing so
    if not run_complete.is_set():
      logging.warning("Subprocess terminated prematurely")
      run_complete.set()

    data_load_process.join()
    search_query_process.join()

    logging.info("Waiting for index to reach 100% coverage on full dataset")
    after_load_index_start = time.time()
    _WaitForIndexCompletion(
        edw_service_instance,
        search_table,
        index_name,
        results_proxy,
        after_load_index_start,
        INDEXING_TIMEOUT_SEC,
        "post_load",
        {},
    )

    logging.info("Running post load queries")
    _ExecuteIndexQueries(
        edw_service_instance,
        search_table,
        index_name,
        search_kw,
        results_proxy,
        num_queries=5,
        bench_meta={"search_query_block": "postload"},
    )

    results = list(results_proxy)

  return results


def Cleanup(spec: benchmark_spec.BenchmarkSpec):
  """Cleans up the benchmark resources.

  Args:
    spec: The benchmark specification.
  """
  spec.edw_service.Cleanup()
