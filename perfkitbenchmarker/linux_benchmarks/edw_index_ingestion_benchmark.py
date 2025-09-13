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
* Start two concurrent subprocesses:
  * One subprocesses continuously ingests new data into the table.
  * Another subprocesses continuously runs search queries against the table.
* Continue benchmarking until the table reaches
<edw_search_ingestion_target_row_count> total rows
* Once the target row count is reached, stop data ingestion and querying and
wait for the index to reach 100% completion
* Run 5 final search queries against the table after the index completes
"""

from collections.abc import Iterable
import dataclasses
import logging
import multiprocessing
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
from perfkitbenchmarker.providers.snowflake import snowflake


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

_INDEX_WAIT = flags.DEFINE_boolean(
    "edw_search_ingestion_index_wait",
    True,
    "Whether or not to wait for indexing to complete. Set to false for fast "
    "debug runs.",
)

_SNOWFLAKE_INGESTION_WAREHOUSE = flags.DEFINE_string(
    "snowflake_ingestion_warehouse",
    None,
    "Separate warehouse to send ingestion queries, per their recommendation:"
    " https://docs.snowflake.com/en/user-guide/data-load-considerations-plan#dedicating-separate-warehouses-to-load-and-query-operations."
    " If unset, will just use the same warehouse set with"
    " --snowflake_warehouse.",
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


def CheckPrerequisites(
    benchmark_config_spec: pkb_benchmark_config_spec.BenchmarkConfigSpec,
) -> None:
  edw_service_type: str = benchmark_config_spec.edw_service.type  # pytype: disable=attribute-error
  if _SNOWFLAKE_INGESTION_WAREHOUSE.value and not edw_service_type.startswith(
      "snowflake"
  ):
    raise errors.Config.InvalidValue(
        "--snowflake_ingestion_warehouse is only valid for Snowflake EDW"
        " services."
    )


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
              metadata,
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
    query_text: The text of the search query to execute.
  """

  edw_service_instance: edw_service.EdwService
  table_name: str
  index_name: str
  query_text: str

  def ExecuteSearchQueryNTimes(
      self,
      n: int,
      cooldown_sec: float = 0,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries n times.

    Args:
      n: The number of times to execute the query.
      cooldown_sec: The time in seconds to wait between queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """

    def _Generator():
      for _ in range(n):
        yield
        time.sleep(cooldown_sec)

    return self._ExecuteIndexSearchQueriesFor(_Generator(), bench_meta)

  def ExecuteSearchQueryUntilEvent(
      self,
      event: threading.Event,
      cooldown_sec: float,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries in a loop until an event is set.

    Args:
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
        yield
        time.sleep(cooldown_sec)

    return self._ExecuteIndexSearchQueriesFor(_Generator(), bench_meta)

  def _ExecuteIndexSearchQueriesFor(
      self, iterable: Iterable[Any], bench_meta: dict[str, Any] | None = None
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
    for _ in iterable:
      query_meta = {"search_query_iter": i}
      samples.append(self._ExecuteIndexSearchQuery(bench_meta | query_meta))
      i += 1
    return samples

  def _ExecuteIndexSearchQuery(
      self,
      bench_meta: dict[Any, Any],
  ) -> sample.Sample:
    """Executes a single text search query and returns the sample.

    Args:
      bench_meta: Metadata to add to the collected sample.

    Returns:
      A sample.Sample object representing the execution time of the query.
    """
    execution_time, metadata = self.edw_service_instance.TextSearchQuery(
        self.table_name, self.query_text, self.index_name
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
  bench_meta["benchmark_stage"] = "ingestion"
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
    stage_label: str,
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
    stage_label: Label for the current benchmark stage (e.g. 'initialization').
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
  return list(wait_results.values())


# MARK: Run
def Run(spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs the Edw Index Ingestion benchmark.

  Measure the performance of text search queries against an
  indexed dataset with concurrent data ingestion. Overview of the benchmark:

  1. Load an initial dataset into a table.
  2. Create a search index on the table and waits for it to build.
  3. Run a set of queries against the initial data.
  4. Start two concurrent subprocesses:
     - One subprocesses continuously ingests new data into the table.
     - Another subprocesses continuously runs search queries against the table.
  5. Wait for both subprocesses to complete.
  6. Wait for the index to cover the newly ingested data.
  7. Run a final set of queries.
  8. Collect and returns performance metrics from all stages.

  Args:
    spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
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
      "edw_index_search_keyword": _SEARCH_KEYWORD.value,
      "edw_index_ingestion_load_interval_sec": _LOAD_INTERVAL_SEC.value,
      "edw_index_ingestion_query_interval_sec": _QUERY_INTERVAL_SEC.value,
      "edw_index_ingestion_index_wait": _INDEX_WAIT.value,
  }

  edw_service_instance: edw_service.EdwService = spec.edw_service
  samples: list[sample.Sample] = []

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

  logging.info("Creating index")
  indexing_start_time = time.time()
  edw_service_instance.CreateSearchIndex(
      edw_service.EDW_SEARCH_TABLE_NAME.value,
      edw_service.EDW_SEARCH_INDEX_NAME.value,
  )
  if _INDEX_WAIT.value:
    logging.info("Waiting for index to reach 100% coverage on init data")
    samples += _WaitForIndexCompletion(
        edw_service_instance,
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
        indexing_start_time,
        INDEXING_TIMEOUT_SEC,
        "initialization",
        bench_meta=gen_metadata,
    )
  logging.info("Initial dataset indexing stage complete")

  logging.info("Running preload search queries")
  query_submitter = _IndexSearchQuerySubmitter(
      edw_service_instance,
      edw_service.EDW_SEARCH_TABLE_NAME.value,
      edw_service.EDW_SEARCH_INDEX_NAME.value,
      _SEARCH_KEYWORD.value,
  )
  samples += query_submitter.ExecuteSearchQueryNTimes(
      5, bench_meta={"search_query_block": "preload"} | gen_metadata
  )

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
            {},
        ),
        (
            query_submitter.ExecuteSearchQueryUntilEvent,
            (ingestion_finished, _QUERY_INTERVAL_SEC.value),
            {"bench_meta": {"search_query_block": "ingestion"} | gen_metadata},
        ),
        (
            _FetchQueryPercentageUntilEvent,
            (
                edw_service_instance,
                edw_service.EDW_SEARCH_TABLE_NAME.value,
                edw_service.EDW_SEARCH_INDEX_NAME.value,
                ingestion_finished,
            ),
            {"bench_meta": gen_metadata},
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

  if _INDEX_WAIT.value:
    logging.info("Waiting for index to reach 100% coverage on full dataset")
    after_load_index_start = time.time()
    samples += _WaitForIndexCompletion(
        edw_service_instance,
        edw_service.EDW_SEARCH_TABLE_NAME.value,
        edw_service.EDW_SEARCH_INDEX_NAME.value,
        after_load_index_start,
        INDEXING_TIMEOUT_SEC,
        "post_load",
        bench_meta=gen_metadata,
    )

  logging.info("Running post load queries")
  samples += query_submitter.ExecuteSearchQueryNTimes(
      5, bench_meta={"search_query_block": "postload"} | gen_metadata
  )

  return sorted(samples, key=lambda s: s.timestamp)


def Cleanup(spec: benchmark_spec.BenchmarkSpec):
  """Cleans up the benchmark resources.

  Args:
    spec: The benchmark specification.
  """
  spec.edw_service.Cleanup()
