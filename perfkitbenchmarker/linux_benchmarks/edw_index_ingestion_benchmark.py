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
* Initial Load:
  * Create/recreate a table
  * Load <edw_search_ingestion_init_dataset_copies> copies of the user-provided
    init dataset into table from cloud storage.
  * Create a text search index on all supported columns of the new table
* Wait for the service to report 100% completion on the index
* Run <edw_search_ingestion_initial_search_count> text search queries for each
  one of the provided <edw_search_ingestion_queries>.
* Main step: Start three concurrent subprocesses:
  * One subprocesses continuously ingests new data into the table.
  * Another subprocesses continuously runs search queries against the table.
  * A third one continuously fetches index completion metrics.
* Main step finishes when <edw_search_ingestion_dataset_copies_to_ingest>
  copies of the dataset are ingested.
* Once ingestion is done wait for the index to reach 100% completion
* Run <edw_search_ingestion_final_search_count> final search queries for each
  one of the provided <edw_search_ingestion_queries> after the index completes.
"""

from collections.abc import Iterable
import dataclasses
import datetime
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

_LOAD_INTERVAL_SEC = flags.DEFINE_integer(
    "edw_search_ingestion_load_interval_sec",
    60,
    "The time in seconds to wait between data insertion calls during live data"
    " ingestion. Does not apply to init dataset loading.",
)

_QUERY_INTERVAL_SEC = flags.DEFINE_integer(
    "edw_search_ingestion_query_interval_sec",
    0,
    "The time in seconds to wait between search queries during live data"
    " ingestion.",
)

_INIT_DATASET_COPIES = flags.DEFINE_integer(
    "edw_search_ingestion_init_dataset_copies",
    2,
    "The number of copies of the dataset to insert into the table on Initial"
    " Load step (before Main step, which consists of concurrent ingestion and"
    " text searches).",
    lower_bound=1,
)
_DATASET_COPIES_TO_INGEST = flags.DEFINE_integer(
    "edw_search_ingestion_dataset_copies_to_ingest",
    2,
    "The number of copies of the dataset specified via"
    " --edw_search_data_location (which shouldn't have the rare token) to"
    " ingest during the Main step (where we concurrently ingest new records and"
    " perform text searches).",
)
_RARE_TOKEN_DATASET_COPIES_TO_INGEST = flags.DEFINE_integer(
    "edw_search_ingestion_rare_token_copies_to_ingest",
    2,
    "The number of copies of the rare token dataset to ingest into the table"
    " during the Main step.",
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

_INITIAL_SEARCH_COUNT = flags.DEFINE_integer(
    "edw_search_ingestion_initial_search_count",
    5,
    "Number of times to perform each search query defined with"
    " --edw_search_ingestion_queries after Initial Load.",
)

_FINAL_SEARCH_COUNT = flags.DEFINE_integer(
    "edw_search_ingestion_final_search_count",
    5,
    "Number of times to perform each search query defined"
    " --edw_search_ingestion_queries at the end of the benchmark.",
)

_COMMON_TOKEN = flags.DEFINE_string(
    "edw_search_ingestion_common_token", None, "The common token to search for."
)

_RARE_TOKEN = flags.DEFINE_string(
    "edw_search_ingestion_rare_token",
    None,
    "The rare token to search for. This token will be injected into"
    " --edw_search_ingestion_initial_data_rare_token_count rows on the initial"
    " dataset.",
)

_INITIAL_DATA_RARE_TOKEN_COUNT = flags.DEFINE_integer(
    "edw_search_ingestion_initial_data_rare_token_count",
    None,
    "The number of rows to inject the rare token into on the initial dataset.",
)

_DATA_DATE_RANGES = flags.DEFINE_list(
    "edw_search_ingestion_data_date_range",
    [],
    "A list 3 of ISO-formatted dates in ISO date format (e.g. '2025-01-01'):"
    " initial_date; the first date in table. initial_data_last_date; the last"
    " date in initial data (inclusive). last_date; the last date after for all"
    " the data (inclusive).",
)

_RARE_TOKEN_DATA_LOCATION = flags.DEFINE_string(
    "edw_search_ingestion_rare_token_data_location",
    None,
    "Cloud directory of bucket to source ongoing load data with the rare"
    " token for EDW search benchmarks.",
)


FLAGS = flags.FLAGS


@dataclasses.dataclass(frozen=True)
class _SearchQuery:
  """Represents a search query to be issued.

  Attributes:
    name: The name of the query (for returned sample metadata).
    term: The term to search for.
    order_by: The column to order the results by.
    limit: The maximum number of results to return.
    date_between: A tuple of two dates to search between.
  """

  name: str
  term: str
  order_by: str | None = None
  limit: str | None = None
  date_between: tuple[datetime.date, datetime.date] | None = None

  def ExecuteIndexSearchQuery(
      self,
      edw_service_instance: edw_service.EdwService,
      table_path: str,
      bench_meta: dict[Any, Any],
  ) -> sample.Sample:
    """Executes a single text search query and returns the sample.

    Args:
      edw_service_instance: The EdwService instance to use for executing
        queries.
      table_path: The name of the table to query.
      bench_meta: Metadata to add to the collected sample.

    Returns:
      A sample.Sample object representing the execution time of the query.
    """
    execution_time, metadata = edw_service_instance.TextSearchQuery(
        table_path,
        self.term,
        self.order_by,
        self.limit,
        self.date_between,
    )
    return sample.Sample(
        "query_execution_time",
        execution_time,
        "seconds",
        metadata | bench_meta | {"search_query_name": self.name},
    )


_ONE_DAY = datetime.timedelta(days=1)


def _ParseDateRanges(date_ranges: list[str]) -> list[datetime.date]:
  """Parses a list of ISO-formatted dates.

  Args:
    date_ranges: A list of strings, expected to be in ISO date format.

  Returns:
    A list of datetime.date objects.

  Raises:
    ValueError: If any date is not in the correct format.
  """
  return [datetime.date.fromisoformat(date_str) for date_str in date_ranges]


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
  required_flags = [
      _COMMON_TOKEN,
      _RARE_TOKEN,
      _INITIAL_DATA_RARE_TOKEN_COUNT,
      _RARE_TOKEN_DATA_LOCATION,
      edw_service.EDW_SEARCH_TABLE_NAME,
      edw_service.EDW_SEARCH_INIT_DATA_LOCATION,
      edw_service.EDW_SEARCH_DATA_LOCATION,
      edw_service.EDW_SEARCH_INDEX_NAME,
  ]
  for flag in required_flags:
    if flag.value is None:
      raise errors.Config.InvalidValue(
          f"edw_index_ingestion_benchmark requires --{flag.name} flag to be"
          " set."
      )
  if _DATA_DATE_RANGES.value:
    if len(_DATA_DATE_RANGES.value) != 3:
      raise errors.Config.InvalidValue(
          "Expected exactly 3 dates for --edw_search_ingestion_data_date_range,"
          f" but got {len(_DATA_DATE_RANGES.value)}."
      )
    try:
      _ParseDateRanges(_DATA_DATE_RANGES.value)
    except ValueError as e:
      raise errors.Config.InvalidValue(
          "Invalid date format found in --edw_search_ingestion_data_date_range."
          " Expected ISO format (YYYY-MM-DD)."
      ) from e


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
    rare_token_data_path: str,
    dataset_copies_to_ingest: int,
    rare_token_dataset_copies_to_ingest: int,
    interval: float,
    ingestion_finished: threading.Event,
    ingestion_warehouse: str | None,
    already_loaded_rows: int,
    dataset_rows: int,
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
    data_path: The path to the data to be loaded (without rare token).
    rare_token_data_path: The path to the data with the rare token to be loaded.
    dataset_copies_to_ingest: The number of copies of the dataset to ingest
      (without rare token).
    rare_token_dataset_copies_to_ingest: The number of copies of the rare token
      dataset to ingest.
    interval: The time in seconds to wait between data insertion calls.
    ingestion_finished: A threading.Event-like object to signal when the loading
      process is finished.
    ingestion_warehouse: The name of the warehouse to use for ingestion queries
      (Snowflake only).
    already_loaded_rows: The number of rows already loaded into the table.
    dataset_rows: The number of rows in the dataset.
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
    data_path_seq = random.sample(
        [data_path, rare_token_data_path],
        counts=[
            dataset_copies_to_ingest,
            rare_token_dataset_copies_to_ingest,
        ],
        k=dataset_copies_to_ingest + rare_token_dataset_copies_to_ingest,
    )
    for i, path in enumerate(data_path_seq):
      ingestion_start_time = time.monotonic()
      execution_time, metadata = service.InsertSearchData(table_name, path)
      current_rows = already_loaded_rows + (i + 1) * dataset_rows
      metadata["current_rows"] = current_rows
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
  """

  edw_service_instance: edw_service.EdwService
  table_name: str

  def _CreateOneRandomDaySearch(
      self, query: _SearchQuery, date_range: tuple[datetime.date, datetime.date]
  ) -> _SearchQuery:
    random_days_passed = random.randint(0, (date_range[1] - date_range[0]).days)
    random_date = date_range[0] + random_days_passed * _ONE_DAY
    return dataclasses.replace(
        query,
        name=f"{query.name}_1day",
        date_between=(random_date, random_date),
    )

  def _CreateLast30DaysSearch(
      self, query: _SearchQuery, end_date: datetime.date
  ) -> _SearchQuery:
    return dataclasses.replace(
        query,
        name=f"{query.name}_last30days",
        date_between=(end_date - 29 * _ONE_DAY, end_date),
    )

  def ExecuteSearchQueryNTimes(
      self,
      search_query: _SearchQuery,
      date_range: tuple[datetime.date, datetime.date],
      n: int,
      cooldown_sec: float = 0,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries n times.

    Args:
      search_query: The search query to execute.
      date_range: The date range to search within.
      n: The number of times to execute the query.
      cooldown_sec: The time in seconds to wait between queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """

    def _Generator():
      for _ in range(n):
        # search one random day
        yield self._CreateOneRandomDaySearch(search_query, date_range)
        # search last 30 days
        yield self._CreateLast30DaysSearch(search_query, date_range[1])
        time.sleep(cooldown_sec)

    return self._ExecuteIndexSearchQueriesFor(_Generator(), bench_meta)

  def ExecuteSearchQueryUntilEvent(
      self,
      search_queries: list[_SearchQuery],
      date_range: tuple[datetime.date, datetime.date],
      event: threading.Event,
      cooldown_sec: float,
      bench_meta: dict[str, Any] | None = None,
  ) -> list[sample.Sample]:
    """Executes search queries in a loop until an event is set.

    Args:
      search_queries: A list of _SearchQuery objects to randomly choose from.
      date_range: The date range to search within.
      event: A threading.Event-like object to signal when to stop executing
        queries.
      cooldown_sec: The time in seconds to wait between queries.
      bench_meta: Metadata to add to the collected samples.

    Returns:
      A list of sample.Sample objects representing the execution time of each
      query.
    """

    def _Generator():
      search_queries_with_dates = []
      for q in search_queries:
        search_queries_with_dates += [
            self._CreateOneRandomDaySearch(q, date_range),
            self._CreateLast30DaysSearch(q, date_range[1]),
        ]
      while not event.is_set():
        search_query = random.choice(search_queries_with_dates)
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
    for i, search_query in enumerate(iterable):
      query_meta = {
          "search_query_iter": i,
      }
      samples.append(
          search_query.ExecuteIndexSearchQuery(
              self.edw_service_instance,
              self.table_name,
              bench_meta=bench_meta | query_meta,
          )
      )
    return samples


def _FetchQueryPercentageUntilEvent(
    service: edw_service.EdwService,
    table_name: str,
    index_name: str,
    ingestion_finished: threading.Event,
    bench_meta: dict[Any, Any] | None = None,
) -> list[sample.Sample]:
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
  search_queries = [
      _SearchQuery(
          "common",
          _COMMON_TOKEN.value,
          order_by="event_timestamp DESC",
          limit=100,
      ),
      _SearchQuery("rare", _RARE_TOKEN.value, order_by="event_timestamp DESC"),
  ]
  first_date, initial_data_last_date, last_date = _ParseDateRanges(
      _DATA_DATE_RANGES.value
  )

  gen_metadata = {
      "edw_index_search_table": edw_service.EDW_SEARCH_TABLE_NAME.value,
      "edw_index_search_index": edw_service.EDW_SEARCH_INDEX_NAME.value,
      "edw_index_init_data_location": (
          edw_service.EDW_SEARCH_INIT_DATA_LOCATION.value
      ),
      "edw_index_data_location": edw_service.EDW_SEARCH_DATA_LOCATION.value,
      "edw_index_dataset_copies_to_ingest": _DATASET_COPIES_TO_INGEST.value,
      "edw_index_init_dataset_copies": _INIT_DATASET_COPIES.value,
      "edw_index_ingestion_load_interval_sec": _LOAD_INTERVAL_SEC.value,
      "edw_index_ingestion_query_interval_sec": _QUERY_INTERVAL_SEC.value,
      "edw_index_ingestion_index_wait": _INDEX_WAIT.value,
      "edw_index_ingestion_initial_search_count": _INITIAL_SEARCH_COUNT.value,
      "edw_index_ingestion_final_search_count": _FINAL_SEARCH_COUNT.value,
      "edw_index_ingestion_initial_data_rare_token_count": (
          _INITIAL_DATA_RARE_TOKEN_COUNT.value
      ),
      "edw_index_ingestion_rare_token_data_location": (
          _RARE_TOKEN_DATA_LOCATION.value
      ),
      "edw_index_ingestion_rare_token_copies_to_ingest": (
          _RARE_TOKEN_DATASET_COPIES_TO_INGEST.value
      ),
  }
  if isinstance(edw_service_instance, bigquery.Bigquery):
    gen_metadata["edw_index_table_partitioned"] = (
        bigquery.INITIALIZE_SEARCH_TABLE_PARTITIONED.value
    )

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

  logging.info("Inserting rare token rows.")
  edw_service_instance.InjectTokenIntoTable(
      edw_service.EDW_SEARCH_TABLE_NAME.value,
      _RARE_TOKEN.value,
      _INITIAL_DATA_RARE_TOKEN_COUNT.value,
  )

  indexing_start_time = time.time()
  logging.info("Creating index")
  edw_service_instance.CreateSearchIndex(
      edw_service.EDW_SEARCH_TABLE_NAME.value,
      edw_service.EDW_SEARCH_INDEX_NAME.value,
  )
  already_loaded_rows, _ = edw_service_instance.GetTableRowCount(
      edw_service.EDW_SEARCH_TABLE_NAME.value
  )
  dataset_rows = already_loaded_rows / _INIT_DATASET_COPIES.value

  if _INDEX_WAIT.value:
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
  )

  if _INITIAL_SEARCH_COUNT.value > 0:
    current_step_meta = {"edw_index_current_step": _Steps.INITIAL_SEARCH.value}
    logging.info("Running preload search queries")
    for search_query in search_queries:
      samples += query_submitter.ExecuteSearchQueryNTimes(
          search_query,
          (first_date, initial_data_last_date),
          _INITIAL_SEARCH_COUNT.value,
          bench_meta=gen_metadata | current_step_meta,
      )

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
                _RARE_TOKEN_DATA_LOCATION.value,
                _DATASET_COPIES_TO_INGEST.value,
                _RARE_TOKEN_DATASET_COPIES_TO_INGEST.value,
                _LOAD_INTERVAL_SEC.value,
                ingestion_finished,
                _SNOWFLAKE_INGESTION_WAREHOUSE.value,
                already_loaded_rows,
                dataset_rows,
            ),
            {"bench_meta": gen_metadata | current_step_meta},
        ),
        (
            query_submitter.ExecuteSearchQueryUntilEvent,
            (
                search_queries,
                (first_date, last_date),
                ingestion_finished,
                _QUERY_INTERVAL_SEC.value,
            ),
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

  if _INDEX_WAIT.value:
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

  if _FINAL_SEARCH_COUNT.value > 0:
    current_step_meta = {"edw_index_current_step": _Steps.FINAL_SEARCH.value}
    logging.info("Running post load queries")
    for search_query in search_queries:
      samples += query_submitter.ExecuteSearchQueryNTimes(
          search_query,
          (first_date, last_date),
          _FINAL_SEARCH_COUNT.value,
          bench_meta=gen_metadata | current_step_meta,
      )

  return sorted(samples, key=lambda s: s.timestamp)


def Cleanup(spec: benchmark_spec.BenchmarkSpec):
  """Cleans up the benchmark resources.

  Args:
    spec: The benchmark specification.
  """
  spec.edw_service.Cleanup()
