r"""Driver script for P3rf EDW BigQuery benchmarks.

This is a benchmark driver script designed to integrate with Artemis' EDW
benchmarking framework via the EdwClientInterface interfaces. See
third_party/py/perfkitbenchmarker/edw_service.py and
third_party/py/perfkitbenchmarker/providers/gcp/bigquery.py for context.

This script is designed to be deployed to an Artemis runner VM and executed
remotely. It does not rely on the google3 environment or Blaze, and is not
intended to be built or run inside a google3 environment.

Dependendies:
  pip install google-cloud-bigquery
  pip install google-cloud-bigquery-storage
  pip install pyarrow

pyarrow is necessary for fast loading of large results.
Usage example:
    ./bq_python_driver.py single \
    --project=p3rf-bigquery-us-py \
    --credentials_file=p3rf-bigquery-smallquery-slots.json \
    --dataset=tpcds_10T \
    --labels={"pkb-test-run":"value"} \
    --query_file=1
"""

import argparse
import enum
import json
import multiprocessing
from multiprocessing import managers
import os
import time
from typing import Any
from google.cloud import bigquery
from google.oauth2 import service_account

RESULTS_PAGE_SIZE = 1000
SETUP_SYNCHRONIZATION_TIMEOUT = 1200


class BqConfiguration(enum.StrEnum):
  """Enum of BigQuery configurations for different features.

  Use this to enable experimental or optional BQ features, or to specify
  configurations optimized for different benchmarking scenarios.
  """

  JOB_OPTIONAL = enum.auto()  # Enable optional small-query optimizations.
  DEFAULT = enum.auto()  # Default configuration.


def main() -> None:

  parser = build_arg_parser()

  args = parser.parse_args()

  if args.skip_results and args.print_results:
    raise ValueError(
        "Invalid configuration: --skip_results and --print_results are mutually"
        " exclusive."
    )

  # Set global configuration options (environment variables, etc.)
  args.feature_config = [
      BqConfiguration(feature) for feature in args.feature_config
  ]
  for feature in args.feature_config:
    match feature:
      case BqConfiguration.JOB_OPTIONAL:
        os.environ["QUERY_PREVIEW_ENABLED"] = "true"

  # Dispatches to the appropriate function, as specified by the subparser.
  args.func(args)


def build_arg_parser() -> argparse.ArgumentParser:
  """Builds the argparse parser for this driver.

  Returns:
    An argparse.ArgumentParser object for parsing command line arguments.
  """

  # Shared arguments for all subcommands.
  shared_parser = argparse.ArgumentParser(add_help=False)
  shared_parser.add_argument(
      "-p",
      "--project",
      required=True,
      help="BigQuery project ID containing the benchmarking dataset.",
  )
  shared_parser.add_argument(
      "-c",
      "--credentials_file",
      required=True,
      help="Path to the service account credentials JSON file.",
  )
  shared_parser.add_argument(
      "-d",
      "--dataset",
      required=True,
      help="Name of dataset to run queries against.",
  )
  shared_parser.add_argument(
      "-sr",
      "--skip_results",
      default=False,
      action="store_true",
      help="Skip downloading query results.",
  )
  shared_parser.add_argument(
      "-pr",
      "--print_results",
      default=False,
      action="store_true",
      help="Include query results in the output.",
  )
  shared_parser.add_argument(
      "-l",
      "--labels",
      type=str,
      help="Labels to apply to query jobs, supplied in JSON format.",
      default="{}",
  )
  shared_parser.add_argument(
      "-f",
      "--feature_config",
      type=str.lower,
      default=[],
      nargs="*",
      action="extend",
      choices=[e.value for e in BqConfiguration],
      help="Which BigQuery configuration options to use.",
  )

  parser = argparse.ArgumentParser(
      prog="p3rf bigquery python benchmark driver", add_help=False
  )

  subparsers = parser.add_subparsers()

  single_parser = subparsers.add_parser(
      "single",
      help="Benchmark a single query.",
      parents=[shared_parser],
  )
  single_parser.add_argument(
      "-q", "--query_file", required=True, help="Path to the query file."
  )
  single_parser.add_argument(
      "--destination",
      type=str,
      help="Fully qualified table name where query results will be saved on.",
  )
  single_parser.set_defaults(func=benchmark_single_query)

  throughput_parser = subparsers.add_parser(
      "throughput",
      help="Benchmark a multiplexed set of query streams in parallel.",
      parents=[shared_parser],
  )
  throughput_parser.add_argument(
      "-qs",
      "--query_streams",
      required=True,
      help=(
          "A list of lists of query files, representing streams to benchmark."
          "Should be supplied in JSON format as follows: "
          "[[query1, query2], [query3, query4], ...]"
      ),
  )
  throughput_parser.set_defaults(func=benchmark_throughput)

  return parser


def benchmark_single_query(args: argparse.Namespace):
  """Benchmarks the performance of a single query.

  Args:
    args: Command line arguments supplied by argparse.
  """

  query_file: str = args.query_file
  project_id: str = args.project
  credentials: str = args.credentials_file
  dataset_id: str = args.dataset
  labels: dict[str, str] = json.loads(args.labels)
  skip_results: bool = args.skip_results
  print_results: bool = args.print_results
  destination: str | None = args.destination

  with open(query_file, "r") as f:
    querytext = f.read()

  # Configure connection
  credentials = service_account.Credentials.from_service_account_file(
      credentials
  )
  client = bigquery.Client(project=project_id, credentials=credentials)

  query_config = create_query_config(
      project_id, dataset_id, labels, destination
  )

  benchmark_results = run_and_time_query(
      client, querytext, query_config, skip_results, print_results
  )

  benchmark_results["query"] = query_file

  print(json.dumps(benchmark_results, indent=2, sort_keys=True))


def benchmark_throughput(args: argparse.Namespace):
  """Benchmarks the performance of a multiplexed set of query streams in parallel."""
  query_file_streams: list[list[str]] = json.loads(args.query_streams)
  project_id: str = args.project
  credentials_file: str = args.credentials_file
  dataset_id: str = args.dataset
  labels: dict[str, str] = json.loads(args.labels)
  skip_results: bool = args.skip_results
  print_results: bool = args.print_results

  credentials = service_account.Credentials.from_service_account_file(
      credentials_file
  )

  stream_count = len(query_file_streams)

  with multiprocessing.Manager() as proc_manager:
    stream_results = proc_manager.list()
    processes = []
    barrier = multiprocessing.Barrier(stream_count + 1)
    streams: list[BqThroughputStream] = []
    for i in range(stream_count):
      streams.append(
          BqThroughputStream(
              query_stream=query_file_streams[i],
              stream_index=i,
              project_id=project_id,
              credentials=credentials,
              dataset_id=dataset_id,
              labels=labels,
              skip_results=skip_results,
              print_results=print_results,
              barrier=barrier,
              output=stream_results,
          )
      )
    for config in streams:
      processes.append(multiprocessing.Process(target=config))
    for p in processes:
      # Setting daemon=True ensures that child processes are killed when the
      # parent process exits.
      p.daemon = True
      p.start()

    # Wait for child processes to finish initialization step
    barrier.wait(timeout=SETUP_SYNCHRONIZATION_TIMEOUT)
    throughput_start = time.monotonic_ns()
    throughput_start_timestamp = time.time_ns()
    barrier.wait()
    throughput_end = time.monotonic_ns()
    throughput_end_timestamp = time.time_ns()
    results = {
        "throughput_start": throughput_start_timestamp / 1000000,
        "throughput_end": throughput_end_timestamp / 1000000,
        "throughput_wall_time_in_secs": (
            (throughput_end - throughput_start) / (1000 * 1000000)
        ),
        "all_streams_performance_array": list(stream_results),
    }
    print(json.dumps(results, indent=2, sort_keys=True))


def create_query_config(
    project_id: str,
    dataset_id: str,
    labels: dict[str, str] | None = None,
    destination: str | None = None,
) -> bigquery.QueryJobConfig:
  """Creates a BigQuery QueryJobConfig object.

  Args:
    project_id: The project ID to use for the query.
    dataset_id: The dataset ID to use for the query.
    labels: The labels to apply to the query job.
    destination: The destination table to save query results to, or None if just
      saving them into a random temp table (default behavior).

  Returns:
    A BigQuery QueryJobConfig object.
  """
  config = bigquery.QueryJobConfig(
      default_dataset=f"{project_id}.{dataset_id}",
      use_query_cache=False,
      use_legacy_sql=False,
  )
  if destination:
    config.destination = destination
  if labels:
    config.labels = labels
  return config


def run_and_time_query(
    client: bigquery.Client,
    querytext: str,
    query_config: bigquery.QueryJobConfig,
    skip_results: bool,
    print_results: bool,
) -> dict[str, Any]:
  """Runs a single query and measures its execution time.

  Args:
    client: BigQuery client to use for the query.
    querytext: The query to run.
    query_config: The configuration to use for the query job.
    skip_results: Whether to skip downloading query results.
    print_results: Whether to include query results in the output details.

  Returns:
    A dictionary containing the query start time, end time, wall time, and other
    details.
  """

  output_rows = (
      [] if print_results else ["<omitted, use --print_results to include>"]
  )

  start_timestamp_ms = time.time_ns()  # convert to ms
  start_time_ms = time.monotonic_ns()
  results = client.query_and_wait(
      querytext, job_config=query_config, page_size=RESULTS_PAGE_SIZE
  )
  end_before_result_read_timestamp_ms = time.time_ns()
  end_before_result_read = time.monotonic_ns()
  result_count = results.total_rows
  if not skip_results:
    if result_count is not None and result_count > 0:
      loaded_results = results.to_arrow()
      if print_results:
        output_rows = loaded_results.to_pydict()
  end_timestamp_ms = time.time_ns()
  end_time_ms = time.monotonic_ns()

  return {
      "query_start": start_timestamp_ms / 1000000,
      "query_end": end_timestamp_ms / 1000000,
      "query_wall_time_in_secs": (
          (end_time_ms - start_time_ms) / (1000 * 1000000)
      ),
      "details": {
          "query_end_before_result_read": (
              end_before_result_read_timestamp_ms / 1000000
          ),
          "rows_returned": result_count,
          "results_load_time": (end_time_ms - end_before_result_read) / 1000000,
          "job_id": results.job_id,
          "query_id": results.query_id,
          "query_results_loaded": not skip_results,
          "query_results": output_rows,
      },
  }


class BqThroughputStream:
  """Class for running a single stream of queries in throughput mode."""

  def __init__(
      self,
      query_stream: list[str],
      stream_index: int,
      project_id: str,
      credentials: service_account.Credentials,
      dataset_id: str,
      labels: dict[str, str],
      skip_results: bool,
      print_results: bool,
      barrier: multiprocessing.Barrier,
      output: managers.ListProxy,
  ):
    self.query_stream = query_stream
    self.stream_index = stream_index
    self.project_id = project_id
    self.credentials = credentials
    self.dataset_id = dataset_id
    self.labels = labels
    self.skip_results = skip_results
    self.print_results = print_results
    self.throughput_synchro = barrier
    self.output = output
    self.loaded_queries: list[tuple[str, str]] = []
    self.client: bigquery.Client | None = None
    self.query_config: bigquery.QueryJobConfig | None = None

  def __call__(self) -> dict[str, Any]:
    """Orchestrates a single throughput stream for throughput benchmarks on BQ.

    This function should only be called in child processes.
    """
    self._setup()
    # Synchronize so that all processes start benchmarking at the same time.
    self.throughput_synchro.wait()
    results = self._run()
    self.output.append(results)
    self.throughput_synchro.wait()

  def _setup(self) -> None:
    """Prepares the stream for benchmarking.

    Loads queries into memory and configures a BigQuery client connection.
    """
    self.client = bigquery.Client(
        project=self.project_id, credentials=self.credentials
    )
    self.query_config = create_query_config(
        self.project_id, self.dataset_id, self.labels
    )

    self.labels["stream_id"] = str(self.stream_index)
    self.loaded_queries = []
    for query_file in self.query_stream:
      with open(query_file, "r") as f:
        self.loaded_queries.append((query_file, f.read()))

  def _run(self) -> dict[str, Any]:
    """Benchmarks the performance of a single stream of queries.

    Returns:
      A dictionary of the format:
      {
        "stream_start": <float>,
        "stream_end": <float>,
        "stream_wall_time_in_secs": <float>,
        "stream_performance_array": [
            {
                "query": <str>,
                "query_start": <float>,
                "query_end": <float>,
                "query_wall_time_in_secs": <float>,
                "details": {
                        <optional query metadata>
                    ],
                },
            },
            ...
        ],
        "details": {
            <optional stream metadata>
        },
      }
    """
    stream_results = []
    stream_start_timestamp = time.time_ns()
    stream_start = time.monotonic_ns()
    for query_file_name, querytext in self.loaded_queries:
      self.labels["query"] = query_file_name
      benchmark_results = run_and_time_query(
          self.client,
          querytext,
          self.query_config,
          self.skip_results,
          self.print_results,
      )
      benchmark_results["query"] = query_file_name
      stream_results.append(benchmark_results)
    stream_end = time.monotonic_ns()
    stream_end_timestamp = time.time_ns()

    return {
        "stream_start": stream_start_timestamp / 1000000,
        "stream_end": stream_end_timestamp / 1000000,
        "stream_wall_time_in_secs": (
            (stream_end - stream_start) / (1000 * 1000000)
        ),
        "stream_performance_array": stream_results,
        "details": {
            "stream_index": self.stream_index,
        },
    }


if __name__ == "__main__":
  main()

