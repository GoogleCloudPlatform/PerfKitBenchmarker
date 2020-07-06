# Lint as: python3
"""Aggregates the performance results from a edw benchmark.

An edw benchmark, runs multiple iterations of a suite of queries.
Independent raw query performance is aggregated during the benchmark, and used
for generating:
a. Raw query performance samples
b. Aggregated query performance samples
c. Raw wall time for each suite iteration
d. Aggregated wall time by summing the aggregated query performances
e. Raw geo mean performance for each suite iteration
f. Aggregated geo mean performance using the aggregated query performances
"""
import copy
import enum
import functools
from typing import Any, Dict, List, Text

import numpy as np
from perfkitbenchmarker import sample


class EdwPerformanceAggregationError(Exception):
  """Error encountered during aggregation of performance results."""


def geometric_mean(iterable: List[float]):
  """Function to compute the geo mean for a list of numeric values.

  Args:
    iterable: A List of Float performance values

  Returns:
    A float value equal to the geometric mean of the inpute performance values

  Raises:
    EdwPerformanceAggregationError: If an invalid performance value was included
    for aggregation
  """
  if (not iterable or any(perf <= 0.0 for perf in iterable)):
    raise EdwPerformanceAggregationError('Invalid values cannot be aggregated.')
  a = np.array(iterable)
  return a.prod() ** (1.0 / len(a))


class EdwQueryExecutionStatus(enum.Enum):
  """Enum class for potential status of query execution.

  Potential values:
  FAILED: Indicates that the query execution failed.
  SUCCESSFUL: Indicates that the query execution succeeded.
  """
  FAILED = 'query_execution_failed'
  SUCCESSFUL = 'query_execution_successful'


class EdwQueryPerformance(object):
  """Class that represents the performance of an executed edw query.

  Attributes:
    name: A string name of the query that was executed
    performance: A Float variable set to the query's completion time in secs.
    -1.0 is used as a sentinel value implying the query failed. For a successful
    query the value is expected to be positive.
    execution_status: An EdwQueryExecutionStatus enum indicating success/failure
    metadata: A dictionary of query execution attributes (job_id, etc.)
  """

  def __init__(self, query_name: Text, performance: float,
               metadata: Dict[str, str]):
    self.name = query_name
    self.performance = performance
    self.execution_status = (EdwQueryExecutionStatus.FAILED
                             if performance == -1.0
                             else EdwQueryExecutionStatus.SUCCESSFUL)
    self.metadata = metadata

  def get_performance_sample(self, metadata: Dict[str, str]) -> sample.Sample:
    """Method to generate a sample for the query performance.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
      execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample for the edw query performance.
    """
    query_metadata = copy.copy(metadata)
    query_metadata['query'] = self.name
    query_metadata['execution_status'] = self.execution_status
    query_metadata.update(self.metadata)
    return sample.Sample('edw_raw_query_time', self.performance, 'seconds',
                         query_metadata)

  def get_performance_value(self) -> float:
    """Method to get the query's completion time in secs.

    Returns:
      A float value set to the query's completion time in secs.
    """
    return self.performance

  def get_performance_metadata(self) -> Dict[str, str]:
    """Method to get the query's execution attributes (job_id, etc.).

    Returns:
      A dictionary set to query's execution attributes (job_id, etc.)
    """
    return self.metadata

  def is_successful(self) -> bool:
    """Validates if the query was successful."""
    return self.execution_status == EdwQueryExecutionStatus.SUCCESSFUL


class EdwSuitePerformance(object):
  """Class that represents the performance of a suite of edw queries.

  Attributes:
    name: A string name of the suite comrised of suite_name and sequence (tpc_1)
    performance: A dictionary of query name to its execution performance which
      is a EdwQueryPerformance instance
    total_count: An integer count of the total number of queries in the suite
    successful_count: An integer count of the successful queries in the suite
  """

  def __init__(self, suite_name: Text, suite_sequence: Text,
               total_suite_queries: int):
    self.name = suite_name + '_' + suite_sequence
    self.performance = {}
    self.total_count = total_suite_queries
    self.successful_count = 0

  def add_query_performance(self, query_performance: EdwQueryPerformance):
    """Updates the suite's performance map, with a member query performance.

    The method also increaments the success and failure query counts.

    Args:
      query_performance: An instance of EdwQueryPerformance to be added
    """
    self.performance[query_performance.name] = query_performance
    if query_performance.is_successful():
      self.successful_count += 1

  def has_query_performance(self, query_name: Text) -> bool:
    """Makes sure query was executed as part of the suite.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was executed in the suite.
    """
    return query_name in self.performance

  def is_query_successful(self, query_name: Text) -> bool:
    """Makes sure query was successful in the suite.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was successful in the suite.
    """
    return self.performance.get(query_name).is_successful()

  def get_query_performance(self, query_name: Text) -> EdwQueryPerformance:
    """Gets a query's execution performance generated during suite execution.

    Args:
      query_name: A String name of the query to retrieve details for

    Returns:
      An EdwQueryPerformance instance for the requested query
    """
    return self.performance[query_name]

  def get_all_queries_in_suite(self) -> List[Text]:
    """Gets a list of names of all queries in the Suite.

    Returns:
      A list of all queries in the Suite.
    """
    return self.performance.keys()

  def get_all_query_performance_samples(self, metadata: Dict[str, str]) -> List[
      sample.Sample]:
    """Get a list of samples for all queries in the Suite.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
      execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples of each query's performance
    """
    return [x.get_performance_sample(metadata)
            for x in self.performance.values()]

  def is_successful(self) -> bool:
    """Check if the suite was successful, if all the member queries succeed."""
    return self.total_count == self.successful_count

  def get_wall_time_performance_sample(self, metadata: Dict[
      str, str]) -> sample.Sample:
    """Get a sample for wall time performance of the suite.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
      execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of suite wall time performance
    """
    wall_time = sum([x.performance for x in self.performance.values()])
    wall_time_metadata = copy.copy(metadata)
    return sample.Sample('edw_raw_wall_time', wall_time, 'seconds',
                         wall_time_metadata)

  def get_queries_geomean_performance_sample(self, metadata: Dict[
      str, str]) -> sample.Sample:
    """Get a sample for Geometric mean performance of the suite.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
      execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of suite geomean performance

    Raises:
      EdwPerformanceAggregationError: If the suite contains unsuccessful query
      executions
    """
    if not self.is_successful():
      raise EdwPerformanceAggregationError('Failed executions in suite.')
    query_performances = [x.performance for x in self.performance.values()]
    raw_geo_mean = geometric_mean(query_performances)
    geo_mean_metadata = copy.copy(metadata)
    return sample.Sample('edw_raw_geomean_time', raw_geo_mean, 'seconds',
                         geo_mean_metadata)


class EdwBenchmarkPerformance(object):
  """Class that represents the performance of an edw benchmark.

  Attributes:
    total_iterations: An integer variable set to total of number of iterations.
    expected_suite_queries: A list of query names that are executed in a
       benchmark's suite
    suite_performances: A dictionary of suite's execution sequence index (String
      value) to its execution performance (an instance of EdwSuitePerformance)
  """

  def __init__(self, total_iterations: int, expected_suite_queries: List[Text]):
    self.total_iterations = total_iterations
    self.expected_suite_queries = expected_suite_queries
    self.suite_performances = {}

  def validate_suite_query_coverage(self, edw_suite_performance:
                                    EdwSuitePerformance):
    """Validates that the suite has the performance of the expected queries.

    Args:
      edw_suite_performance: An instance of EdwSuitePerformance encapsulating
        the suite performance details.

    Returns:
      A boolean value indicating if the suite has expected query coverage
    """
    return set(edw_suite_performance.get_all_queries_in_suite()) == set(
        self.expected_suite_queries)

  def add_suite_performance(self, sequence_idx: Text,
                            edw_suite_performance: EdwSuitePerformance):
    """Add a suite's execution performance to the benchmark results.

    Args:
      sequence_idx: String iteration index for the suite's run. It serves as the
        key in the benchmark's suite_performances map
      edw_suite_performance: An instance of EdwSuitePerformance encapsulating
        the suite performance details.

    Raises:
      EdwPerformanceAggregationError: If suite contains unexpected queries
    """
    if not self.validate_suite_query_coverage(edw_suite_performance):
      raise EdwPerformanceAggregationError('Attempting to aggregate an invalid'
                                           ' suite.')
    self.suite_performances[sequence_idx] = edw_suite_performance

  def is_successful(self) -> bool:
    """Check a benchmark's success, only if all the suite sequences succeed."""
    return functools.reduce(
        (lambda x, y: x and y),
        [x.is_successful() for x in self.suite_performances.values()])

  def get_consistently_failing_queries(self) -> List[str]:
    """Returns a list of any queries that failed on every attempt."""
    if self.total_iterations == 1:
      # If a query is only attempted once, a failure can't be called consistent
      # with certainty, as it may be a passing issue.
      return []
    failing_queries = []
    for query in self.expected_suite_queries:
      if all(not suite_performance.is_query_successful(query)
             for suite_performance in self.suite_performances.values()):
        failing_queries.append(query)
    return failing_queries

  def aggregated_query_status(self, query_name: Text) -> bool:
    """Get the status of query aggregated across all iterations.

    A query is considered successful only if
      a. Query was executed in every suite run
      b. Query was successful in every suite run

    Args:
      query_name: Name of the query whose aggregated success is requested

    Returns:
      A boolean value indicating if the query was successful in the benchmark.
    """
    for performance in self.suite_performances.values():
      if not performance.has_query_performance(query_name):
        return False
      if not performance.is_query_successful(query_name):
        return False
    return True

  def aggregated_query_execution_time(self, query_name: Text) -> float:
    """Get the execution time of query aggregated across all iterations.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A float value set to the query's aggregated execution time

    Raises:
      EdwPerformanceAggregationError: If the query failed in one or more suites
    """
    if not self.aggregated_query_status(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed'
                                           ' query' + query_name)
    query_performances = [x.get_query_performance(query_name)
                          .get_performance_value()
                          for x in self.suite_performances.values()]
    return sum(query_performances) / self.total_iterations

  def aggregated_query_metadata(self, query_name: Text) -> Dict[str, Any]:
    """Get the metadata of query aggregated across all iterations.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A dictionary set to the query's aggregated metadata, accumulated from the
       raw query runs.

    Raises:
      EdwPerformanceAggregationError: If the query failed in one or more suites
    """
    if not self.aggregated_query_status(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed'
                                           ' query' + query_name)
    result = {}
    for s_idx, s_performance in self.suite_performances.items():
      q_performance = s_performance.get_query_performance(query_name)
      result[s_idx + '_runtime'] = q_performance.get_performance_value()
      result.update({s_idx + '_' + k: v for (k, v) in
                     q_performance.get_performance_metadata().items()})
    return result

  def get_aggregated_query_performance_sample(
      self, query_name: Text, metadata: Dict[str, str]) -> sample.Sample:
    """Get the performance of query aggregated across all iterations.

    Args:
      query_name: Name of the query whose aggregated performance is requested
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of the query's aggregated execution time
    """
    query_metadata = copy.copy(metadata)
    query_metadata['query'] = query_name
    query_metadata['aggregation_method'] = 'mean'
    perf, exec_status, agg_md = -1.0, EdwQueryExecutionStatus.FAILED, {}
    if self.aggregated_query_status(query_name):
      perf = self.aggregated_query_execution_time(query_name=query_name)
      exec_status = EdwQueryExecutionStatus.SUCCESSFUL
      agg_md = self.aggregated_query_metadata(query_name=query_name)
    query_metadata['execution_status'] = exec_status
    query_metadata.update(agg_md)
    return sample.Sample('edw_aggregated_query_time', perf, 'seconds',
                         query_metadata)

  def get_all_query_performance_samples(self, metadata: Dict[str, str]) -> List[
      sample.Sample]:
    """Generates samples for all query performances.

    Benchmark relies on suite runs to generate the raw query performance samples
    Benchmark appends the aggregated query performance sample

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples (raw and aggregated)
    """
    results = []
    # Raw query performance samples
    for iteration, performance in self.suite_performances.items():
      iteration_metadata = copy.copy(metadata)
      iteration_metadata['iteration'] = iteration
      results.extend(performance.get_all_query_performance_samples(
          iteration_metadata))
    # Aggregated query performance samples
    for query in self.expected_suite_queries:
      results.append(self.get_aggregated_query_performance_sample(
          query_name=query, metadata=metadata))
    return results

  def get_aggregated_wall_time_performance_sample(self,
                                                  metadata: Dict[str, str]
                                                  ) -> sample.Sample:
    """Get the wall time performance aggregated across all iterations.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of aggregated wall time
    """
    wall_time = 0
    for query in self.expected_suite_queries:
      wall_time += self.aggregated_query_execution_time(query_name=query)
    wall_time_metadata = copy.copy(metadata)
    wall_time_metadata['aggregation_method'] = 'mean'
    return sample.Sample('edw_aggregated_wall_time', wall_time, 'seconds',
                         wall_time_metadata)

  def get_wall_time_performance_samples(self, metadata: Dict[str, str]) -> List[
      sample.Sample]:
    """Generates samples for all wall time performances.

    Benchmark relies on suite runs to generate the raw wall time performance
      samples
    Benchmark appends the aggregated wall time performance sample

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples (raw and aggregated)
    """
    results = []

    for iteration, performance in self.suite_performances.items():
      iteration_metadata = copy.copy(metadata)
      iteration_metadata['iteration'] = iteration
      results.append(performance.get_wall_time_performance_sample(
          iteration_metadata))

    results.append(self.get_aggregated_wall_time_performance_sample(
        metadata=metadata))
    return results

  def get_aggregated_geomean_performance_sample(self,
                                                metadata:
                                                Dict[str,
                                                     str]) -> sample.Sample:
    """Get the geomean performance aggregated across all iterations.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of aggregated geomean

    Raises:
      EdwPerformanceAggregationError: If the benchmark conatins a failed query
      execution
    """
    if not self.is_successful():
      raise EdwPerformanceAggregationError('Benchmark contains a failed query.')
    aggregated_geo_mean = geometric_mean(
        [self.aggregated_query_execution_time(query_name=query)
         for query in self.expected_suite_queries])

    geomean_metadata = copy.copy(metadata)
    geomean_metadata['intra_query_aggregation_method'] = 'mean'
    geomean_metadata['inter_query_aggregation_method'] = 'geomean'
    return sample.Sample('edw_aggregated_geomean', aggregated_geo_mean,
                         'seconds', geomean_metadata)

  def get_queries_geomean_performance_samples(self, metadata: Dict[str, str]
                                              ) -> List[sample.Sample]:
    """Generates samples for all geomean performances.

    Benchmark relies on suite runs to generate the raw geomean performance
      samples
    Benchmark appends the aggregated geomean performance sample

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples (raw and aggregated)

    Raises:
      EdwPerformanceAggregationError: If the benchmark conatins a failed query
      execution
    """
    if not self.is_successful():
      raise EdwPerformanceAggregationError('Benchmark contains a failed query.')
    results = []

    for iteration, performance in self.suite_performances.items():
      iteration_metadata = copy.copy(metadata)
      iteration_metadata['iteration'] = iteration
      results.append(performance.get_queries_geomean_performance_sample(
          iteration_metadata))

    results.append(self.get_aggregated_geomean_performance_sample(
        metadata=metadata))
    return results
