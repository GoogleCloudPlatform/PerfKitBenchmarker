"""Aggregates the performance results from a edw benchmark.

An edw benchmark, runs multiple iterations of a suite of queries.
Independent raw query performance is aggregated during the benchmark, and used
for generating:
a. Raw query performance samples
b. Aggregated query performance samples
c. Raw wall time for each stream in each iteration
d. Raw wall time for each iteration
e. Aggregated (average) iteration wall time
f. Raw geo mean performance for each iteration
g. Aggregated geo mean performance using the aggregated query performances
"""
import copy
import enum
import functools
import json
import logging
from typing import Any, Dict, Iterable, List, Text

from absl import flags
import numpy as np
from perfkitbenchmarker import sample


flags.DEFINE_bool('edw_generate_aggregated_metrics', True,
                  'Whether the benchmark generates aggregated_metrics such as '
                  'geomean. Query performance metrics are still generated.')

FLAGS = flags.FLAGS


class EdwPerformanceAggregationError(Exception):
  """Error encountered during aggregation of performance results."""


def geometric_mean(iterable: List[float]) -> float:
  """Function to compute the geo mean for a list of numeric values.

  Args:
    iterable: A List of Float performance values

  Returns:
    A float value equal to the geometric mean of the input performance values.

  Raises:
    EdwPerformanceAggregationError: If an invalid performance value was included
      for aggregation.
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
    # TODO(user): add query start and query end as attributes.
    self.name = query_name
    self.performance = performance
    self.execution_status = (EdwQueryExecutionStatus.FAILED
                             if performance == -1.0
                             else EdwQueryExecutionStatus.SUCCESSFUL)
    self.metadata = metadata

  @classmethod
  def from_json(cls, serialized_performance: str):
    """Process the serialized query performance from client jar.

    Expected Performance format:
      {"query_wall_time_in_secs":1.998,"query_end":1601695222108,"query":"1",
      "query_start":1601695220110,
      "details":{"job_id":"b66b5a8e-633f-4ee4-8632-4e3d0856172f"}}

    Args:
      serialized_performance: Stringified json performance.

    Returns:
      An instance of EdwQueryPerformance
    """
    results = json.loads(serialized_performance)
    if 'details' in results:
      metadata = results['details']
    else:
      metadata = {}
    if results['query_wall_time_in_secs'] == -1:
      logging.warning('Query %s failed.', results['query'])
    return cls(query_name=results['query'],
               performance=results['query_wall_time_in_secs'],
               metadata=metadata)

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


class EdwBaseIterationPerformance(object):
  """Class that represents the performance of an iteration of edw queries."""


class EdwPowerIterationPerformance(EdwBaseIterationPerformance):
  """Class that represents the performance of a power iteration of edw queries.

  Attributes:
    id: A unique string id for the iteration.
    performance: A dictionary of query name to its execution performance which
      is a EdwQueryPerformance instance.
    successful_count: An integer count of the successful queries in the
      iteration.
    total_count: An integer count of the total number of queries in the
      iteration.
  """

  def __init__(self, iteration_id: Text, total_queries: int):
    self.id = iteration_id
    self.performance = {}
    self.total_count = total_queries
    self.successful_count = 0

  def add_query_performance(self, query_name: Text, performance: float,
                            metadata: Dict[str, str]):
    """Creates and populates a query performance from the input results.

    Updates the iteration's performance map with the query performance.
    The method also increaments the success and failure query counts for the
    iteration.

    Args:
      query_name: A string name of the query that was executed
      performance: A Float variable set to the query's completion time in secs.
        -1.0 is used as a sentinel value implying the query failed. For a
        successful query the value is expected to be positive.
      metadata: Extra metadata to add to each performance.

    Raises:
      EdwPerformanceAggregationError: If the query has already been added.
    """
    query_metadata = copy.copy(metadata)
    query_performance = EdwQueryPerformance(
        query_name=query_name, performance=performance, metadata=query_metadata)

    if query_performance.name in self.performance:
      raise EdwPerformanceAggregationError('Attempting to aggregate a '
                                           'duplicate query: %s.' %
                                           query_performance.name)
    self.performance[query_performance.name] = query_performance
    if query_performance.is_successful():
      self.successful_count += 1

  def has_query_performance(self, query_name: Text) -> bool:
    """Returns whether the query was run at least once in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was executed in the iteration.
    """
    return query_name in self.performance

  def is_query_successful(self, query_name: Text) -> bool:
    """Returns whether the query was successful in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was successful in the iteration.
    """
    return self.performance.get(query_name).is_successful()

  def get_query_performance(self, query_name: Text) -> float:
    """Gets a query's execution performance generated during iteration execution.

    Args:
      query_name: A String name of the query to retrieve details for

    Returns:
      A float value set to the query's completion time in secs.
    """
    return self.performance[query_name].get_performance_value()

  def get_query_metadata(self, query_name: Text) -> Dict[str, Any]:
    """Gets the metadata of a query as executed in the current iteration.

    Args:
      query_name: Name of the query whose performance is requested.

    Returns:
      A dictionary set to the query's metadata.

    Raises:
      EdwPerformanceAggregationError: If the query failed.
    """
    if not self.is_query_successful(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed'
                                           ' query' + query_name)
    return self.performance.get(query_name).metadata

  def get_all_queries_in_iteration(self) -> List[Text]:
    """Gets a list of names of all queries in the iteration.

    Returns:
      A list of all queries in the iteration.
    """
    return self.performance.keys()

  def get_all_query_performance_samples(
      self, metadata: Dict[str, str]) -> List[sample.Sample]:
    """Gets a list of samples for all queries in the iteration.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples of each query's performance
    """
    return [
        query_performance.get_performance_sample(metadata)
        for query_performance in self.performance.values()
    ]

  def is_successful(self, expected_queries: List[Text]) -> bool:
    """Check if all the expected queries ran and all succeeded."""
    all_queries_ran = set(
        self.get_all_queries_in_iteration()) == set(expected_queries)
    all_queries_were_successful = self.total_count == self.successful_count
    return all_queries_ran and all_queries_were_successful

  def get_queries_geomean(self) -> float:
    """Gets the geometric mean of all queries in the iteration.

    Returns:
      The (float) geometric mean of all the queries ran in the iteration.

    Raises:
      EdwPerformanceAggregationError: If the iteration contains unsuccessful
        query executions.
    """
    return geometric_mean([
        query_performance.performance
        for query_performance in self.performance.values()
    ])

  def get_queries_geomean_performance_sample(
      self, expected_queries: List[Text], metadata: Dict[str,
                                                         str]) -> sample.Sample:
    """Gets a sample for geomean of all queries in the iteration.

    Args:
      expected_queries: A list of query names expected to have been executed in
        an iteration.
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of iteration geomean performance.

    Raises:
      EdwPerformanceAggregationError: If the iteration contains unsuccessful
        query executions.
    """
    if not self.is_successful(expected_queries):
      raise EdwPerformanceAggregationError('Failed executions in iteration.')
    raw_geo_mean = self.get_queries_geomean()
    geo_mean_metadata = copy.copy(metadata)
    return sample.Sample('edw_iteration_geomean_time', raw_geo_mean, 'seconds',
                         geo_mean_metadata)


class EdwSimultaneousIterationPerformance(EdwBaseIterationPerformance):
  """Class that represents the performance of a simultaneous iteration.

  Attributes:
    id: A unique string id for the iteration.
    start_time: The start time of the iteration in milliseconds since epoch.
    end_time: The end time of the iteration in milliseconds since epoch.
    wall_time: The wall time in seconds as a double value.
    performance: A dictionary of query name to its execution performance which
      is an EdwQueryPerformance instance.
    all_queries_succeeded: Whether all queries in the iteration were successful.
  """

  def __init__(self, iteration_id: Text, iteration_start_time: int,
               iteration_end_time: int, iteration_wall_time: float,
               iteration_performance: Dict[str, EdwQueryPerformance],
               all_queries_succeeded: bool):
    self.id = iteration_id
    self.start_time = iteration_start_time
    self.end_time = iteration_end_time
    self.wall_time = iteration_wall_time
    self.performance = iteration_performance
    self.all_queries_succeeded = all_queries_succeeded

  @classmethod
  def from_json(cls, iteration_id: str, serialized_performance: str):
    """Process the serialized simultaneous iteration performance from client jar.

    Expected Performance format:
      {"simultaneous_end":1601145943197,"simultaneous_start":1601145940113,
      "all_queries_performance_array":[{"query_wall_time_in_secs":2.079,
      "query_end":1601145942208,"job_id":"914682d9-4f64-4323-bad2-554267cbbd8d",
      "query":"1","query_start":1601145940129},{"query_wall_time_in_secs":2.572,
      "query_end":1601145943192,"job_id":"efbf93a1-614c-4645-a268-e3801ae994f1",
      "query":"2","query_start":1601145940620}],
      "simultaneous_wall_time_in_secs":3.084}

    Args:
      iteration_id: String identifier of the simultaneous iteration.
      serialized_performance: Stringified json performance.

    Returns:
      An instance of EdwSimultaneousIterationPerformance
    """
    results = json.loads(serialized_performance)
    query_performance_map = {}
    all_queries_succeeded = 'failure_reason' not in results
    if all_queries_succeeded:
      for query_perf_json in results['all_queries_performance_array']:
        query_perf = EdwQueryPerformance.from_json(
            serialized_performance=(json.dumps(query_perf_json)))
        query_performance_map[query_perf.name] = query_perf
    else:
      logging.warning('Failure reported. Reason: %s', results['failure_reason'])
    return cls(
        iteration_id=iteration_id,
        iteration_start_time=(results['simultaneous_start']
                              if all_queries_succeeded else -1),
        iteration_end_time=(results['simultaneous_end']
                            if all_queries_succeeded else -1),
        iteration_wall_time=results['simultaneous_wall_time_in_secs'],
        iteration_performance=query_performance_map,
        all_queries_succeeded=all_queries_succeeded)

  def get_wall_time(self) -> float:
    """Gets the total wall time, in seconds, for the iteration.

    The wall time is the time from the start of the first query to the end time
    of the last query to finish.

    Returns:
      The wall time in seconds.
    """
    return self.wall_time

  def get_wall_time_performance_sample(self, metadata: Dict[
      str, str]) -> sample.Sample:
    """Gets a sample for wall time performance of the iteration.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of iteration wall time performance
    """
    wall_time = self.wall_time
    wall_time_metadata = copy.copy(metadata)
    wall_time_metadata['iteration_start_time'] = self.start_time
    wall_time_metadata['iteration_end_time'] = self.end_time
    return sample.Sample('edw_iteration_wall_time', wall_time, 'seconds',
                         wall_time_metadata)

  def get_all_query_performance_samples(
      self, metadata: Dict[str, str]) -> List[sample.Sample]:
    """Gets a list of samples for all queries in the iteration.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples of each query's performance
    """
    return [
        query_performance.get_performance_sample(metadata)
        for query_performance in self.performance.values()
    ]

  def is_successful(self, expected_queries: List[Text]) -> bool:
    """Check if all the expected queries ran and all succeeded."""
    all_queries_ran = self.performance.keys() == set(expected_queries)
    return all_queries_ran and self.all_queries_succeeded

  def has_query_performance(self, query_name: Text) -> bool:
    """Returns whether the query was run at least once in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was executed in the iteration.
    """
    return query_name in self.performance

  def is_query_successful(self, query_name: Text) -> bool:
    """Returns whether the query was successful in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was successful in the iteration.
    """
    if self.has_query_performance(query_name):
      return self.performance.get(query_name).is_successful()
    return False

  def get_query_performance(self, query_name: Text) -> float:
    """Gets a query's execution performance in the current iteration.

    Args:
      query_name: A String name of the query to retrieve details for

    Returns:
      A float value set to the query's completion time in secs.
    """
    return self.performance[query_name].get_performance_value()

  def get_query_metadata(self, query_name: Text) -> Dict[str, Any]:
    """Gets the metadata of a query in the current iteration.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A dictionary set to the query's aggregated metadata, accumulated from the
       raw query run in the current iteration.

    Raises:
      EdwPerformanceAggregationError: If the query failed in the iteration.
    """
    if not self.is_query_successful(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed'
                                           ' query' + query_name)
    return self.performance.get(query_name).metadata

  def get_queries_geomean(self) -> float:
    """Gets the geometric mean of all queries in the iteration.

    Returns:
      The (float) geometric mean of all the queries ran in the iteration.

    Raises:
      EdwPerformanceAggregationError: If the iteration contains unsuccessful
        query executions.
    """
    return geometric_mean([
        query_performance.performance
        for query_performance in self.performance.values()
    ])

  def get_queries_geomean_performance_sample(
      self, expected_queries: List[Text], metadata: Dict[str,
                                                         str]) -> sample.Sample:
    """Gets a sample for geomean of all queries in the iteration.

    Args:
      expected_queries: A list of query names expected to have been executed in
        an iteration.
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of iteration geomean performance.

    Raises:
      EdwPerformanceAggregationError: If the iteration contains unsuccessful
        query executions.
    """
    if not self.is_successful(expected_queries):
      raise EdwPerformanceAggregationError('Failed executions in iteration.')
    raw_geo_mean = self.get_queries_geomean()
    geo_mean_metadata = copy.copy(metadata)
    return sample.Sample('edw_iteration_geomean_time', raw_geo_mean, 'seconds',
                         geo_mean_metadata)


class EdwThroughputIterationPerformance(EdwBaseIterationPerformance):
  """Class that represents the performance of an iteration of edw queries.

  Attributes:
    id: A unique string id for the iteration.
    start_time: The start time of the iteration execution.
    end_time: The end time of the iteration execution.
    wall_time: The wall time of the stream execution.
    performance: A dict of stream_id to stream performances, each of which is a
      dictionary mapping query names to their execution performances, which are
      EdwQueryPerformance instances.
  """

  def __init__(self, iteration_id: Text, iteration_start_time: int,
               iteration_end_time: int, iteration_wall_time: float,
               iteration_performance: Dict[str, Dict[str,
                                                     EdwQueryPerformance]]):
    self.id = iteration_id
    self.start_time = iteration_start_time
    self.end_time = iteration_end_time
    self.wall_time = iteration_wall_time
    self.performance = iteration_performance

  @classmethod
  def from_json(cls, iteration_id: str, serialized_performance: str):
    """Process the serialized throughput iteration performance from client jar.

    Expected Performance format:
      {"throughput_start":1601666911596,"throughput_end":1601666916139,
        "throughput_wall_time_in_secs":4.543,
        "all_streams_performance_array":[
          {"stream_start":1601666911597,"stream_end":1601666916139,
            "stream_wall_time_in_secs":4.542,
            "stream_performance_array":[
              {"query_wall_time_in_secs":2.238,"query_end":1601666913849,
                "query":"1","query_start":1601666911611,
                "details":{"job_id":"438170b0-b0cb-4185-b733-94dd05b46b05"}},
              {"query_wall_time_in_secs":2.285,"query_end":1601666916139,
                "query":"2","query_start":1601666913854,
                "details":{"job_id":"371902c7-5964-46f6-9f90-1dd00137d0c8"}}
              ]},
          {"stream_start":1601666911597,"stream_end":1601666916018,
            "stream_wall_time_in_secs":4.421,
            "stream_performance_array":[
              {"query_wall_time_in_secs":2.552,"query_end":1601666914163,
                "query":"2","query_start":1601666911611,
                "details":{"job_id":"5dcba418-d1a2-4a73-be70-acc20c1f03e6"}},
              {"query_wall_time_in_secs":1.855,"query_end":1601666916018,
                "query":"1","query_start":1601666914163,
                "details":{"job_id":"568c4526-ae26-4e9d-842c-03459c3a216d"}}
            ]}
        ]}

    Args:
      iteration_id: String identifier of the throughput iteration.
      serialized_performance: Stringified json performance.

    Returns:
      An instance of EdwThroughputIterationPerformance
    """
    results = json.loads(serialized_performance)
    stream_performances = {}
    all_queries_succeeded = 'failure_reason' not in results
    if all_queries_succeeded:
      for stream_id, stream_perf_json in enumerate(
          results['all_streams_performance_array']):
        stream_id = str(stream_id)
        stream_performance_map = {}
        for query_perf_json in stream_perf_json['stream_performance_array']:
          query_perf = EdwQueryPerformance.from_json(
              serialized_performance=(json.dumps(query_perf_json)))
          stream_performance_map[query_perf.name] = query_perf
        stream_performances.update({stream_id: stream_performance_map})
    else:
      logging.warning('Failure reported. Reason: %s', results['failure_reason'])
    return cls(
        iteration_id=iteration_id,
        iteration_start_time=(results['throughput_start']
                              if all_queries_succeeded else -1),
        iteration_end_time=(results['throughput_end']
                            if all_queries_succeeded else -1),
        iteration_wall_time=results['throughput_wall_time_in_secs'],
        iteration_performance=stream_performances)

  def has_query_performance(self, query_name: Text) -> bool:
    """Returns whether the query was run at least once in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was executed in the iteration.
    """
    for stream in self.performance.values():
      if query_name in stream:
        return True
    return False

  def is_query_successful(self, query_name: Text) -> bool:
    """Returns whether the query was successful in the iteration.

    Args:
      query_name: A String name of the query to check.

    Returns:
      A boolean value indicating if the query was successful in the iteration.
    """
    for stream in self.performance.values():
      if query_name in stream:
        if not stream[query_name].is_successful():
          return False
    return True

  def get_query_performance(self, query_name: Text) -> float:
    """Gets a query's execution performance aggregated across all streams in the current iteration.

    Args:
      query_name: A String name of the query to retrieve details for

    Returns:
      A float value set to the query's average completion time in secs.
    """
    all_performances = []
    for stream in self.performance.values():
      if query_name in stream:
        all_performances.append(stream[query_name].get_performance_value())
    if not all_performances:
      return -1.0
    return sum(all_performances) / len(all_performances)

  def get_query_metadata(self, query_name: Text) -> Dict[str, Any]:
    """Gets the metadata of a query aggregated across all streams in the current iteration.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A dictionary set to the query's aggregated metadata, accumulated from the
       raw query runs in all streams of the current iteration.

    Raises:
      EdwPerformanceAggregationError: If the query failed in one or more streams
    """
    result = {}
    for stream_id, stream_performance in self.performance.items():
      if query_name in stream_performance:
        q_performance = stream_performance[query_name]
        result[stream_id + '_runtime'] = q_performance.get_performance_value()
        result.update({
            stream_id + '_' + k: v
            for (k, v) in q_performance.get_performance_metadata().items()
        })
    return result

  def get_all_query_performance_samples(
      self, metadata: Dict[str, str]) -> List[sample.Sample]:
    """Gets a list of samples for all queries in all streams of the iteration.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples of each query's performance
    """
    all_query_performances = []
    for stream_id, stream_performance in self.performance.items():
      stream_metadata = copy.copy(metadata)
      stream_metadata['stream'] = stream_id
      all_query_performances.extend([
          query_perf.get_performance_sample(stream_metadata)
          for query_perf in stream_performance.values()
      ])
    return all_query_performances

  def all_streams_ran_all_expected_queries(
      self, expected_queries: List[Text]) -> bool:
    """Checks that the same set of expected queries ran in all streams."""
    for stream in self.performance.values():
      if set(stream.keys()) != set(expected_queries):
        return False
    return True

  def no_duplicate_queries(self) -> bool:
    """Checks that no streams contain any duplicate queries."""
    for stream in self.performance.values():
      if len(stream.keys()) != len(set(stream.keys())):
        return False
    return True

  def all_queries_succeeded(self) -> bool:
    """Checks if every query in every stream was successful."""
    for stream_performance in self.performance.values():
      for query_perf in stream_performance.values():
        if query_perf.performance == -1:
          return False
    return True

  def is_successful(self, expected_queries: List[Text]) -> bool:
    """Check if the throughput run was successful.

    A successful run meets the following conditions:
    - There were more than 0 streams.
    - Each stream ran the same set of expected queries (regardless of order)
    - Each stream ran each query only once
    - Every query in every stream succeeded

    Args:
      expected_queries: A list of query names expected to have been executed in
        an iteration.

    Returns:
      True if all success conditions were met, false otherwise.
    """
    non_zero_streams = len(self.performance) >= 1
    all_streams_ran_all_queries = self.all_streams_ran_all_expected_queries(
        expected_queries)
    no_duplicate_queries = self.no_duplicate_queries()
    all_queries_succeeded = self.all_queries_succeeded()
    return (non_zero_streams and all_streams_ran_all_queries and
            no_duplicate_queries and all_queries_succeeded)

  def get_queries_geomean(self) -> float:
    """Gets the geometric mean of all queries in all streams of the iteration.

    Returns:
      The (float) geometric mean of all the individual queries ran in all
        streams of the iteration.

    Raises:
      EdwPerformanceAggregationError: If the suite contains unsuccessful query
        executions.
    """
    query_performances = []
    for stream in self.performance.values():
      for query in stream.values():
        query_performances.append(query.get_performance_value())
    return geometric_mean(query_performances)

  def get_queries_geomean_performance_sample(
      self, expected_queries: List[Text], metadata: Dict[str,
                                                         str]) -> sample.Sample:
    """Gets a sample for geomean of all queries in all streams of the iteration.

    Args:
      expected_queries: A list of query names expected to have been executed in
        an iteration.
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of iteration geomean performance.

    Raises:
      EdwPerformanceAggregationError: If the iteration contains unsuccessful
        query executions.
    """
    if not self.is_successful(expected_queries):
      raise EdwPerformanceAggregationError('Failed executions in iteration.')
    raw_geo_mean = self.get_queries_geomean()
    geo_mean_metadata = copy.copy(metadata)
    return sample.Sample('edw_iteration_geomean_time', raw_geo_mean, 'seconds',
                         geo_mean_metadata)

  def get_wall_time(self) -> float:
    """Gets the total wall time, in seconds, for the iteration.

    The wall time is the time from the start of the first stream to the end time
    of the last stream to finish.

    Returns:
      The wall time in seconds.
    """
    return self.wall_time

  def get_wall_time_performance_sample(
      self, metadata: Dict[str, str]) -> sample.Sample:
    """Gets a sample for total wall time performance of the iteration.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of iteration wall time performance
    """
    wall_time_metadata = copy.copy(metadata)
    wall_time_metadata['iteration_start_time'] = self.start_time
    wall_time_metadata['iteration_end_time'] = self.end_time
    return sample.Sample('edw_iteration_wall_time', self.wall_time, 'seconds',
                         wall_time_metadata)


class EdwBenchmarkPerformance(object):
  """Class that represents the performance of an edw benchmark.

  Attributes:
    total_iterations: An integer variable set to total of number of iterations.
    expected_queries: A list of query names that are executed in an iteration of
      the benchmark
    iteration_performances: A dictionary of iteration id (String value) to its
      execution performance (an instance of EdwBaseIterationPerformance)
  """

  def __init__(self, total_iterations: int, expected_queries: Iterable[Text]):
    self.total_iterations = total_iterations
    self.expected_queries = list(expected_queries)
    self.iteration_performances = {}

  def add_iteration_performance(self, performance: EdwBaseIterationPerformance):
    """Add an iteration's performance to the benchmark results.

    Args:
      performance: An instance of EdwBaseIterationPerformance encapsulating the
        iteration performance details.

    Raises:
      EdwPerformanceAggregationError: If the iteration has already been added.
    """
    iteration_id = performance.id
    if iteration_id in self.iteration_performances:
      raise EdwPerformanceAggregationError('Attempting to aggregate a duplicate'
                                           ' iteration: %s.' % iteration_id)
    self.iteration_performances[iteration_id] = performance

  def is_successful(self) -> bool:
    """Check a benchmark's success, only if all the iterations succeed."""
    return functools.reduce((lambda x, y: x and y), [
        iteration_performance.is_successful(self.expected_queries)
        for iteration_performance in self.iteration_performances.values()
    ])

  def aggregated_query_status(self, query_name: Text) -> bool:
    """Gets the status of query aggregated across all iterations.

    A query is considered successful only if
      a. Query was executed in every iteration
      b. Query was successful in every iteration

    Args:
      query_name: Name of the query whose aggregated success is requested

    Returns:
      A boolean value indicating if the query was successful in the benchmark.
    """
    for performance in self.iteration_performances.values():
      if not performance.has_query_performance(query_name):
        return False
      if not performance.is_query_successful(query_name):
        return False
    return True

  def aggregated_query_execution_time(self, query_name: Text) -> float:
    """Gets the execution time of query aggregated across all iterations.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A float value set to the query's aggregated execution time

    Raises:
      EdwPerformanceAggregationError: If the query failed in one or more
      iterations
    """
    if not self.aggregated_query_status(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed '
                                           'query ' + query_name)
    query_performances = [
        iteration_performance.get_query_performance(query_name)
        for iteration_performance in self.iteration_performances.values()
    ]
    return sum(query_performances) / self.total_iterations

  def aggregated_query_metadata(self, query_name: Text) -> Dict[str, Any]:
    """Gets the metadata of a query aggregated across all iterations.

    Args:
      query_name: Name of the query whose aggregated performance is requested

    Returns:
      A dictionary set to the query's aggregated metadata, accumulated from the
       raw query runs.

    Raises:
      EdwPerformanceAggregationError: If the query failed in one or more
      iterations
    """
    if not self.aggregated_query_status(query_name):
      raise EdwPerformanceAggregationError('Cannot aggregate invalid / failed '
                                           'query ' + query_name)
    result = {}
    for iteration_id, iteration_performance in (
        self.iteration_performances.items()):
      result.update({
          iteration_id + '_' + k: v
          for (k, v) in iteration_performance.get_query_metadata(
              query_name).items()
      })
    return result

  def get_aggregated_query_performance_sample(
      self, query_name: Text, metadata: Dict[str, str]) -> sample.Sample:
    """Gets the performance of query aggregated across all iterations.

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

    Benchmark relies on iteration runs to generate the raw query performance
      samples
    Benchmark appends the aggregated query performance sample

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples (raw and aggregated)
    """
    results = []
    # Raw query performance samples
    for iteration, performance in self.iteration_performances.items():
      iteration_metadata = copy.copy(metadata)
      iteration_metadata['iteration'] = iteration
      results.extend(performance.get_all_query_performance_samples(
          iteration_metadata))
    # Aggregated query performance samples
    for query in self.expected_queries:
      results.append(self.get_aggregated_query_performance_sample(
          query_name=query, metadata=metadata))
    return results

  def get_aggregated_wall_time_performance_sample(self,
                                                  metadata: Dict[str, str]
                                                  ) -> sample.Sample:
    """Gets the wall time performance aggregated across all iterations.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of aggregated (averaged) wall time.
    """
    wall_times = [
        iteration.get_wall_time()
        for iteration in self.iteration_performances.values()
    ]
    aggregated_wall_time = sum(wall_times) / self.total_iterations
    wall_time_metadata = copy.copy(metadata)
    wall_time_metadata['aggregation_method'] = 'mean'
    return sample.Sample('edw_aggregated_wall_time', aggregated_wall_time,
                         'seconds', wall_time_metadata)

  def get_wall_time_performance_samples(self, metadata: Dict[str, str]):
    """Generates samples for all wall time performances.

    Benchmark relies on iterations to generate the raw wall time performance
      samples.
    Benchmark appends the aggregated wall time performance sample

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A list of samples (raw and aggregated)
    """
    results = []

    for iteration, performance in self.iteration_performances.items():
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
    """Gets the geomean performance aggregated across all iterations.

    Args:
      metadata: A dictionary of execution attributes to be merged with the query
        execution attributes, for eg. tpc suite, scale of dataset, etc.

    Returns:
      A sample of aggregated geomean

    Raises:
      EdwPerformanceAggregationError: If the benchmark conatins a failed query
        execution.
    """
    if not self.is_successful():
      raise EdwPerformanceAggregationError('Benchmark contains a failed query.')
    aggregated_geo_mean = geometric_mean([
        self.aggregated_query_execution_time(query_name=query)
        for query in self.expected_queries
    ])

    geomean_metadata = copy.copy(metadata)
    geomean_metadata['intra_query_aggregation_method'] = 'mean'
    geomean_metadata['inter_query_aggregation_method'] = 'geomean'
    return sample.Sample('edw_aggregated_geomean', aggregated_geo_mean,
                         'seconds', geomean_metadata)

  def get_queries_geomean_performance_samples(self, metadata: Dict[str, str]
                                              ) -> List[sample.Sample]:
    """Generates samples for all geomean performances.

    Benchmark relies on iteration runs to generate the raw geomean performance
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

    for iteration, performance in self.iteration_performances.items():
      iteration_metadata = copy.copy(metadata)
      iteration_metadata['iteration'] = iteration
      results.append(
          performance.get_queries_geomean_performance_sample(
              self.expected_queries, iteration_metadata))

    results.append(self.get_aggregated_geomean_performance_sample(
        metadata=metadata))
    return results
