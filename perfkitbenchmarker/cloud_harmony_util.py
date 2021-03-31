"""Module for Helper methods when working with Cloud Harmony Suite.

https://github.com/cloudharmony
"""

import io
from typing import Any, Dict, List

from absl import flags
import pandas as pd
from perfkitbenchmarker import providers
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.providers.gcp import util as gcp_util

FLAGS = flags.FLAGS


def GetRegionFromZone(zone: str) -> str:
  # only gcp is supported as cloudharmony metadata is exclusive to gcp runs.
  if FLAGS.cloud == 'GCP':
    return gcp_util.GetRegionFromZone(zone)
  else:
    return zone


def ParseCsvResultsIntoMetadata(vm: virtual_machine.BaseVirtualMachine,
                                path: str) -> List[Dict[str, Any]]:
  """Loads the CSV created by cloud harmony at path in the VM into metadata.

  The CSV located by path inside of virtual machine VM will be loaded. For each
  row of results, a set of key/value pairs is created. The keys will all be
  prepended with `cloudharmony` or similar.

  Args:
     vm: the Virtual Machine that has run a cloud harmony benchmark
     path: The path inside of VM which has the CSV file which should be loaded
  Returns:
     A list of metadata outputs that should be appended to the samples that are
     produced by a cloud harmony benchmark.
  """
  csv_string, _ = vm.RemoteCommand('cat {path}'.format(path=path))

  return ParseCsvResultsFromString(csv_string)


def ParseCsvResultsFromString(csv_string: str,
                              prefix: str = '') -> List[Dict[str, Any]]:
  """Loads the CSV created by cloud harmony in csv_string.

  The CSV will be loaded into a pandas data frame.
  For every row of results - we will create a set of key/value pairs
  representing that row of results.  The keys will all be prepended with
  prefix.

  Args:
     csv_string:  a string of the CSV which was produced by cloud_harmony
     prefix: a string prefix to attach to the metadata. Defaults to empty
     string. It can be set to a unique string if cloudharmony data is
     attached to every sample instead of being its own sample.
  Returns:
     A list of metadata dictionaries, where each dict represents one row of
     results (an iteration) in the csv string.
  """
  data_frame = pd.read_csv(io.StringIO(csv_string)).fillna('')
  number_of_rows = len(data_frame.index)

  results = []
  # one row = one test result
  for row in range(number_of_rows):
    result = {}
    for column in data_frame.columns:
      key = column
      value = data_frame[column][row]
      result_key = f'{prefix}_{key}' if prefix else key
      result[result_key] = value
    results.append(result)

  return results


def GetCommonMetadata(custom_metadata: Dict[str, Any] = None) -> str:
  """Returns pkb metadata associated with this run as cloudharmony metadata.

  Cloudharmony benchmarks take in benchmark setup configurations as inputs and
  include them in the output as metadata for the run. This function creates a
  string of input metadata from pkb flags to be included as run parameter for
  cloudharmony benchmarks.

  Args:
     custom_metadata: a dictionary of metadata key value pairs that should
     override any flag chosen in the function, or should also be included.
  Returns:
     A string of metadata that should be appended to the cloudharmony
     benchmark run.
  """
  if FLAGS.cloud != providers.GCP:
    # Should not be including cloudharmony metadata for non-gcp runs.
    return ''

  metadata = {
      'meta_compute_service': 'Google Compute Engine',
      'meta_compute_service_id': 'google:compute',
      'meta_instance_id': FLAGS.machine_type,
      'meta_provider': 'Google Cloud Platform',
      'meta_provider_id': 'google',
      'meta_region': gcp_util.GetRegionFromZone(FLAGS.zone[0]),
      'meta_zone': FLAGS.zone[0],
      'meta_test_id': FLAGS.run_uri,
  }
  if custom_metadata:
    metadata.update(custom_metadata)

  metadata_pair = [f'--{key} {value}' for key, value in metadata.items()]
  return ' '.join(metadata_pair)


def GetMetadataSamples(
    cloud_harmony_metadata: List[Dict[Any, Any]]) -> List[sample.Sample]:
  """Returns the cloudharmony metadata as a list of samples.

  This function is commonly used across all cloudharmony benchmarks.

  Args:
    cloud_harmony_metadata: list of metadata outputs to save in samples.

  Returns:
    A list of sample.Sample objects of cloudharmony metadata, where one sample
    represents one row of csv results (one row = one test iteration).

  """
  samples = []
  for result in cloud_harmony_metadata:
    samples.append(sample.Sample('cloudharmony_output', '', '', result))
  return samples
