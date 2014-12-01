# Copyright 2014 Google Inc. All rights reserved.
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

"""Class that publishes data collected from PerfKitBenchmarker.

Contains methods to collect data, write it to a file, and export the data
to Perfkit.
"""

import json
import os
import time
import uuid

import gflags as flags
import logging

from perfkitbenchmarker import perfkitbenchmarker_lib
from perfkitbenchmarker import version
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'official',
    False,
    'A boolean indicating whether results are official or not. The '
    'default is False. Official test results are treated and queried '
    'differently from non-official test results.')
flags.DEFINE_string(
    'bigquery_table',
    None,
    'The bigquery table to publish results to. This should be of the form '
    '"dataset_name.table_name".')
flags.DEFINE_list(
    'metadata',
    [],
    'A list of key-value pairs that will be added to the labels field of all '
    'samples as metadata. Each key-value pair in the list should be colon '
    'separated.')

BIGQUERY = 'bq'


def GetLabelsFromDict(metadata):
  """Converts a metadata dictionary to a string of labels.

  Args:
    metadata: a dictionary of string key value pairs.

  Returns:
    A string of labels in the format that Perfkit uses.
  """
  labels = []
  for k, v in metadata.iteritems():
    labels.append('|%s:%s|' % (k, v))
  return ','.join(labels)


def AddStandardMetadata(metadata, benchmark_spec):
  """Adds metadata standard across all benchmarks.

  Args:
    metadata: The metadata dictionary to be modified.
    benchmark_spec: A BenchmarkSpec instance that describes the benchmark.
  """
  metadata['perfkitbenchmarker_version'] = version.VERSION
  metadata['cloud'] = benchmark_spec.cloud
  metadata['zones'] = benchmark_spec.zones
  metadata['machine_type'] = benchmark_spec.machine_type
  metadata['image'] = benchmark_spec.image
  for pair in FLAGS.metadata:
    try:
      key, value = pair.split(':')
      metadata[key] = value
    except ValueError:
      logging.error('Bad metadata flag format. Skipping "%s".', pair)
      continue


class PerfKitBenchmarkerPublisher(object):
  """Class to publish performance benchmark results and their configurations."""

  def __init__(self, json_file=None):
    """Initialize an PerfKitBenchmarkerPublisher class.

    Args:
      json_file: (optional) path to existing json file to be published.
    """
    if json_file:
      self.json_file = json_file
    else:
      self.json_file = vm_util.PrependTempDir('perfkitbenchmarker.json')
    self.samples = []
    self.run_uri = str(uuid.uuid4())

  # TODO(user) Add a Sample class.
  def AddSamples(self, samples, benchmark, benchmark_spec):
    """Adds data sample to the perfkitbenchmarker publisher.

    Args:
      samples: a list of 3or4-tuples. The tuples contain the metric
          name (string), the value (float), and unit (string) of
          each sample. If a 4th element is included, it is a
          dictionary of metadata associated with the sample.
      benchmark: string containing the name of the benchmark.
      benchmark_spec: BenchmarkSpec object of the benchmark.
    """
    for s in samples:
      sample = dict()
      sample['test'] = benchmark
      sample['metric'] = s[0]
      sample['value'] = s[1]
      sample['unit'] = s[2]
      if len(s) == 4:
        metadata = s[3]
      else:
        metadata = dict()
      AddStandardMetadata(metadata, benchmark_spec)
      sample['labels'] = GetLabelsFromDict(metadata)
      sample['product_name'] = 'PerfKitBenchmarker'
      sample['official'] = FLAGS.official
      sample['owner'] = FLAGS.owner
      sample['timestamp'] = time.time()
      sample['run_uri'] = self.run_uri
      sample['sample_uri'] = str(uuid.uuid4())
      self.samples.append(sample)

  def DumpData(self):
    """Dumps all information from the samples the publisher has collected."""
    data = [
        '\n' + '-' * 25 + 'PerfKitBenchmarker Complete Results' + '-' * 25 +
        '\n']
    for sample in self.samples:
      data.append('%s\n' % sample)
    logging.info(''.join(data))

  def PrettyPrintData(self):
    """Pretty prints the samples the publisher has collected."""
    self.samples.sort(key=lambda k: k['test'])
    benchmark = None
    data = [
        '\n' + '-' * 25 + 'PerfKitBenchmarker Results Summary' + '-' * 25 +
        '\n']
    for sample in self.samples:
      if sample['test'] != benchmark:
        benchmark = sample['test']
        data.append('%s:\n' % benchmark.upper())
      data.append('\t%s %s %s\n' %
                  (sample['metric'], sample['value'], sample['unit']))
    logging.info(''.join(data))

  def WriteFile(self):
    """Writes data collected to a json format that can be exported."""
    f = open(self.json_file, 'a')
    encoder = json.JSONEncoder()
    for sample in self.samples:
      line = '%s\n' % encoder.encode(sample)
      f.write(line)
    f.close()

  def DeleteFile(self):
    """Deletes the json results file."""
    try:
      os.remove(self.json_file)
    except OSError:
      pass  # File doesn't exist.

  def PublishData(self, delete_file=True):
    """Writes data to perfkit. The file must have been written already.

    Args:
      delete_file: A boolean indicating whether to delete the json file.
          The file will only be deleted if the upload suceeded and this is
          set to True.
    """
    if FLAGS.bigquery_table:
      load_cmd = [BIGQUERY,
                  'load',
                  '--source_format=NEWLINE_DELIMITED_JSON',
                  FLAGS.bigquery_table,
                  self.json_file]
      perfkitbenchmarker_lib.IssueRetryableCommand(load_cmd)

    if delete_file:
      self.DeleteFile()
