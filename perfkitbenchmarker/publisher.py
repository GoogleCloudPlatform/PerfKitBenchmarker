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

"""Classes to collect and publish performance samples to various sinks."""

import abc
import itertools
import json
import logging
import operator
import sys
import tempfile
import time
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import version
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_boolean(
    'official',
    False,
    'A boolean indicating whether results are official or not. The '
    'default is False. Official test results are treated and queried '
    'differently from non-official test results.')

flags.DEFINE_boolean(
    'json_output',
    True,
    'A boolean indicating whether to write newline-delimited '
    'JSON results to the run-specific temporary directory.')

flags.DEFINE_string(
    'json_path',
    None,
    'A path to write newline-delimited JSON results')

flags.DEFINE_string(
    'bigquery_table',
    None,
    'The BigQuery table to publish results to. This should be of the form '
    '"[project_id:]dataset_name.table_name".')
flags.DEFINE_string(
    'bq_path', 'bq', 'Path to the "bq" executable.')
flags.DEFINE_string(
    'bq_project', None, 'Project to use for authenticating with BigQuery.')
flags.DEFINE_string(
    'service_account', None, 'Service account to use to authenticate with BQ.')
flags.DEFINE_string(
    'service_account_private_key', None,
    'Service private key for authenticating with BQ.')

flags.DEFINE_string(
    'gsutil_path', 'gsutil', 'path to the "gsutil" executable')
flags.DEFINE_string(
    'cloud_storage_bucket',
    None,
    'GCS bucket to upload records to. Bucket must exist.')

flags.DEFINE_list(
    'metadata',
    [],
    'A list of key-value pairs that will be added to the labels field of all '
    'samples as metadata. Each key-value pair in the list should be colon '
    'separated.')

PRODUCT_NAME = 'PerfKitBenchmarker'
DEFAULT_JSON_OUTPUT_NAME = 'perfkitbenchmarker_results.json'
DEFAULT_CREDENTIALS_JSON = 'credentials.json'
GCS_OBJECT_NAME_LENGTH = 20


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


class MetadataProvider(object):
  """A provider of sample metadata."""

  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def AddMetadata(self, metadata, benchmark_spec):
    """Add metadata to a dictionary.

    Existing values will be overwritten.

    Args:
      metadata: dict. Dictionary of metadata to update.
      benchmark_spec: BenchmarkSpec. The benchmark specification.

    Returns:
      Updated 'metadata'.
    """
    raise NotImplementedError()


class DefaultMetadataProvider(MetadataProvider):
  """Adds default metadata to samples."""

  def AddMetadata(self, metadata, benchmark_spec):
    metadata = metadata.copy()
    metadata['perfkitbenchmarker_version'] = version.VERSION
    metadata['cloud'] = benchmark_spec.cloud
    metadata['zones'] = ','.join(benchmark_spec.zones)
    metadata['machine_type'] = benchmark_spec.machine_type
    metadata['image'] = benchmark_spec.image
    for pair in FLAGS.metadata:
      try:
        key, value = pair.split(':')
        metadata[key] = value
      except ValueError:
          logging.error('Bad metadata flag format. Skipping "%s".', pair)
          continue

    return metadata


DEFAULT_METADATA_PROVIDERS = [DefaultMetadataProvider()]


class SamplePublisher(object):
  """An object that can publish performance samples."""

  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def PublishSamples(self, samples):
    """Publishes 'samples'.

    PublishSamples will be called exactly once. Calling
    SamplePublisher.PublishSamples multiple times may result in data being
    overwritten.

    Args:
      samples: list of dicts to publish.
    """
    raise NotImplementedError()


class PrettyPrintStreamPublisher(SamplePublisher):
  """Writes samples to an output stream, defaulting to stdout.

  Samples are pretty-printed and summarized. Example output:

    -------------------------PerfKitBenchmarker Results Summary----------------
    NETPERF_SIMPLE:
            TCP_RR_Transaction_Rate 714.72 transactions_per_second
            TCP_RR_Transaction_Rate 2950.92 transactions_per_second
            TCP_CRR_Transaction_Rate 201.6 transactions_per_second
            TCP_CRR_Transaction_Rate 793.59 transactions_per_second
            TCP_STREAM_Throughput 283.25 Mbits/sec
            TCP_STREAM_Throughput 989.06 Mbits/sec
            UDP_RR_Transaction_Rate 107.9 transactions_per_second
            UDP_RR_Transaction_Rate 3018.51 transactions_per_second
            End to End Runtime 471.35810709 seconds

  Attributes:
    stream: File-like object. Output stream to print samples.
  """
  def __init__(self, stream=None):
    self.stream = stream or sys.stdout

  def __repr__(self):
    return '<{0} stream={1}>'.format(type(self).__name__, self.stream)

  def PublishSamples(self, samples):
    key = operator.itemgetter('test')
    samples = sorted(samples, key=key)
    data = [
        '\n' + '-' * 25 + 'PerfKitBenchmarker Results Summary' + '-' * 25 +
        '\n']
    for benchmark, test_samples in itertools.groupby(samples, key):
      data.append('%s:\n' % benchmark.upper())
      for sample in test_samples:
        data.append('\t%s %s %s\n' %
                    (sample['metric'], sample['value'], sample['unit']))
    data.append('\n')
    self.stream.write(''.join(data))


class LogPublisher(SamplePublisher):
  """Writes samples to a Python Logger.

  Attributes:
    level: Logging level. Defaults to logging.INFO.
    logger: Logger to publish to. Defaults to the root logger.
  """

  def __init__(self, level=logging.INFO, logger=None):
    self.level = level
    self.logger = logger or logging.getLogger()

  def __repr__(self):
    return '<{0} logger={1} level={2}>'.format(type(self).__name__, self.logger,
                                               self.level)

  def PublishSamples(self, samples):
    data = [
        '\n' + '-' * 25 + 'PerfKitBenchmarker Complete Results' + '-' * 25 +
        '\n']
    for sample in samples:
      data.append('%s\n' % sample)
    self.logger.log(self.level, ''.join(data))


# TODO: Extract a function to write delimited JSON to a stream.
class NewlineDelimitedJSONPublisher(SamplePublisher):
  """Publishes samples to a file as newline delimited JSON.

  The resulting output file is compatible with 'bq load' using
  format NEWLINE_DELIMITED_JSON.

  Metadata is converted to a flat string with key 'labels' via
  GetLabelsFromDict.

  Attributes:
    file_path: string. Destination path to write samples.
    mode: Open mode for 'file_path'. Set to 'a' to append.
  """

  def __init__(self, file_path, mode='wb'):
    self.file_path = file_path
    self.mode = mode

  def __repr__(self):
    return '<{0} file_path="{1}" mode="{2}">'.format(
        type(self).__name__, self.file_path, self.mode)

  def PublishSamples(self, samples):
    logging.info('Publishing %d samples to %s', len(samples),
                 self.file_path)
    with open(self.file_path, self.mode) as fp:
      for sample in samples:
        sample = sample.copy()
        sample['labels'] = GetLabelsFromDict(sample.pop('metadata', {}))
        fp.write(json.dumps(sample) + '\n')


class BigQueryPublisher(SamplePublisher):
  """Publishes samples to BigQuery.

  Attributes:
    bigquery_table: string. The bigquery table to publish to, of the form
      '[project_name:]dataset_name.table_name'
    project_id: string. Project to use for authenticating with BigQuery.
    bq_path: string. Path to the 'bq' executable'.
    service_account: string. Use this service account email address for
      authorization. For example, 1234567890@developer.gserviceaccount.com
    service_account_private_key: Filename that contains the service account
      private key. Must be specified if service_account is specified.
  """

  def __init__(self, bigquery_table, project_id=None, bq_path='bq',
               service_account=None, service_account_private_key_file=None):
    self.bigquery_table = bigquery_table
    self.project_id = project_id
    self.bq_path = bq_path
    self.service_account = service_account
    self.service_account_private_key_file = service_account_private_key_file
    self._credentials_file = vm_util.PrependTempDir(DEFAULT_CREDENTIALS_JSON)

    if ((self.service_account is None) !=
        (self.service_account_private_key_file is None)):
      raise ValueError('service_account and service_account_private_key '
                       'must be specified together.')

  def __repr__(self):
    return '<{0} table="{1}">'.format(type(self).__name__, self.bigquery_table)

  def PublishSamples(self, samples):
    if not samples:
      logging.warn('No samples: not publishing to BigQuery')
      return

    with tempfile.NamedTemporaryFile(prefix='perfkit-bq-pub',
                                     dir=vm_util.GetTempDir(),
                                     suffix='.json') as tf:
      json_publisher = NewlineDelimitedJSONPublisher(tf.name)
      json_publisher.PublishSamples(samples)
      logging.info('Publishing %d samples to %s', len(samples),
                   self.bigquery_table)
      load_cmd = [self.bq_path]
      if self.project_id:
        load_cmd.append('--project_id=' + self.project_id)
      if self.service_account:
        assert self.service_account_private_key_file is not None
        load_cmd.extend(['--service_account=' + self.service_account,
                         '--service_account_credential_file=' +
                         self._credentials_file,
                         '--service_account_private_key_file=' +
                         self.service_account_private_key_file])
      load_cmd.extend(['load',
                       '--source_format=NEWLINE_DELIMITED_JSON',
                       self.bigquery_table,
                       tf.name])
      vm_util.IssueRetryableCommand(load_cmd)


class CloudStoragePublisher(SamplePublisher):
  """Publishes samples to a Google Cloud Storage bucket using gsutil.

  Samples are formatted using a NewlineDelimitedJSONPublisher, and written to a
  the destination file within the specified bucket named:

    <time>_<uri>

  where <time> is the number of milliseconds since the Epoch, and <uri> is a
  random UUID.

  Attributes:
    bucket: string. The GCS bucket name to publish to.
    gsutil_path: string. The path to the 'gsutil' tool.
  """

  def __init__(self, bucket, gsutil_path='gsutil'):
    self.bucket = bucket
    self.gsutil_path = gsutil_path

  def __repr__(self):
    return '<{0} bucket="{1}">'.format(type(self).__name__, self.bucket)

  def _GenerateObjectName(self):
      object_name = str(int(time.time() * 100)) + '_' + str(uuid.uuid4())
      return object_name[:GCS_OBJECT_NAME_LENGTH]

  def PublishSamples(self, samples):
    with tempfile.NamedTemporaryFile(prefix='perfkit-gcs-pub',
                                     dir=vm_util.GetTempDir(),
                                     suffix='.json') as tf:
      json_publisher = NewlineDelimitedJSONPublisher(tf.name)
      json_publisher.PublishSamples(samples)
      object_name = self._GenerateObjectName()
      storage_uri = 'gs://{0}/{1}'.format(self.bucket, object_name)
      logging.info('Publishing %d samples to %s', len(samples), storage_uri)
      copy_cmd = [self.gsutil_path, 'cp', tf.name, storage_uri]
      vm_util.IssueRetryableCommand(copy_cmd)


class SampleCollector(object):
  """A performance sample collector.

  Supports incorporating additional metadata into samples, and publishing
  results via any number of SamplePublishers.

  Attributes:
    samples: a list of 3 or 4-tuples. The tuples contain the metric
        name (string), the value (float), and unit (string) of
        each sample. If a 4th element is included, it is a
        dictionary of metadata associated with the sample.
    metadata_providers: list of MetadataProvider. Metadata providers to use.
      Defaults to DEFAULT_METADATA_PROVIDERS.
    publishers: list of SamplePublishers. If not specified, defaults to a
      LogPublisher, PrettyPrintStreamPublisher, NewlineDelimitedJSONPublisher, a
      BigQueryPublisher if FLAGS.bigquery_table is specified, and a
      CloudStoragePublisher if FLAGS.cloud_storage_bucket is specified. See
      SampleCollector._DefaultPublishers.
    run_uri: A unique tag for the run.
  """
  def __init__(self, metadata_providers=None, publishers=None):
    self.samples = []

    if metadata_providers is not None:
      self.metadata_providers = metadata_providers
    else:
      self.metadata_providers = DEFAULT_METADATA_PROVIDERS

    if publishers is not None:
      self.publishers = publishers
    else:
      self.publishers = SampleCollector._DefaultPublishers()

    logging.debug('Using publishers: {0}'.format(self.publishers))

    self.run_uri = str(uuid.uuid4())

  @classmethod
  def _DefaultPublishers(cls):
    """Gets a list of default publishers."""
    publishers = [LogPublisher(), PrettyPrintStreamPublisher()]
    default_json_path = vm_util.PrependTempDir(DEFAULT_JSON_OUTPUT_NAME)
    publishers.append(NewlineDelimitedJSONPublisher(
        FLAGS.json_path or default_json_path))
    if FLAGS.bigquery_table:
      publishers.append(BigQueryPublisher(
          FLAGS.bigquery_table,
          project_id=FLAGS.bq_project,
          bq_path=FLAGS.bq_path,
          service_account=FLAGS.service_account,
          service_account_private_key_file=FLAGS.service_account_private_key))

    if FLAGS.cloud_storage_bucket:
      publishers.append(CloudStoragePublisher(FLAGS.cloud_storage_bucket,
                                              gsutil_path=FLAGS.gsutil_path))

    return publishers

  def AddSamples(self, samples, benchmark, benchmark_spec):
    """Adds data samples to the publisher.

    Args:
      samples: a list of 3 or 4-tuples. The tuples contain the metric
          name (string), the value (float), and unit (string) of
          each sample. If a 4th element is included, it is a
          dictionary of metadata associated with the sample.
      benchmark: string. The name of the benchmark.
      benchmark_spec: BenchmarkSpec. Benchmark specification.
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
      for meta_provider in self.metadata_providers:
        metadata = meta_provider.AddMetadata(metadata, benchmark_spec)

      sample['metadata'] = metadata
      sample['product_name'] = PRODUCT_NAME
      sample['official'] = FLAGS.official
      sample['owner'] = FLAGS.owner
      sample['timestamp'] = time.time()
      sample['run_uri'] = self.run_uri
      sample['sample_uri'] = str(uuid.uuid4())
      self.samples.append(sample)

  def PublishSamples(self):
    """Publish samples via all registered publishers."""
    for publisher in self.publishers:
      publisher.PublishSamples(self.samples)
