# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
import csv
import io
import itertools
import json
import logging
import operator
import pprint
import sys
import time
import uuid

from perfkitbenchmarker import disk
from perfkitbenchmarker import events
from perfkitbenchmarker import flags
from perfkitbenchmarker import version
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'product_name',
    'PerfKitBenchmarker',
    'The product name to use when publishing results.')

flags.DEFINE_boolean(
    'official',
    False,
    'A boolean indicating whether results are official or not. The '
    'default is False. Official test results are treated and queried '
    'differently from non-official test results.')

flags.DEFINE_string(
    'json_path',
    None,
    'A path to write newline-delimited JSON results '
    'Default: write to a run-specific temporary directory')
flags.DEFINE_boolean(
    'collapse_labels',
    True,
    'Collapse entries in labels in JSON output.')
flags.DEFINE_string(
    'csv_path',
    None,
    'A path to write CSV-format results')

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

flags.DEFINE_multistring(
    'metadata',
    [],
    'A colon separated key-value pair that will be added to the labels field '
    'of all samples as metadata. Multiple key-value pairs may be specified '
    'by separating each pair by commas. This option can be repeated multiple '
    'times.')

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
    for name, vms in benchmark_spec.vm_groups.iteritems():
      if len(vms) == 0:
        continue
      # Get a representative VM so that we can publish the cloud, zone,
      # machine type, and image.
      vm = vms[-1]
      name_prefix = '' if name == 'default' else name + '_'
      metadata[name_prefix + 'cloud'] = vm.CLOUD
      metadata[name_prefix + 'zone'] = vm.zone
      metadata[name_prefix + 'image'] = vm.image
      for k, v in vm.GetMachineTypeDict().iteritems():
        metadata[name_prefix + k] = v
      metadata[name_prefix + 'vm_count'] = len(vms)

      if vm.scratch_disks:
        data_disk = vm.scratch_disks[0]
        # Legacy metadata keys
        metadata[name_prefix + 'scratch_disk_type'] = data_disk.disk_type
        metadata[name_prefix + 'scratch_disk_size'] = data_disk.disk_size
        metadata[name_prefix + 'num_striped_disks'] = (
            data_disk.num_striped_disks)
        if getattr(data_disk, 'iops', None) is not None:
          metadata[name_prefix + 'scratch_disk_iops'] = data_disk.iops
          metadata[name_prefix + 'aws_provisioned_iops'] = data_disk.iops
        # Modern metadata keys
        metadata[name_prefix + 'data_disk_0_type'] = data_disk.disk_type
        metadata[name_prefix + 'data_disk_0_size'] = (
            data_disk.disk_size * data_disk.num_striped_disks)
        metadata[name_prefix + 'data_disk_0_num_stripes'] = (
            data_disk.num_striped_disks)
        if getattr(data_disk, 'metadata', None) is not None:
          if disk.LEGACY_DISK_TYPE in data_disk.metadata:
            metadata[name_prefix + 'scratch_disk_type'] = (
                data_disk.metadata[disk.LEGACY_DISK_TYPE])
          for key, value in data_disk.metadata.iteritems():
            metadata[name_prefix + 'data_disk_0_' + key] = value

    # Flatten all user metadata into a single list (since each string in the
    # FLAGS.metadata can actually be several key-value pairs) and then iterate
    # over it.
    for pair in [kv for item in FLAGS.metadata for kv in item.split(',')]:
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



class CSVPublisher(SamplePublisher):
  """Publisher which writes results in CSV format to a specified path.

  The default field names are written first, followed by all unique metadata
  keys found in the data.
  """

  _DEFAULT_FIELDS = ('timestamp', 'test', 'metric', 'value', 'unit',
                     'product_name', 'official', 'owner', 'run_uri',
                     'sample_uri')

  def __init__(self, path):
    self._path = path

  def PublishSamples(self, samples):
    samples = list(samples)
    # Union of all metadata keys.
    meta_keys = sorted(
        set(key for sample in samples for key in sample['metadata']))

    logging.info('Writing CSV results to %s', self._path)
    with open(self._path, 'w') as fp:
      writer = csv.DictWriter(fp, list(self._DEFAULT_FIELDS) + meta_keys)
      writer.writeheader()

      for sample in samples:
        d = {}
        d.update(sample)
        d.update(d.pop('metadata'))
        writer.writerow(d)


class PrettyPrintStreamPublisher(SamplePublisher):
  """Writes samples to an output stream, defaulting to stdout.

  Samples are pretty-printed and summarized. Example output (truncated):

    -------------------------PerfKitBenchmarker Results Summary--------------
    COREMARK:
      num_cpus="4"
      Coremark Score                    44145.237832
      End to End Runtime                  289.477677 seconds
    NETPERF:
      client_machine_type="n1-standard-4" client_zone="us-central1-a" ....
      TCP_RR_Transaction_Rate  1354.04 transactions_per_second (ip_type="ext ...
      TCP_RR_Transaction_Rate  3972.70 transactions_per_second (ip_type="int ...
      TCP_CRR_Transaction_Rate  449.69 transactions_per_second (ip_type="ext ...
      TCP_CRR_Transaction_Rate 1271.68 transactions_per_second (ip_type="int ...
      TCP_STREAM_Throughput    1171.04 Mbits/sec               (ip_type="ext ...
      TCP_STREAM_Throughput    6253.24 Mbits/sec               (ip_type="int ...
      UDP_RR_Transaction_Rate  1380.37 transactions_per_second (ip_type="ext ...
      UDP_RR_Transaction_Rate  4336.37 transactions_per_second (ip_type="int ...
      End to End Runtime        444.33 seconds

    -------------------------
    For all tests: cloud="GCP" image="ubuntu-14-04" machine_type="n1-standa ...

  Attributes:
    stream: File-like object. Output stream to print samples.
  """

  def __init__(self, stream=None):
    self.stream = stream or sys.stdout

  def __repr__(self):
    return '<{0} stream={1}>'.format(type(self).__name__, self.stream)

  def _FindConstantMetadataKeys(self, samples):
    """Finds metadata keys which are constant across a collection of samples.

    Args:
      samples: List of dicts, as passed to SamplePublisher.PublishSamples.

    Returns:
      The set of metadata keys for which all samples in 'samples' have the same
      value.
    """
    unique_values = {}

    for sample in samples:
      for k, v in sample['metadata'].iteritems():
        if len(unique_values.setdefault(k, set())) < 2:
          unique_values[k].add(v)

    # Find keys which are not present in all samples
    for sample in samples:
      for k in frozenset(unique_values) - frozenset(sample['metadata']):
        unique_values[k].add(None)

    return frozenset(k for k, v in unique_values.iteritems() if len(v) == 1)

  def _FormatMetadata(self, metadata):
    """Format 'metadata' as space-delimited key="value" pairs."""
    return ' '.join('{0}="{1}"'.format(k, v)
                    for k, v in sorted(metadata.iteritems()))

  def PublishSamples(self, samples):
    # result will store the formatted text, then be emitted to self.stream and
    # logged.
    result = io.BytesIO()
    dashes = '-' * 25
    result.write('\n' + dashes +
                 'PerfKitBenchmarker Results Summary' +
                 dashes + '\n')

    if not samples:
      logging.debug('Pretty-printing results to %s:\n%s', self.stream,
                    result.getvalue())
      self.stream.write(result.getvalue())
      return

    key = operator.itemgetter('test')
    samples = sorted(samples, key=key)
    globally_constant_keys = self._FindConstantMetadataKeys(samples)

    for benchmark, test_samples in itertools.groupby(samples, key):
      test_samples = list(test_samples)
      # Drop end-to-end runtime: it always has no metadata.
      non_endtoend_samples = [i for i in test_samples
                              if i['metric'] != 'End to End Runtime']
      locally_constant_keys = (
          self._FindConstantMetadataKeys(non_endtoend_samples) -
          globally_constant_keys)
      all_constant_meta = globally_constant_keys.union(locally_constant_keys)

      benchmark_meta = {k: v for k, v in test_samples[0]['metadata'].iteritems()
                        if k in locally_constant_keys}
      result.write('{0}:\n'.format(benchmark.upper()))

      if benchmark_meta:
        result.write('  {0}\n'.format(
            self._FormatMetadata(benchmark_meta)))

      for sample in test_samples:
        meta = {k: v for k, v in sample['metadata'].iteritems()
                if k not in all_constant_meta}
        result.write('  {0:<30s} {1:>15f} {2:<30s}'.format(
            sample['metric'], sample['value'], sample['unit']))
        if meta:
          result.write(' ({0})'.format(self._FormatMetadata(meta)))
        result.write('\n')

    global_meta = {k: v for k, v in samples[0]['metadata'].iteritems()
                   if k in globally_constant_keys}
    result.write('\n' + dashes + '\n')
    result.write('For all tests: {0}\n'.format(
        self._FormatMetadata(global_meta)))

    value = result.getvalue()
    logging.debug('Pretty-printing results to %s:\n%s', self.stream, value)
    self.stream.write(value)


class LogPublisher(SamplePublisher):
  """Writes samples to a Python Logger.

  Attributes:
    level: Logging level. Defaults to logging.INFO.
    logger: Logger to publish to. Defaults to the root logger.
  """

  def __init__(self, level=logging.INFO, logger=None):
    self.level = level
    self.logger = logger or logging.getLogger()
    self._pprinter = pprint.PrettyPrinter()

  def __repr__(self):
    return '<{0} logger={1} level={2}>'.format(type(self).__name__, self.logger,
                                               self.level)

  def PublishSamples(self, samples):
    data = [
        '\n' + '-' * 25 + 'PerfKitBenchmarker Complete Results' + '-' * 25 +
        '\n']
    for sample in samples:
      data.append('%s\n' % self._pprinter.pformat(sample))
    self.logger.log(self.level, ''.join(data))


# TODO: Extract a function to write delimited JSON to a stream.
class NewlineDelimitedJSONPublisher(SamplePublisher):
  """Publishes samples to a file as newline delimited JSON.

  The resulting output file is compatible with 'bq load' using
  format NEWLINE_DELIMITED_JSON.

  If 'collapse_labels' is True, metadata is converted to a flat string with key
  'labels' via GetLabelsFromDict.

  Attributes:
    file_path: string. Destination path to write samples.
    mode: Open mode for 'file_path'. Set to 'a' to append.
    collapse_labels: boolean. If true, collapse sample metadata.
  """

  def __init__(self, file_path, mode='wb', collapse_labels=True):
    self.file_path = file_path
    self.mode = mode
    self.collapse_labels = collapse_labels

  def __repr__(self):
    return '<{0} file_path="{1}" mode="{2}">'.format(
        type(self).__name__, self.file_path, self.mode)

  def PublishSamples(self, samples):
    logging.info('Publishing %d samples to %s', len(samples),
                 self.file_path)
    with open(self.file_path, self.mode) as fp:
      for sample in samples:
        sample = sample.copy()
        if self.collapse_labels:
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

    with vm_util.NamedTemporaryFile(prefix='perfkit-bq-pub',
                                    dir=vm_util.GetTempDir(),
                                    suffix='.json') as tf:
      json_publisher = NewlineDelimitedJSONPublisher(tf.name,
                                                     collapse_labels=True)
      json_publisher.PublishSamples(samples)
      tf.close()
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
    with vm_util.NamedTemporaryFile(prefix='perfkit-gcs-pub',
                                    dir=vm_util.GetTempDir(),
                                    suffix='.json') as tf:
      json_publisher = NewlineDelimitedJSONPublisher(tf.name)
      json_publisher.PublishSamples(samples)
      tf.close()
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
    samples: A list of Sample objects.
    metadata_providers: A list of MetadataProvider objects. Metadata providers
      to use.  Defaults to DEFAULT_METADATA_PROVIDERS.
    publishers: A list of SamplePublisher objects. If not specified, defaults to
      a LogPublisher, PrettyPrintStreamPublisher, NewlineDelimitedJSONPublisher,
      a BigQueryPublisher if FLAGS.bigquery_table is specified, and a
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

  @classmethod
  def _DefaultPublishers(cls):
    """Gets a list of default publishers."""
    publishers = [LogPublisher(), PrettyPrintStreamPublisher()]
    default_json_path = vm_util.PrependTempDir(DEFAULT_JSON_OUTPUT_NAME)
    publishers.append(NewlineDelimitedJSONPublisher(
        FLAGS.json_path or default_json_path,
        collapse_labels=FLAGS.collapse_labels))
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
    if FLAGS.csv_path:
      publishers.append(CSVPublisher(FLAGS.csv_path))

    return publishers

  def AddSamples(self, samples, benchmark, benchmark_spec):
    """Adds data samples to the publisher.

    Args:
      samples: A list of Sample objects.
      benchmark: string. The name of the benchmark.
      benchmark_spec: BenchmarkSpec. Benchmark specification.
    """
    for s in samples:
      # Annotate the sample.
      sample = dict(s.asdict())
      sample['test'] = benchmark

      for meta_provider in self.metadata_providers:
        sample['metadata'] = meta_provider.AddMetadata(
            sample['metadata'], benchmark_spec)

      sample['product_name'] = FLAGS.product_name
      sample['official'] = FLAGS.official
      sample['owner'] = FLAGS.owner
      sample['run_uri'] = benchmark_spec.uuid
      sample['sample_uri'] = str(uuid.uuid4())
      events.sample_created.send(benchmark_spec=benchmark_spec,
                                 sample=sample)
      self.samples.append(sample)

  def PublishSamples(self):
    """Publish samples via all registered publishers."""
    for publisher in self.publishers:
      publisher.PublishSamples(self.samples)
