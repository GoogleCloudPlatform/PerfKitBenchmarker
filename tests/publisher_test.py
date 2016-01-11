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
"""Tests for perfkitbenchmarker.publisher."""

import collections
import csv
import io
import json
import re
import tempfile
import uuid
import unittest

import mock

from perfkitbenchmarker import publisher
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util


class PrettyPrintStreamPublisherTestCase(unittest.TestCase):

  def testDefaultsToStdout(self):
    with mock.patch('sys.stdout') as mock_stdout:
      instance = publisher.PrettyPrintStreamPublisher()
      self.assertEqual(mock_stdout, instance.stream)

  def testSucceedsWithNoSamples(self):
    stream = io.BytesIO()
    instance = publisher.PrettyPrintStreamPublisher(stream)
    instance.PublishSamples([])
    self.assertRegexpMatches(
        stream.getvalue(), r'^\s*-+PerfKitBenchmarker\sResults\sSummary-+\s*$')

  def testWritesToStream(self):
    stream = io.BytesIO()
    instance = publisher.PrettyPrintStreamPublisher(stream)
    samples = [{'test': 'testb', 'metric': '1', 'value': 1.0, 'unit': 'MB',
                'metadata': {}},
               {'test': 'testb', 'metric': '2', 'value': 14.0, 'unit': 'MB',
                'metadata': {}},
               {'test': 'testa', 'metric': '3', 'value': 47.0, 'unit': 'us',
                'metadata': {}}]
    instance.PublishSamples(samples)

    value = stream.getvalue()
    self.assertRegexpMatches(value, re.compile(r'TESTA.*TESTB', re.DOTALL))


class LogPublisherTestCase(unittest.TestCase):

  def testCallsLoggerAtCorrectLevel(self):
    logger = mock.MagicMock()
    level = mock.MagicMock()

    instance = publisher.LogPublisher(logger=logger, level=level)

    instance.PublishSamples([{'test': 'testa'}, {'test': 'testb'}])
    logger.log.assert_called_once_with(level, mock.ANY)


class NewlineDelimitedJSONPublisherTestCase(unittest.TestCase):

  def setUp(self):
    self.fp = tempfile.NamedTemporaryFile(prefix='perfkit-test-',
                                          suffix='.json')
    self.addCleanup(self.fp.close)
    self.instance = publisher.NewlineDelimitedJSONPublisher(self.fp.name)

  def testEmptyInput(self):
    self.instance.PublishSamples([])
    self.assertEqual('', self.fp.read())

  def testMetadataConvertedToLabels(self):
    samples = [{'test': 'testa',
                'metadata': collections.OrderedDict([('key', 'value'),
                                                     ('foo', 'bar')])}]
    self.instance.PublishSamples(samples)
    d = json.load(self.fp)
    self.assertDictEqual({'test': 'testa', 'labels': '|key:value|,|foo:bar|'},
                         d)

  def testJSONRecordPerLine(self):
    samples = [{'test': 'testa', 'metadata': {'key': 'val'}},
               {'test': 'testb', 'metadata': {'key2': 'val2'}}]
    self.instance.PublishSamples(samples)
    self.assertRaises(ValueError, json.load, self.fp)
    self.fp.seek(0)
    result = [json.loads(i) for i in self.fp]
    self.assertListEqual([{u'test': u'testa', u'labels': u'|key:val|'},
                          {u'test': u'testb', u'labels': u'|key2:val2|'}],
                         result)


class BigQueryPublisherTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(publisher.__name__ + '.vm_util', spec=publisher.vm_util)
    self.mock_vm_util = p.start()
    publisher.vm_util.NamedTemporaryFile = vm_util.NamedTemporaryFile
    self.mock_vm_util.GetTempDir.return_value = tempfile.gettempdir()
    self.addCleanup(p.stop)

    self.samples = [{'test': 'testa', 'metadata': {}},
                    {'test': 'testb', 'metadata': {}}]
    self.table = 'samples_mart.results'

  def testNoSamples(self):
    instance = publisher.BigQueryPublisher(self.table)
    instance.PublishSamples([])
    self.assertEqual([], self.mock_vm_util.IssueRetryableCommand.mock_calls)

  def testNoProject(self):
    instance = publisher.BigQueryPublisher(self.table)
    instance.PublishSamples(self.samples)
    self.mock_vm_util.IssueRetryableCommand.assert_called_once_with(
        ['bq',
         'load',
         '--source_format=NEWLINE_DELIMITED_JSON',
         self.table,
         mock.ANY])

  def testServiceAccountFlags_MissingPrivateKey(self):
    self.assertRaises(ValueError,
                      publisher.BigQueryPublisher,
                      self.table,
                      service_account=mock.MagicMock())

  def testServiceAccountFlags_MissingServiceAccount(self):
    self.assertRaises(ValueError,
                      publisher.BigQueryPublisher,
                      self.table,
                      service_account_private_key_file=mock.MagicMock())

  def testServiceAccountFlags_BothSpecified(self):
    instance = publisher.BigQueryPublisher(
        self.table,
        service_account=mock.MagicMock(),
        service_account_private_key_file=mock.MagicMock())
    instance.PublishSamples(self.samples)  # No error
    self.mock_vm_util.IssueRetryableCommand.assert_called_once_with(mock.ANY)


class CloudStoragePublisherTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(publisher.__name__ + '.vm_util', spec=publisher.vm_util)
    self.mock_vm_util = p.start()
    publisher.vm_util.NamedTemporaryFile = vm_util.NamedTemporaryFile
    self.mock_vm_util.GetTempDir.return_value = tempfile.gettempdir()
    self.addCleanup(p.stop)

    p = mock.patch(publisher.__name__ + '.time', spec=publisher.time)
    self.mock_time = p.start()
    self.addCleanup(p.stop)

    p = mock.patch(publisher.__name__ + '.uuid', spec=publisher.uuid)
    self.mock_uuid = p.start()
    self.addCleanup(p.stop)

    self.samples = [{'test': 'testa', 'metadata': {}},
                    {'test': 'testb', 'metadata': {}}]

  def testPublishSamples(self):
    self.mock_time.time.return_value = 1417647763.387665
    self.mock_uuid.uuid4.return_value = uuid.UUID(
        'be428eb3-a54a-4615-b7ca-f962b729c7ab')
    instance = publisher.CloudStoragePublisher('test-bucket')
    instance.PublishSamples(self.samples)
    self.mock_vm_util.IssueRetryableCommand.assert_called_once_with(
        ['gsutil', 'cp', mock.ANY,
         'gs://test-bucket/141764776338_be428eb'])


class SampleCollectorTestCase(unittest.TestCase):

  def setUp(self):
    self.instance = publisher.SampleCollector(publishers=[])
    self.sample = sample.Sample('widgets', 100, 'oz', {'foo': 'bar'})
    self.benchmark = 'test!'
    self.benchmark_spec = mock.MagicMock()

  def _VerifyResult(self, contains_metadata=True):
    self.assertEqual(1, len(self.instance.samples))
    collector_sample = self.instance.samples[0]
    metadata = collector_sample.pop('metadata')
    self.assertDictContainsSubset(
        {
            'value': 100,
            'metric': 'widgets',
            'unit': 'oz',
            'test': self.benchmark,
            'product_name': 'PerfKitBenchmarker'
        },
        collector_sample)
    if contains_metadata:
      self.assertDictContainsSubset({'foo': 'bar'}, metadata)
    else:
      self.assertNotIn('foo', metadata)

  def testAddSamples_SampleClass(self):
    samples = [self.sample]
    self.instance.AddSamples(samples, self.benchmark, self.benchmark_spec)
    self._VerifyResult()

  def testAddSamples_WithTimestamp(self):
    timestamp_sample = sample.Sample('widgets', 100, 'oz', {}, 1.0)
    samples = [timestamp_sample]
    self.instance.AddSamples(samples, self.benchmark, self.benchmark_spec)
    self.assertDictContainsSubset(
        {
            'timestamp': 1.0
        },
        self.instance.samples[0])


class DefaultMetadataProviderTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(publisher.__name__ + '.FLAGS')
    self.mock_flags = p.start()
    self.mock_flags.configure_mock(metadata=[],
                                   num_striped_disks=1)
    self.addCleanup(p.stop)

    self.maxDiff = None
    p = mock.patch(publisher.__name__ + '.version',
                   VERSION='v1')
    p.start()
    self.addCleanup(p.stop)

    # Need iops=None in self.mock_disk because otherwise doing
    # mock_disk.iops returns a mock.MagicMock, which is not None,
    # which defeats the getattr check in
    # publisher.DefaultMetadataProvider.
    self.mock_disk = mock.MagicMock(disk_size=20, num_striped_disks=1,
                                    iops=None)

    self.mock_vm = mock.MagicMock(CLOUD='GCP',
                                  zone='us-central1-a',
                                  machine_type='n1-standard-1',
                                  image='ubuntu-14-04',
                                  scratch_disks=[])
    self.mock_vm.GetMachineTypeDict.return_value = {
        'machine_type': self.mock_vm.machine_type}
    self.mock_spec = mock.MagicMock(vm_groups={'default': [self.mock_vm]})

    self.default_meta = {'perfkitbenchmarker_version': 'v1',
                         'cloud': self.mock_vm.CLOUD,
                         'zone': 'us-central1-a',
                         'machine_type': self.mock_vm.machine_type,
                         'image': self.mock_vm.image,
                         'vm_count': 1,
                         'num_striped_disks': 1}

  def _RunTest(self, spec, expected, input_metadata=None):
    input_metadata = input_metadata or {}
    instance = publisher.DefaultMetadataProvider()
    result = instance.AddMetadata(input_metadata, self.mock_spec)
    self.assertIsNot(input_metadata, result,
                     msg='Input metadata was not copied.')
    self.assertDictEqual(expected, result)

  def testAddMetadata_ScratchDiskUndefined(self):
    del self.mock_spec.scratch_disk
    meta = self.default_meta.copy()
    meta.pop('num_striped_disks')
    self._RunTest(self.mock_spec, meta)

  def testAddMetadata_NoScratchDisk(self):
    self.mock_spec.scratch_disk = False
    meta = self.default_meta.copy()
    meta.pop('num_striped_disks')
    self._RunTest(self.mock_spec, meta)

  def testAddMetadata_WithScratchDisk(self):
    self.mock_disk.configure_mock(disk_type='disk-type')
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(scratch_disk_size=20,
                    scratch_disk_type='disk-type',
                    data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_0_num_stripes=1)
    self._RunTest(self.mock_spec, expected)

  def testAddMetadata_PIOPS(self):
    self.mock_disk.configure_mock(disk_type='disk-type',
                                  iops=1000)
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(scratch_disk_size=20,
                    scratch_disk_type='disk-type',
                    scratch_disk_iops=1000,
                    data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_0_num_stripes=1,
                    aws_provisioned_iops=1000)
    self._RunTest(self.mock_spec, expected)

  def testDiskMetadata(self):
    self.mock_disk.configure_mock(disk_type='disk-type',
                                  metadata={'foo': 'bar'})
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(scratch_disk_size=20,
                    scratch_disk_type='disk-type',
                    data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_0_num_stripes=1,
                    data_disk_0_foo='bar')
    self._RunTest(self.mock_spec, expected)

  def testDiskLegacyDiskType(self):
    self.mock_disk.configure_mock(disk_type='disk-type',
                                  metadata={'foo': 'bar',
                                            'legacy_disk_type': 'remote_ssd'})
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(scratch_disk_size=20,
                    scratch_disk_type='remote_ssd',
                    data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_0_num_stripes=1,
                    data_disk_0_foo='bar',
                    data_disk_0_legacy_disk_type='remote_ssd')
    self._RunTest(self.mock_spec, expected)


class CSVPublisherTestCase(unittest.TestCase):
  def setUp(self):
    self.tf = tempfile.NamedTemporaryFile(prefix='perfkit-csv-publisher',
                                          suffix='.csv')
    self.addCleanup(self.tf.close)

  def testWritesToStream(self):
    instance = publisher.CSVPublisher(self.tf.name)
    samples = [{'test': 'testb', 'metric': '1', 'value': 1.0, 'unit': 'MB',
                'metadata': {}},
               {'test': 'testb', 'metric': '2', 'value': 14.0, 'unit': 'MB',
                'metadata': {}},
               {'test': 'testa', 'metric': '3', 'value': 47.0, 'unit': 'us',
                'metadata': {}}]
    instance.PublishSamples(samples)
    self.tf.seek(0)
    rows = list(csv.DictReader(self.tf))
    self.assertItemsEqual(['1', '2', '3'], [i['metric'] for i in rows])

  def testUsesUnionOfMetaKeys(self):
    instance = publisher.CSVPublisher(self.tf.name)
    samples = [{'test': 'testb', 'metric': '1', 'value': 1.0, 'unit': 'MB',
                'metadata': {'key1': 'value1'}},
               {'test': 'testb', 'metric': '2', 'value': 14.0, 'unit': 'MB',
                'metadata': {'key1': 'value2'}},
               {'test': 'testa', 'metric': '3', 'value': 47.0, 'unit': 'us',
                'metadata': {'key3': 'value3'}}]
    instance.PublishSamples(samples)
    self.tf.seek(0)
    reader = csv.DictReader(self.tf)
    rows = list(reader)
    self.assertEqual(['key1', 'key3'], reader.fieldnames[-2:])
    self.assertEqual(3, len(rows))
