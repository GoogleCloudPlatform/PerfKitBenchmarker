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
from perfkitbenchmarker.providers.gcp import util


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

    p = mock.patch(publisher.__name__ + '.FLAGS')
    p2 = mock.patch(util.__name__ + '.GetDefaultProject')
    p2.start()
    self.addCleanup(p2.stop)
    self.mock_flags = p.start()
    self.addCleanup(p.stop)

    self.mock_flags.product_name = 'PerfKitBenchmarker'

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
                                   num_striped_disks=1,
                                   sysctl=[],
                                   set_files=[],
                                   simulate_maintenance=False)
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
    self.mock_disk = mock.MagicMock(disk_type='disk-type',
                                    disk_size=20, num_striped_disks=1,
                                    iops=None)
    self.disk_metadata = {
        'type': self.mock_disk.disk_type,
        'size': self.mock_disk.disk_size,
        'num_stripes': self.mock_disk.num_striped_disks,
    }
    self.mock_disk.GetResourceMetadata.return_value = self.disk_metadata

    self.mock_vm = mock.MagicMock(CLOUD='GCP',
                                  zone='us-central1-a',
                                  machine_type='n1-standard-1',
                                  image='ubuntu-14-04',
                                  scratch_disks=[],
                                  hostname='Hostname')
    self.mock_vm.GetResourceMetadata.return_value = {
        'machine_type': self.mock_vm.machine_type,
        'image': self.mock_vm.image,
        'zone': self.mock_vm.zone,
        'cloud': self.mock_vm.CLOUD,
    }
    self.mock_spec = mock.MagicMock(vm_groups={'default': [self.mock_vm]},
                                    vms=[self.mock_vm])

    self.default_meta = {'perfkitbenchmarker_version': 'v1',
                         'cloud': self.mock_vm.CLOUD,
                         'zone': 'us-central1-a',
                         'machine_type': self.mock_vm.machine_type,
                         'image': self.mock_vm.image,
                         'vm_count': 1,
                         'hostnames': 'Hostname'}

  def _RunTest(self, spec, expected, input_metadata=None):
    input_metadata = input_metadata or {}
    instance = publisher.DefaultMetadataProvider()
    result = instance.AddMetadata(input_metadata, self.mock_spec)
    self.assertIsNot(input_metadata, result,
                     msg='Input metadata was not copied.')
    self.assertDictContainsSubset(expected, result)

  def testAddMetadata_ScratchDiskUndefined(self):
    self._RunTest(self.mock_spec, self.default_meta)

  def testAddMetadata_NoScratchDisk(self):
    self.mock_spec.scratch_disk = False
    self._RunTest(self.mock_spec, self.default_meta)

  def testAddMetadata_WithScratchDisk(self):
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_count=1,
                    data_disk_0_num_stripes=1)
    self._RunTest(self.mock_spec, expected)

  def testAddMetadata_DiskSizeNone(self):
    # This situation can happen with static VMs
    self.disk_metadata['size'] = None
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(data_disk_0_size=None,
                    data_disk_0_type='disk-type',
                    data_disk_count=1,
                    data_disk_0_num_stripes=1)
    self._RunTest(self.mock_spec, expected)

  def testAddMetadata_PIOPS(self):
    self.disk_metadata['iops'] = 1000
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_count=1,
                    data_disk_0_num_stripes=1,
                    data_disk_0_iops=1000)
    self._RunTest(self.mock_spec, expected)

  def testDiskMetadata(self):
    self.disk_metadata['foo'] = 'bar'
    self.mock_vm.configure_mock(scratch_disks=[self.mock_disk])
    expected = self.default_meta.copy()
    expected.update(data_disk_0_size=20,
                    data_disk_0_type='disk-type',
                    data_disk_count=1,
                    data_disk_0_num_stripes=1,
                    data_disk_0_foo='bar')
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


class InfluxDBPublisherTestCase(unittest.TestCase):
  def setUp(self):
    self.db_name = 'test_db'
    self.db_uri = 'test'
    self.test_db = publisher.InfluxDBPublisher(self.db_uri, self.db_name)

  def testFormatToKeyValue(self):
    sample_1 = {'test': 'testa', 'metric': '3', 'official': 47.0,
                'value': 'non', 'unit': 'us', 'owner': 'Rackspace',
                'run_uri': '5rtw', 'sample_uri': '5r', 'timestamp': 123}
    sample_2 = {'test': 'testb', 'metric': '2', 'official': 14.0,
                'value': 'non', 'unit': 'MB', 'owner': 'Rackspace',
                'run_uri': 'bba3', 'sample_uri': 'bb',
                'timestamp': 55}
    sample_3 = {'test': 'testc', 'metric': '1', 'official': 1.0,
                'value': 'non', 'unit': 'MB', 'owner': 'Rackspace',
                'run_uri': '323', 'sample_uri': '33',
                'timestamp': 123}
    sample_4 = {'test': 'testc', 'metric': 'some,metric', 'official': 1.0,
                'value': 'non', 'unit': 'Some MB', 'owner': 'Rackspace',
                'run_uri': '323', 'sample_uri': '33',
                'timestamp': 123}
    sample_5 = {'test': 'testc', 'metric': 'some,metric', 'official': 1.0,
                'value': 'non', 'unit': '', 'owner': 'Rackspace',
                'run_uri': '323', 'sample_uri': '',
                'timestamp': 123}

    sample_1_formatted_key_value = self.test_db._FormatToKeyValue(sample_1)
    sample_2_formatted_key_value = self.test_db._FormatToKeyValue(sample_2)
    sample_3_formatted_key_value = self.test_db._FormatToKeyValue(sample_3)
    sample_4_formatted_key_value = self.test_db._FormatToKeyValue(sample_4)
    sample_5_formatted_key_value = self.test_db._FormatToKeyValue(sample_5)

    expected_sample_1 = ['owner=Rackspace', 'unit=us', 'run_uri=5rtw',
                         'test=testa', 'timestamp=123', 'metric=3',
                         'official=47.0', 'value=non', 'sample_uri=5r']
    expected_sample_2 = ['owner=Rackspace', 'unit=MB', 'run_uri=bba3',
                         'test=testb', 'timestamp=55', 'metric=2',
                         'official=14.0', 'value=non', 'sample_uri=bb']
    expected_sample_3 = ['owner=Rackspace', 'unit=MB', 'run_uri=323',
                         'test=testc', 'timestamp=123', 'metric=1',
                         'official=1.0', 'value=non', 'sample_uri=33']
    expected_sample_4 = ['owner=Rackspace', 'unit=Some\ MB', 'run_uri=323',
                         'test=testc', 'timestamp=123', 'metric=some\,metric',
                         'official=1.0', 'value=non', 'sample_uri=33']
    expected_sample_5 = ['owner=Rackspace', 'unit=\\"\\"', 'run_uri=323',
                         'test=testc', 'timestamp=123', 'metric=some\,metric',
                         'official=1.0', 'value=non', 'sample_uri=\\"\\"']

    self.assertItemsEqual(sample_1_formatted_key_value, expected_sample_1)
    self.assertItemsEqual(sample_2_formatted_key_value, expected_sample_2)
    self.assertItemsEqual(sample_3_formatted_key_value, expected_sample_3)
    self.assertItemsEqual(sample_4_formatted_key_value, expected_sample_4)
    self.assertItemsEqual(sample_5_formatted_key_value, expected_sample_5)

  def testConstructSample(self):
    sample_with_metadata = {
        'test': 'testc', 'metric': '1', 'official': 1.0,
        'value': 'non', 'unit': 'MB', 'owner': 'Rackspace',
        'run_uri': '323', 'sample_uri': '33',
        'timestamp': 123,
        'metadata': collections.OrderedDict([('info', '1'),
                                            ('more_info', '2'),
                                            ('bar', 'foo')])}

    constructed_sample = self.test_db._ConstructSample(sample_with_metadata)

    sample_results = ('perfkitbenchmarker,test=testc,official=1.0,'
                      'owner=Rackspace,run_uri=323,sample_uri=33,'
                      'metric=1,unit=MB,info=1,more_info=2,bar=foo '
                      'value=non 123000000000')

    self.assertEqual(constructed_sample, sample_results)

  @mock.patch.object(publisher.InfluxDBPublisher, '_Publish')
  def testPublishSamples(self, mock_publish_method):
    samples = [
        {'test': 'testc', 'metric': '1', 'official': 1.0,
         'value': 'non', 'unit': 'MB', 'owner': 'Rackspace',
         'run_uri': '323', 'sample_uri': '33', 'timestamp': 123,
         'metadata': collections.OrderedDict([('info', '1'),
                                              ('more_info', '2'),
                                              ('bar', 'foo')])
         },
        {'test': 'testb', 'metric': '2', 'official': 14.0,
         'value': 'non', 'unit': 'MB', 'owner': 'Rackspace',
         'run_uri': 'bba3', 'sample_uri': 'bb', 'timestamp': 55,
         'metadata': collections.OrderedDict()
         },
        {'test': 'testa', 'metric': '3', 'official': 47.0,
         'value': 'non', 'unit': 'us', 'owner': 'Rackspace',
         'run_uri': '5rtw', 'sample_uri': '5r', 'timestamp': 123
         }
    ]

    expected = [
        ('perfkitbenchmarker,test=testc,official=1.0,owner=Rackspace,'
         'run_uri=323,sample_uri=33,metric=1,unit=MB,info=1,more_info=2,'
         'bar=foo value=non 123000000000'),
        ('perfkitbenchmarker,test=testb,official=14.0,owner=Rackspace,'
         'run_uri=bba3,sample_uri=bb,metric=2,unit=MB value=non 55000000000'),
        ('perfkitbenchmarker,test=testa,official=47.0,owner=Rackspace,'
         'run_uri=5rtw,sample_uri=5r,metric=3,unit=us value=non 123000000000')
    ]

    mock_publish_method.return_value = None
    self.test_db.PublishSamples(samples)
    mock_publish_method.assert_called_once_with(expected)

  @mock.patch.object(publisher.InfluxDBPublisher, '_WriteData')
  @mock.patch.object(publisher.InfluxDBPublisher, '_CreateDB')
  def testPublish(self, mock_create_db, mock_write_data):
    formatted_samples = [
        ('perfkitbenchmarker,test=testc,official=1.0,owner=Rackspace,'
         'run_uri=323,sample_uri=33,metric=1,unit=MB,info=1,more_info=2,'
         'bar=foo value=non 123000000000'),
        ('perfkitbenchmarker,test=testb,official=14.0,owner=Rackspace,'
         'run_uri=bba3,sample_uri=bb,metric=2,unit=MB value=non 55000000000'),
        ('perfkitbenchmarker,test=testa,official=47.0,owner=Rackspace,'
         'run_uri=5rtw,sample_uri=5r,metric=3,unit=us value=non 123000000000')
    ]

    expected_output = ('perfkitbenchmarker,test=testc,official=1.0,'
                       'owner=Rackspace,run_uri=323,sample_uri=33,metric=1,'
                       'unit=MB,info=1,more_info=2,bar=foo value=non '
                       '123000000000\nperfkitbenchmarker,test=testb,'
                       'official=14.0,owner=Rackspace,run_uri=bba3,'
                       'sample_uri=bb,metric=2,unit=MB value=non 55000000000\n'
                       'perfkitbenchmarker,test=testa,official=47.0,'
                       'owner=Rackspace,run_uri=5rtw,sample_uri=5r,'
                       'metric=3,unit=us value=non 123000000000')

    mock_create_db.return_value = None
    mock_write_data.return_value = None
    self.test_db._Publish(formatted_samples)
    mock_create_db.assert_called_once()
    mock_write_data.assert_called_once_with(expected_output)
