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
"""Tests for perfkitbenchmarker.publisher."""

import collections
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

  def testAddSamples_3Tuple(self):
    samples = [tuple(self.sample[:3])]
    self.instance.AddSamples(samples, self.benchmark, self.benchmark_spec)
    self._VerifyResult(False)

  def testAddSamples_4Tuple(self):
    samples = [tuple(self.sample)]
    self.instance.AddSamples(samples, self.benchmark, self.benchmark_spec)
    self._VerifyResult()
