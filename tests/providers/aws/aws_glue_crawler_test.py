"""Tests for aws_glue_crawler."""

import json
import unittest
from unittest import mock

from absl.testing import parameterized
from perfkitbenchmarker import data_discovery_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_glue_crawler
from perfkitbenchmarker.providers.aws import util
from tests import pkb_common_test_case

CRAWLER_ROLE = 'arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole-CrawlerTutorial'


class AwsGlueCrawlerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.path_flag_mock = self.enter_context(
        mock.patch.object(data_discovery_service,
                          '_DATA_DISCOVERY_OBJECT_STORE_PATH'))
    self.path_flag_mock.value = 's3://foo/bar'
    self.region_flag_mock = self.enter_context(
        mock.patch.object(data_discovery_service, '_DATA_DISCOVERY_REGION'))
    self.region_flag_mock.value = 'us-east-1'
    self.flags_mock = self.enter_context(
        mock.patch.object(aws_glue_crawler, 'FLAGS'))
    self.flags_mock.run_uri = 'deadbeef'
    self.flags_mock.aws_glue_crawler_role = CRAWLER_ROLE
    self.flags_mock.aws_glue_crawler_sample_size = None
    self.service = aws_glue_crawler.AwsGlueCrawler()
    self.issue_command_mock = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand'))

  def testInitialization(self):
    self.assertFalse(self.service.user_managed)
    self.assertEqual(self.service.data_discovery_path, 's3://foo/bar')
    self.assertEqual(self.service.region, 'us-east-1')
    self.assertEqual(self.service.db_name, 'pkb-db-deadbeef')
    self.assertEqual(self.service.crawler_name, 'pkb-crawler-deadbeef')
    self.assertEqual(self.service.role, CRAWLER_ROLE)
    self.assertIsNone(self.service.sample_size)

  def testGetMetadata(self):
    self.assertEqual(
        self.service.GetMetadata(), {
            'cloud': 'AWS',
            'data_discovery_region': 'us-east-1',
            'aws_glue_crawler_sample_size': None,
            'aws_glue_crawler_name': 'pkb-crawler-deadbeef',
            'aws_glue_db_name': 'pkb-db-deadbeef',
        })

  def testGetCrawler(self):
    result = self.service._GetCrawler()
    self.assertEqual(result, self.issue_command_mock.return_value)
    self.issue_command_mock.assert_called_once_with(
        util.AWS_PREFIX + [
            'glue', 'get-crawler', '--name', 'pkb-crawler-deadbeef', '--region',
            'us-east-1'
        ],
        raise_on_failure=True)

  def testGetCrawlerRaiseOnFailureFalse(self):
    result = self.service._GetCrawler(raise_on_failure=False)
    self.assertEqual(result, self.issue_command_mock.return_value)
    self.issue_command_mock.assert_called_once_with(
        util.AWS_PREFIX + [
            'glue', 'get-crawler', '--name', 'pkb-crawler-deadbeef', '--region',
            'us-east-1'
        ],
        raise_on_failure=False)

  @mock.patch.object(
      aws_glue_crawler.AwsGlueCrawler, '_GetCrawler', return_value=('', '', 0))
  def testCrawlerExists(self, get_crawler_mock):
    self.assertTrue(self.service._CrawlerExists())
    get_crawler_mock.assert_called_once_with(raise_on_failure=False)

  @mock.patch.object(
      aws_glue_crawler.AwsGlueCrawler,
      '_GetCrawler',
      return_value=('', 'EntityNotFoundException', 1))
  def testCrawlerDoesntExist(self, get_crawler_mock):
    self.assertFalse(self.service._CrawlerExists())
    get_crawler_mock.assert_called_once_with(raise_on_failure=False)

  def testDbExists(self):
    self.issue_command_mock.return_value = ('', '', 0)
    self.assertTrue(self.service._DbExists())
    self.issue_command_mock.assert_called_once_with(
        util.AWS_PREFIX + [
            'glue',
            'get-database',
            '--name',
            'pkb-db-deadbeef',
            '--region',
            'us-east-1',
        ],
        raise_on_failure=False)

  def testDbDoesntExist(self):
    self.issue_command_mock.return_value = ('', 'EntityNotFoundException', 1)
    self.assertFalse(self.service._DbExists())
    self.issue_command_mock.assert_called_once_with(
        util.AWS_PREFIX + [
            'glue',
            'get-database',
            '--name',
            'pkb-db-deadbeef',
            '--region',
            'us-east-1',
        ],
        raise_on_failure=False)

  def testCreate(self):
    self.service._Create()
    self.assertEqual(self.issue_command_mock.call_count, 2)
    self.issue_command_mock.assert_has_calls([
        mock.call([
            'aws', '--output', 'json', 'glue', 'create-database',
            '--database-input',
            '{"Name": "pkb-db-deadbeef", "Description": ""}',
            '--region=us-east-1'
        ]),
        mock.call([
            'aws', '--output', 'json', 'glue', 'create-crawler', '--name',
            'pkb-crawler-deadbeef', '--role', CRAWLER_ROLE, '--database-name',
            'pkb-db-deadbeef', '--targets',
            '{"S3Targets": [{"Path": "s3://foo/bar"}]}', '--region',
            'us-east-1', '--tags', ''
        ])
    ])

  @parameterized.named_parameters(('WithCrawlerAndDb', True, True, True),
                                  ('WithCrawlerOnly', True, False, False),
                                  ('WithDbOnly', False, True, False),
                                  ('WithoutCrawlerAndDb', False, False, False))
  def testExists(self, crawler_exists, db_exists, result):
    crawler_exists_patch = mock.patch.object(
        aws_glue_crawler.AwsGlueCrawler,
        '_CrawlerExists',
        return_value=crawler_exists)
    db_exists_patch = mock.patch.object(
        aws_glue_crawler.AwsGlueCrawler, '_DbExists', return_value=db_exists)
    with db_exists_patch, crawler_exists_patch:
      self.assertEqual(self.service._Exists(), result)

  @parameterized.named_parameters(('True', 'READY', True),
                                  ('False', 'RUNNING', False))
  def testIsReady(self, state_str, expected_result):
    with mock.patch.object(aws_glue_crawler.AwsGlueCrawler,
                           '_GetCrawler') as get_crawler_mock:
      get_crawler_mock.return_value = (
          f'{{"Crawler":{{"State":"{state_str}"}}}}', '', 0)
      self.assertEqual(self.service._IsReady(), expected_result)

  def testIsReadyRaiseOnCrawlFailure(self):
    crawler_data = {
        'Crawler': {
            'LastCrawl': {
                'ErrorMessage': 'Internal Service Exception',
                'LogGroup': '/aws-glue/crawlers',
                'LogStream': 'pkb-crawler-deadbeef',
                'MessagePrefix': '12345678-1234-1234-1234-1234567890ab',
                'StartTime': 1640736374.0,
                'Status': 'FAILED'
            },
            'State': 'READY'
        }
    }
    self.issue_command_mock.return_value = (json.dumps(crawler_data), '', 0)
    with self.assertRaises(aws_glue_crawler.CrawlFailedError) as ctx:
      self.service._IsReady(raise_on_crawl_failure=True)
    self.assertEqual(str(ctx.exception), 'Internal Service Exception')

  def testDelete(self):
    self.service._Delete()
    self.assertEqual(self.issue_command_mock.call_count, 2)
    self.issue_command_mock.assert_has_calls([
        mock.call(
            util.AWS_PREFIX + [
                'glue',
                'delete-database',
                '--name',
                'pkb-db-deadbeef',
                '--region',
                'us-east-1',
            ],
            raise_on_failure=False),
        mock.call(
            util.AWS_PREFIX + [
                'glue',
                'delete-crawler',
                '--name',
                'pkb-crawler-deadbeef',
                '--region',
                'us-east-1',
            ],
            raise_on_failure=False)
    ])

  @parameterized.named_parameters(('WithCrawlerAndDb', True, True, True),
                                  ('WithCrawlerOnly', True, False, True),
                                  ('WithDbOnly', False, True, True),
                                  ('WithoutCrawlerAndDb', False, False, False))
  def testIsDeleting(self, crawler_exists, db_exists, result):
    crawler_exists_patch = mock.patch.object(
        aws_glue_crawler.AwsGlueCrawler,
        '_CrawlerExists',
        return_value=crawler_exists)
    db_exists_patch = mock.patch.object(
        aws_glue_crawler.AwsGlueCrawler, '_DbExists', return_value=db_exists)
    with db_exists_patch, crawler_exists_patch:
      self.assertEqual(self.service._IsDeleting(), result)

  @mock.patch.object(
      aws_glue_crawler.AwsGlueCrawler, '_IsReady', return_value=True)
  def testDiscoverData(self, is_ready_mock):
    self.issue_command_mock.return_value = (
        '{"CrawlerMetricsList":[{"LastRuntimeSeconds":42.7}]}', '', 0)
    self.assertEqual(self.service.DiscoverData(), 42.7)
    is_ready_mock.assert_called_once_with(raise_on_crawl_failure=True)
    self.assertEqual(self.issue_command_mock.call_count, 2)


if __name__ == '__main__':
  unittest.main()
