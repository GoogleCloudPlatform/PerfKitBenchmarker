"""Module containing class for AWS's Glue Crawler."""

import json
from typing import Any, Dict, Optional, Tuple

from absl import flags
from perfkitbenchmarker import data_discovery_service
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS


class CrawlNotCompletedError(Exception):
  """Used to signal a crawl is still running."""


class CrawlFailedError(Exception):
  """Used to signal a crawl has failed."""


class AwsGlueCrawler(data_discovery_service.BaseDataDiscoveryService):
  """AWS Glue Crawler Resource Class.

  Attributes:
    db_name: Name of the Glue database that will be provisioned.
    crawler_name: Name of the crawler that will be provisioned.
    role: Role the crawler will use. Refer to aws_glue_crawler_role flag for
      more info.
    sample_size: How many files will be crawled in each leaf directory. Refer to
      aws_glue_crawler_sample_size flag for more info.
  """

  CLOUD = providers.AWS
  SERVICE_TYPE = 'glue'
  READY = 'READY'
  FAILED = 'FAILED'
  CRAWL_TIMEOUT = 21600
  CRAWL_POLL_INTERVAL = 5

  def __init__(self):
    super().__init__()
    self.db_name = f'pkb-db-{FLAGS.run_uri}'
    self.crawler_name = f'pkb-crawler-{FLAGS.run_uri}'
    self.role = FLAGS.aws_glue_crawler_role
    self.sample_size = FLAGS.aws_glue_crawler_sample_size

  def _Create(self) -> None:
    # creating database
    database_input = {
        'Name': self.db_name,
        'Description': '\n'.join(
            f'{k}={v}' for k, v in util.MakeDefaultTags().items()),
    }
    cmd = util.AWS_PREFIX + [
        'glue',
        'create-database',
        '--database-input', json.dumps(database_input),
        f'--region={self.region}',
    ]
    vm_util.IssueCommand(cmd)

    targets = {'S3Targets': [{'Path': self.data_discovery_path}]}
    if self.sample_size is not None:
      targets['S3Targets'][0]['SampleSize'] = self.sample_size

    # creating crawler
    cmd = util.AWS_PREFIX + [
        'glue',
        'create-crawler',
        '--name', self.crawler_name,
        '--role', self.role,
        '--database-name', self.db_name,
        '--targets', json.dumps(targets),
        '--region', self.region,
        '--tags', ','.join(
            f'{k}={v}' for k, v in util.MakeDefaultTags().items()),
    ]
    vm_util.IssueCommand(cmd)

  def _Exists(self) -> bool:
    return self._DbExists() and self._CrawlerExists()

  def _IsReady(self, raise_on_crawl_failure=False) -> bool:
    stdout, _, _ = self._GetCrawler()
    data = json.loads(stdout)
    if (data['Crawler'].get('LastCrawl', {}).get('Status') == self.FAILED and
        raise_on_crawl_failure):
      raise CrawlFailedError(
          data['Crawler'].get('LastCrawl', {}).get('ErrorMessage', ''))
    return data['Crawler']['State'] == self.READY

  def _Delete(self) -> None:
    # deleting database
    cmd = util.AWS_PREFIX + [
        'glue',
        'delete-database',
        '--name', self.db_name,
        '--region', self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

    # deleting crawler
    cmd = util.AWS_PREFIX + [
        'glue',
        'delete-crawler',
        '--name', self.crawler_name,
        '--region', self.region,
    ]
    vm_util.IssueCommand(cmd, raise_on_failure=False)

  def _IsDeleting(self) -> bool:
    crawler_exists = self._CrawlerExists()
    db_exists = self._DbExists()
    if db_exists is None or crawler_exists is None:
      return True
    return self._DbExists() or self._CrawlerExists()

  def DiscoverData(self) -> float:
    """Runs the AWS Glue Crawler. Returns the time elapsed in secs."""

    cmd = util.AWS_PREFIX + [
        'glue',
        'start-crawler',
        '--name', self.crawler_name,
        '--region', self.region,
    ]
    vm_util.IssueCommand(cmd)
    self._WaitUntilCrawlerReady()
    cmd = util.AWS_PREFIX + [
        'glue',
        'get-crawler-metrics',
        '--crawler-name-list', self.crawler_name,
        '--region', self.region,
    ]
    output, _, _ = vm_util.IssueCommand(cmd)
    data = json.loads(output)
    assert (isinstance(data['CrawlerMetricsList'], list) and
            len(data['CrawlerMetricsList']) == 1)
    return data['CrawlerMetricsList'][0]['LastRuntimeSeconds']

  def GetMetadata(self) -> Dict[str, Any]:
    """Return a dictionary of the metadata for this service."""
    metadata = super().GetMetadata()
    metadata.update(
        aws_glue_crawler_sample_size=self.sample_size,
        aws_glue_db_name=self.db_name,
        aws_glue_crawler_name=self.crawler_name,
    )
    return metadata

  @vm_util.Retry(
      timeout=CRAWL_TIMEOUT,
      poll_interval=CRAWL_POLL_INTERVAL,
      fuzz=0,
      retryable_exceptions=CrawlNotCompletedError,)
  def _WaitUntilCrawlerReady(self):
    if not self._IsReady(raise_on_crawl_failure=True):
      raise CrawlNotCompletedError(
          f'Crawler {self.crawler_name} still running.')

  def _DbExists(self) -> Optional[bool]:
    """Whether the database exists or not.

    It might return None if the API call failed with an unknown error.

    Returns:
      A bool or None.
    """
    cmd = util.AWS_PREFIX + [
        'glue',
        'get-database',
        '--name', self.db_name,
        '--region', self.region,
    ]
    _, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if not retcode:
      return True
    return False if 'EntityNotFoundException' in stderr else None

  def _CrawlerExists(self) -> Optional[bool]:
    """Whether the crawler exists or not.

    It might return None if the API call failed with an unknown error.

    Returns:
      A bool or None.
    """
    _, stderr, retcode = self._GetCrawler(raise_on_failure=False)
    if not retcode:
      return True
    return False if 'EntityNotFoundException' in stderr else None

  def _GetCrawler(self, raise_on_failure=True) -> Tuple[str, str, int]:
    """Calls the AWS CLI to retrieve a crawler."""
    cmd = util.AWS_PREFIX + [
        'glue',
        'get-crawler',
        '--name', self.crawler_name,
        '--region', self.region,
    ]
    return vm_util.IssueCommand(cmd, raise_on_failure=raise_on_failure)

