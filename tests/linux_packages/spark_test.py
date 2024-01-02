"""Tests for dbp_service."""

import unittest

from absl.testing import flagsaver
from packaging import version
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import spark
from tests import pkb_common_test_case
import requests_mock

SPARK_VERSION_LIST = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /spark</title>
 </head>
 <body>
<h1>Index of /spark</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                    <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/">Parent Directory</a>                             -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="spark-2.4.8/">spark-2.4.8/</a>            2022-06-17 11:13    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="spark-3.0.3/">spark-3.0.3/</a>            2022-06-17 11:12    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="spark-3.1.3/">spark-3.1.3/</a>            2022-06-17 11:12    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="spark-3.2.2/">spark-3.2.2/</a>            2022-07-15 14:43    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="spark-3.3.0/">spark-3.3.0/</a>            2022-06-17 11:11    -
<img src="/icons/unknown.gif" alt="[   ]"> <a href="KEYS">KEYS</a>                    2022-06-15 08:08  102K
<hr></pre>
</body></html>
"""


class SparkVersionsTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    spark.SparkVersion.cache_clear()

  @requests_mock.Mocker()
  def testDefaultSparkVersion(self, mock_requests):
    mock_requests.get(
        'https://downloads.apache.org/spark', text=SPARK_VERSION_LIST
    )
    for _ in range(5):
      observed = spark.SparkVersion()
      self.assertEqual(version.Version('3.3.0'), observed)
    # test caching
    self.assertEqual(1, mock_requests.call_count)

  @requests_mock.Mocker()
  def testSparkVersionConnectionError(self, mock_requests):
    mock_requests.get('https://downloads.apache.org/spark', status_code=404)
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not load https://downloads.apache.org/spark',
    ):
      spark.SparkVersion()

  @requests_mock.Mocker()
  def testSparkVersionParsingError(self, mock_requests):
    mock_requests.get(
        'https://downloads.apache.org/spark',
        text='<html><body><a href="foo">bar</a></body></html>',
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find valid spark versions at '
        'https://downloads.apache.org/spark',
    ):
      spark.SparkVersion()

  @requests_mock.Mocker()
  @flagsaver.flagsaver(spark_version='4.2.0')
  def testSparkVersionProvider(self, mock_requests):
    observed = spark.SparkVersion()
    self.assertFalse(mock_requests.called)
    self.assertEqual(version.Version('4.2.0'), observed)


if __name__ == '__main__':
  unittest.main()
