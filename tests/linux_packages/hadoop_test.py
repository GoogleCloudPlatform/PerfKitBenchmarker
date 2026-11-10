"""Tests for dbp_service."""

import unittest

from absl.testing import flagsaver
from packaging import version
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import hadoop
from tests import pkb_common_test_case
import requests_mock

HADOOP_VERSION_LIST = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /hadoop/common</title>
 </head>
 <body>
<h1>Index of /hadoop/common</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                            <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/hadoop/common/">Parent Directory</a>                                     -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-2.10.2/">hadoop-2.10.2/</a>          2022-06-17 11:13    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-3.2.4/">hadoop-3.2.4/</a>           2022-06-17 11:12    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-3.3.5/">hadoop-3.3.5/</a>           2022-06-17 11:12    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-3.3.6/">hadoop-3.3.6/</a>           2022-07-15 14:43    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-3.4.0/">hadoop-3.4.0/</a>           2024-03-15 14:43    -
<img src="/icons/folder.gif" alt="[DIR]"> <a href="hadoop-3.4.1/">hadoop-3.4.1/</a>           2024-10-15 14:43    -
<img src="/icons/unknown.gif" alt="[   ]"> <a href="KEYS">KEYS</a>                    2022-06-15 08:08  102K
<hr></pre>
</body></html>
"""


class HadoopVersionsTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    hadoop.HadoopVersion.cache_clear()

  @requests_mock.Mocker()
  def testDefaultHadoopVersion(self, mock_requests):
    mock_requests.get(
        'https://dlcdn.apache.org/hadoop/common',
        text=HADOOP_VERSION_LIST,
    )
    for _ in range(5):
      observed = hadoop.HadoopVersion()
      self.assertEqual(version.Version('3.4.1'), observed)
    # test caching
    self.assertEqual(1, mock_requests.call_count)

  @requests_mock.Mocker()
  def testHadoopVersionConnectionError(self, mock_requests):
    mock_requests.get(
        'https://dlcdn.apache.org/hadoop/common', status_code=404
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not load https://dlcdn.apache.org/hadoop/common',
    ):
      hadoop.HadoopVersion()

  @requests_mock.Mocker()
  def testHadoopVersionParsingError(self, mock_requests):
    mock_requests.get(
        'https://dlcdn.apache.org/hadoop/common',
        text='<html><body><a href="foo">bar</a></body></html>',
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find valid hadoop versions at '
        'https://dlcdn.apache.org/hadoop/common',
    ):
      hadoop.HadoopVersion()

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='4.2.0')
  def testHadoopVersionProvider(self, mock_requests):
    observed = hadoop.HadoopVersion()
    self.assertFalse(mock_requests.called)
    self.assertEqual(version.Version('4.2.0'), observed)

  @requests_mock.Mocker()
  @flagsaver.flagsaver(
      hadoop_version='', hadoop_bin_url='http://my/hadooop-4.2.0.tar.gz'
  )
  def testHadoopVersionUrlOverride(self, mock_requests):
    observed = hadoop.HadoopVersion()
    self.assertFalse(mock_requests.called)
    self.assertEqual(version.Version('4.2.0'), observed)

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='3.3')
  def testHadoopVersionPrefix(self, mock_requests):
    mock_requests.get(
        'https://dlcdn.apache.org/hadoop/common', text=HADOOP_VERSION_LIST
    )
    for _ in range(5):
      observed = hadoop.HadoopVersion()
      self.assertEqual(version.Version('3.3.6'), observed)
    self.assertEqual(1, mock_requests.call_count)

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='3.9')
  def testHadoopVersionPrefixNotFound(self, mock_requests):
    mock_requests.get(
        'https://dlcdn.apache.org/hadoop/common', text=HADOOP_VERSION_LIST
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find valid hadoop versions at '
        'https://dlcdn.apache.org/hadoop/common',
    ):
      hadoop.HadoopVersion()


if __name__ == '__main__':
  unittest.main()
