"""Tests for dbp_service."""

import unittest

from absl.testing import flagsaver
from packaging import version
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import hadoop
from tests import pkb_common_test_case
import requests_mock

HADOOP_STABLE_DIR = """\
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /hadoop/common/stable</title>
 </head>
 <body>
<h1>Index of /hadoop/common/stable</h1>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                            <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/hadoop/common/">Parent Directory</a>                                     -
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md">CHANGELOG.md</a>                    2022-05-11 16:49  5.2K
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md.asc">CHANGELOG.md.asc</a>                2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md.sha512">CHANGELOG.md.sha512</a>             2022-05-11 16:49  153
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md">RELEASENOTES.md</a>                 2022-05-11 16:49  2.0K
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md.asc">RELEASENOTES.md.asc</a>             2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md.sha512">RELEASENOTES.md.sha512</a>          2022-05-11 16:49  156
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-rat.txt">hadoop-3.3.3-rat.txt</a>            2022-05-11 16:49  2.0M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-rat.txt.asc">hadoop-3.3.3-rat.txt.asc</a>        2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-rat.txt.sha512">hadoop-3.3.3-rat.txt.sha512</a>     2022-05-11 16:49  161
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.3.3-site.tar.gz">hadoop-3.3.3-site.tar.gz</a>        2022-05-11 16:49   42M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-site.tar.gz.asc">hadoop-3.3.3-site.tar.gz.asc</a>    2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-site.tar.gz.sha512">hadoop-3.3.3-site.tar.gz.sha512</a> 2022-05-11 16:49  165
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.3.3-src.tar.gz">hadoop-3.3.3-src.tar.gz</a>         2022-05-11 16:49   34M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-src.tar.gz.asc">hadoop-3.3.3-src.tar.gz.asc</a>     2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3-src.tar.gz.sha512">hadoop-3.3.3-src.tar.gz.sha512</a>  2022-05-11 16:49  164
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.3.3.tar.gz">hadoop-3.3.3.tar.gz</a>             2022-05-11 16:49  615M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3.tar.gz.asc">hadoop-3.3.3.tar.gz.asc</a>         2022-05-11 16:49  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.3.3.tar.gz.sha512">hadoop-3.3.3.tar.gz.sha512</a>      2022-05-11 16:49  160
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
        'https://downloads.apache.org/hadoop/common/stable',
        text=HADOOP_STABLE_DIR,
    )
    for _ in range(5):
      observed = hadoop.HadoopVersion()
      self.assertEqual(version.Version('3.3.3'), observed)
    # test caching
    self.assertEqual(1, mock_requests.call_count)

  @requests_mock.Mocker()
  def testHadoopVersionConnectionError(self, mock_requests):
    mock_requests.get(
        'https://downloads.apache.org/hadoop/common/stable', status_code=404
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not load https://downloads.apache.org/hadoop/common/stable',
    ):
      hadoop.HadoopVersion()

  @requests_mock.Mocker()
  def testHadoopVersionParsingError(self, mock_requests):
    mock_requests.get(
        'https://downloads.apache.org/hadoop/common/stable',
        text='<html><body><a href="foo">bar</a></body></html>',
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find valid hadoop version at '
        'https://downloads.apache.org/hadoop/common/stable',
    ):
      hadoop.HadoopVersion()

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='4.2.0')
  def testHadoopVersionProvider(self, mock_requests):
    observed = hadoop.HadoopVersion()
    self.assertFalse(mock_requests.called)
    self.assertEqual(version.Version('4.2.0'), observed)

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_bin_url='http://my/hadooop-4.2.0.tar.gz')
  def testHadoopVersionUrlOverride(self, mock_requests):
    observed = hadoop.HadoopVersion()
    self.assertFalse(mock_requests.called)
    self.assertEqual(version.Version('4.2.0'), observed)


if __name__ == '__main__':
  unittest.main()
