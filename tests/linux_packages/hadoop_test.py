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
  <title>Index of /dist/hadoop/common/stable</title>
 </head>
 <body>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of /</title>
 </head>
 <body>
  <body style="background: #3333; font-family: Montserrat, Sans-Serif;">
    <div style="padding: 6px; background: #FFF; max-width: 780px; margin: 0 auto; min-height: 500px;  border-radius: 4px; border: 1.25px solid #3333:">
      <h1>Apache Archive Distribution Directory</h1>
      <hr/>
<p>The directories and files linked below are a historical archive of software released by
Apache Software Foundation projects. <br/><br/>
<b style='color: maroon;'>THEY MAY BE UNSUPPORTED AND UNSAFE TO USE</b><br/>
Current releases can be found on our <a href="http://downloads.apache.org/">download server</a>.</p>

<script> document.title = "Apache Archive Distribution Directory"; </script>
<pre>
<pre><img src="/icons/blank.gif" alt="Icon "> <a href="?C=N;O=D">Name</a>                            <a href="?C=M;O=A">Last modified</a>      <a href="?C=S;O=A">Size</a>  <a href="?C=D;O=A">Description</a><hr><img src="/icons/back.gif" alt="[PARENTDIR]"> <a href="/dist/hadoop/common/">Parent Directory</a>                                     -
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md">CHANGELOG.md</a>                    2024-10-10 10:53   23K
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md.asc">CHANGELOG.md.asc</a>                2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="CHANGELOG.md.sha512">CHANGELOG.md.sha512</a>             2024-10-10 10:53  153
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md">RELEASENOTES.md</a>                 2024-10-10 10:53  3.2K
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md.asc">RELEASENOTES.md.asc</a>             2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="RELEASENOTES.md.sha512">RELEASENOTES.md.sha512</a>          2024-10-10 10:53  156
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.4.1-lean.tar.gz">hadoop-3.4.1-lean.tar.gz</a>        2024-10-10 10:53  471M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-lean.tar.gz.asc">hadoop-3.4.1-lean.tar.gz.asc</a>    2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-lean.tar.gz.sha512">hadoop-3.4.1-lean.tar.gz.sha512</a> 2024-10-10 10:53  165
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-rat.txt">hadoop-3.4.1-rat.txt</a>            2024-10-10 10:53  2.2M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-rat.txt.asc">hadoop-3.4.1-rat.txt.asc</a>        2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-rat.txt.sha512">hadoop-3.4.1-rat.txt.sha512</a>     2024-10-10 10:53  161
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.4.1-site.tar.gz">hadoop-3.4.1-site.tar.gz</a>        2024-10-10 10:53   41M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-site.tar.gz.asc">hadoop-3.4.1-site.tar.gz.asc</a>    2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-site.tar.gz.sha512">hadoop-3.4.1-site.tar.gz.sha512</a> 2024-10-10 10:53  165
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.4.1-src.tar.gz">hadoop-3.4.1-src.tar.gz</a>         2024-10-10 10:53   37M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-src.tar.gz.asc">hadoop-3.4.1-src.tar.gz.asc</a>     2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1-src.tar.gz.sha512">hadoop-3.4.1-src.tar.gz.sha512</a>  2024-10-10 10:53  164
<img src="/icons/compressed.gif" alt="[   ]"> <a href="hadoop-3.4.1.tar.gz">hadoop-3.4.1.tar.gz</a>             2024-10-10 10:53  929M
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1.tar.gz.asc">hadoop-3.4.1.tar.gz.asc</a>         2024-10-10 10:53  833
<img src="/icons/text.gif" alt="[TXT]"> <a href="hadoop-3.4.1.tar.gz.sha512">hadoop-3.4.1.tar.gz.sha512</a>      2024-10-10 10:53  160
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
        'https://archive.apache.org/dist/hadoop/common/stable',
        text=HADOOP_STABLE_DIR,
    )
    for _ in range(5):
      observed = hadoop.HadoopVersion()
      self.assertEqual(version.Version('3.4.1'), observed)
    # test caching
    self.assertEqual(1, mock_requests.call_count)

  @requests_mock.Mocker()
  def testHadoopVersionConnectionError(self, mock_requests):
    mock_requests.get(
        'https://archive.apache.org/dist/hadoop/common/stable', status_code=404
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not load https://archive.apache.org/dist/hadoop/common/stable',
    ):
      hadoop.HadoopVersion()

  @requests_mock.Mocker()
  def testHadoopVersionParsingError(self, mock_requests):
    mock_requests.get(
        'https://archive.apache.org/dist/hadoop/common/stable',
        text='<html><body><a href="foo">bar</a></body></html>',
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find valid hadoop version at '
        'https://archive.apache.org/dist/hadoop/common/stable',
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
