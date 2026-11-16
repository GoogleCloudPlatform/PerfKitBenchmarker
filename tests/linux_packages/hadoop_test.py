"""Tests for dbp_service."""

import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from packaging import version
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine
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


HADOOP_PROJECT_POM = """\
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <properties>
    <!-- <aws-java-sdk-v2.version>0.0.0</aws-java-sdk-v2.version> -->
    <aws-java-sdk-v2.version>2.35.4</aws-java-sdk-v2.version>
  </properties>
</project>
"""


class InstallAwsSdkBundleTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    hadoop.HadoopVersion.cache_clear()
    hadoop._AwsSdkBundleVersion.cache_clear()
    self.vm = mock.create_autospec(
        virtual_machine.BaseVirtualMachine, instance=True, spec_set=True
    )

  @parameterized.named_parameters(
      dict(testcase_name='3_4_2', hadoop_version='3.4.2'),
      dict(testcase_name='3_3_6', hadoop_version='3.3.6'),
  )
  def testSkipsPreLeanHadoopVersions(self, hadoop_version):
    with (
        flagsaver.flagsaver(hadoop_version=hadoop_version),
        requests_mock.Mocker() as mock_requests,
    ):
      hadoop.InstallAwsSdkBundle(self.vm)
      self.assertFalse(mock_requests.called)
      self.vm.RemoteCommand.assert_not_called()

  @parameterized.named_parameters(
      dict(testcase_name='3_4_3', hadoop_version='3.4.3'),
      dict(testcase_name='3_5_0', hadoop_version='3.5.0'),
  )
  def testInstallsMatchingAwsSdkBundle(self, hadoop_version):
    pom_url = (
        'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/'
        f'{hadoop_version}/hadoop-project-{hadoop_version}.pom'
    )
    with (
        flagsaver.flagsaver(hadoop_version=hadoop_version),
        requests_mock.Mocker() as mock_requests,
    ):
      mock_requests.get(pom_url, text=HADOOP_PROJECT_POM)
      hadoop.InstallAwsSdkBundle(self.vm)
      hadoop.InstallAwsSdkBundle(self.vm)
      self.assertEqual(mock_requests.call_count, 1)
      expected_cmd = (
          'cd /opt/pkb/hadoop/share/hadoop/common/lib && '
          'curl -fsSL -O '
          'https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/'
          '2.35.4/bundle-2.35.4.jar'
      )
      self.vm.RemoteCommand.assert_has_calls([
          mock.call(expected_cmd),
          mock.call(expected_cmd),
      ])

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='3.4.3')
  def testAwsSdkBundlePomConnectionError(self, mock_requests):
    pom_url = (
        'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/'
        '3.4.3/hadoop-project-3.4.3.pom'
    )
    mock_requests.get(pom_url, status_code=404)
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not load ' + pom_url,
    ):
      hadoop.InstallAwsSdkBundle(self.vm)
    self.vm.RemoteCommand.assert_not_called()

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='3.4.3')
  def testAwsSdkBundlePomParseError(self, mock_requests):
    pom_url = (
        'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/'
        '3.4.3/hadoop-project-3.4.3.pom'
    )
    mock_requests.get(pom_url, text='<project><unclosed>')
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not parse POM at ' + pom_url,
    ):
      hadoop.InstallAwsSdkBundle(self.vm)
    self.vm.RemoteCommand.assert_not_called()

  @requests_mock.Mocker()
  @flagsaver.flagsaver(hadoop_version='3.4.3')
  def testAwsSdkBundleMissingPropertyError(self, mock_requests):
    pom_url = (
        'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-project/'
        '3.4.3/hadoop-project-3.4.3.pom'
    )
    mock_requests.get(
        pom_url,
        text=(
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<properties/></project>'
        ),
    )
    with self.assertRaisesRegex(
        errors.Setup.MissingExecutableError,
        'Could not find aws-java-sdk-v2.version in ' + pom_url,
    ):
      hadoop.InstallAwsSdkBundle(self.vm)
    self.vm.RemoteCommand.assert_not_called()


if __name__ == '__main__':
  unittest.main()
