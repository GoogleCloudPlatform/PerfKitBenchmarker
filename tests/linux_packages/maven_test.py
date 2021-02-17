# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.linux_packages.maven."""

import posixpath
import unittest
from absl import flags
import mock
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker.linux_packages import maven
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class MavenTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(MavenTest, self).setUp()
    self.vm = mock.Mock()
    self.vm.RemoteCommand.return_value = ('/home/jre/java', '')

  def assertCallArgsEqual(self, call_args_singles, mock_method):
    """Compare the list of single arguments to all mocked calls in mock_method.

    Mock calls can be tested like this:
      (('x',),) == call('x')
    As all the mocked method calls have one single argument (ie 'x') they need
    to be converted into the tuple of positional arguments tuple that mock
    expects.
    Args:
      call_args_singles: List of single arguments sent to the mock_method,
        ie ['x', 'y'] is for when mock_method was called twice: once with
        x and then with y.
      mock_method: Method that was mocked and called with call_args_singles.
    """
    # convert from ['a', 'b'] into [(('a',),), (('b',),)]
    expected = [((arg,),) for arg in call_args_singles]
    self.assertEqual(expected, mock_method.call_args_list)

  def assertRemoteCommandsEqual(self, expected_cmds):
    # tests the calls to vm.RemoteCommand(str)
    self.assertCallArgsEqual(expected_cmds, self.vm.RemoteCommand)

  def assertVmInstallCommandsEqual(self, expected_cmds):
    # tests the calls to vm.Install(str)
    self.assertCallArgsEqual(expected_cmds, self.vm.Install)

  def assertOnlyKnownMethodsCalled(self, *known_methods):
    # this test will fail if vm.foo() was called and "foo" was not in the
    # known methods
    found_methods = set()
    for mock_call in self.vm.mock_calls:
      found_methods.add(mock_call[0])
    self.assertEqual(set(known_methods), found_methods)

  def testGetRunCommandWithProxy(self):
    FLAGS['http_proxy'].parse('http://some-proxy.com:888')
    FLAGS['https_proxy'].parse('https://some-proxy.com:888')
    cmd = maven.GetRunCommand('install')
    expected = (
        'source {} && mvn install'
        ' -Dhttp.proxyHost=some-proxy.com -Dhttp.proxyPort=888'
        ' -Dhttps.proxyHost=some-proxy.com -Dhttps.proxyPort=888'.format(
            maven.MVN_ENV_PATH))
    self.assertEqual(expected, cmd)

  def testGetRunCommandNoProxy(self):
    FLAGS['http_proxy'].present = 0
    FLAGS['https_proxy'].present = 0
    cmd = maven.GetRunCommand('install')
    expected = ('source {} && mvn install'.format(maven.MVN_ENV_PATH))
    self.assertEqual(expected, cmd)

  def testAptInstall(self):
    maven.AptInstall(self.vm)
    maven_full_ver = maven.FLAGS.maven_version
    maven_major_ver = maven_full_ver[:maven_full_ver.index('.')]
    maven_url = maven.MVN_URL.format(maven_major_ver, maven_full_ver)
    maven_tar = maven_url.split('/')[-1]
    maven_remote_path = posixpath.join(linux_packages.INSTALL_DIR, maven_tar)
    self.assertRemoteCommandsEqual([
        'mkdir -p {0} && '
        'tar -C {0} --strip-components=1 -xzf {1}'.format(maven.MVN_DIR,
                                                          maven_remote_path),
        'java -XshowSettings:properties 2>&1 > /dev/null '
        '| awk \'/java.home/{print $3}\'',
        'echo "{0}" | sudo tee -a {1}'.format(
            maven.MVN_ENV.format(java_home='/home', maven_home=maven.MVN_DIR),
            maven.MVN_ENV_PATH)
    ])
    self.assertVmInstallCommandsEqual(['openjdk', 'curl'])
    self.assertOnlyKnownMethodsCalled('RemoteCommand',
                                      'InstallPreprovisionedPackageData',
                                      'Install')


if __name__ == '__main__':
  unittest.main()
