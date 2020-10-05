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
# limitations under the License
"""Module containing maven installation functions."""

import os
import posixpath
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import linux_packages
from six.moves.urllib.parse import urlparse

flags.DEFINE_string('maven_version', '3.6.3',
                    'The version of maven')
flags.DEFINE_string('maven_mirror_url', None,
                    'If specified, this URL will be used as a Maven mirror')
FLAGS = flags.FLAGS
MVN_URL = 'https://archive.apache.org/dist/maven/maven-{0}/{1}/binaries/apache-maven-{1}-bin.tar.gz'
MVN_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'maven')
MVN_ENV_PATH = '/etc/profile.d/maven.sh'

MVN_ENV = '''
export JAVA_HOME={java_home}
export M2_HOME={maven_home}
export MAVEN_HOME={maven_home}
export PATH={maven_home}/bin:$PATH
'''

PACKAGE_NAME = 'maven'
PREPROVISIONED_DATA = {
    'apache-maven-{0}-bin.tar.gz'.format('3.6.1'):
        '2528c35a99c30f8940cc599ba15d34359d58bec57af58c1075519b8cd33b69e7',
    'apache-maven-{0}-bin.tar.gz'.format('3.6.3'):
        '26ad91d751b3a9a53087aefa743f4e16a17741d3915b219cf74112bf87a438c5'
}
PACKAGE_DATA_URL = {
    'apache-maven-{0}-bin.tar.gz'.format('3.6.1'): MVN_URL.format('3', '3.6.1'),
    'apache-maven-{0}-bin.tar.gz'.format('3.6.3'): MVN_URL.format('3', '3.6.3')
}


def GetRunCommand(arguments):
  """Return Maven run command including proxy settings."""
  command = 'source {} && mvn {}'.format(MVN_ENV_PATH, arguments)

  if FLAGS['http_proxy'].present:
    parsed_url = urlparse(FLAGS.http_proxy)
    http_proxy_params = ' -Dhttp.proxyHost={host} -Dhttp.proxyPort={port}'
    command += http_proxy_params.format(
        host=parsed_url.hostname, port=parsed_url.port)

  if FLAGS['https_proxy'].present:
    parsed_url = urlparse(FLAGS.https_proxy)
    https_proxy_params = ' -Dhttps.proxyHost={host} -Dhttps.proxyPort={port}'
    command += https_proxy_params.format(
        host=parsed_url.hostname, port=parsed_url.port)

  return command


def _GetJavaHome(vm):
  out, _ = vm.RemoteCommand("java -XshowSettings:properties 2>&1 > /dev/null "
                            "| awk '/java.home/{print $3}'")
  out = out.strip()
  if '/jre' in out:
    return out[:out.index('/jre')]
  else:
    return out


def AptInstall(vm):
  _Install(vm)


def YumInstall(vm):
  vm.InstallPackages('which')
  _Install(vm)


def _Install(vm):
  """Install maven package."""
  vm.Install('openjdk')
  vm.Install('curl')

  # Download and extract maven
  maven_full_ver = FLAGS.maven_version
  maven_major_ver = maven_full_ver[:maven_full_ver.index('.')]
  maven_url = MVN_URL.format(maven_major_ver, maven_full_ver)
  maven_tar = maven_url.split('/')[-1]
  # will only work with preprovision_ignore_checksum
  if maven_tar not in PREPROVISIONED_DATA:
    PREPROVISIONED_DATA[maven_tar] = ''
    PACKAGE_DATA_URL[maven_tar] = maven_url
  maven_remote_path = posixpath.join(linux_packages.INSTALL_DIR, maven_tar)
  vm.InstallPreprovisionedPackageData(PACKAGE_NAME, [maven_tar],
                                      linux_packages.INSTALL_DIR)
  vm.RemoteCommand(('mkdir -p {0} && '
                    'tar -C {0} --strip-components=1 -xzf {1}').format(
                        MVN_DIR, maven_remote_path))

  java_home = _GetJavaHome(vm)

  # Set env variables for maven
  maven_env = MVN_ENV.format(java_home=java_home, maven_home=MVN_DIR)
  cmd = 'echo "{0}" | sudo tee -a {1}'.format(maven_env, MVN_ENV_PATH)
  vm.RemoteCommand(cmd)

  if FLAGS.maven_mirror_url:
    settings_local_path = data.ResourcePath(os.path.join(
        'maven', 'settings.xml.j2'))
    settings_remote_path = '~/.m2/settings.xml'
    context = {
        'maven_mirror_url': FLAGS.maven_mirror_url
    }
    vm.RemoteCommand('mkdir -p ~/.m2')
    vm.RenderTemplate(settings_local_path, settings_remote_path, context)


def Uninstall(vm):
  vm.Uninstall('openjdk')
  vm.RemoteCommand('rm -rf {0}'.format(MVN_DIR), ignore_failure=True)
  vm.RemoteCommand('sudo rm -f {0}'.format(MVN_ENV_PATH), ignore_failure=True)
