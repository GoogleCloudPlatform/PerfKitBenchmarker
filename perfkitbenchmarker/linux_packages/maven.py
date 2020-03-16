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

import posixpath

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import INSTALL_DIR
from urllib.parse import urlparse

flags.DEFINE_string('maven_version', '3.6.3', 'The version of maven')
FLAGS = flags.FLAGS
MVN_URL = 'https://archive.apache.org/dist/maven/maven-{0}/{1}/binaries/apache-maven-{1}-bin.tar.gz'
MVN_DIR = posixpath.join(INSTALL_DIR, "maven")
MVN_ENV_PATH = "/etc/profile.d/maven.sh"

MVN_ENV = """
export JAVA_HOME={java_home}
export M2_HOME={maven_home}
export MAVEN_HOME={maven_home}
export PATH={maven_home}/bin:$PATH
"""


def GetRunCommand(arguments):
  """ Return Maven run command including proxy settings """
  command = "source {} && mvn {}".format(MVN_ENV_PATH, arguments)

  if FLAGS["http_proxy"].present:
    parsed_url = urlparse(FLAGS.http_proxy)
    http_proxy_params = " -Dhttp.proxyHost={host} -Dhttp.proxyPort={port}"
    command += http_proxy_params.format(host=parsed_url.hostname, port=parsed_url.port)

  if FLAGS["https_proxy"].present:
    parsed_url = urlparse(FLAGS.https_proxy)
    https_proxy_params = " -Dhttps.proxyHost={host} -Dhttps.proxyPort={port}"
    command += https_proxy_params.format(host=parsed_url.hostname, port=parsed_url.port)

  return command


def AptInstall(vm):
  _Install(vm)


def YumInstall(vm):
  vm.InstallPackages('which')
  _Install(vm)


def _Install(vm):
  vm.Install('openjdk')
  vm.Install('curl')

  # Download and extract maven
  maven_full_ver = FLAGS.maven_version
  maven_major_ver = maven_full_ver[:maven_full_ver.index('.')]
  maven_url = MVN_URL.format(maven_major_ver, maven_full_ver)
  vm.RemoteCommand(('mkdir {0} && curl -L {1} | '
                    'tar -C {0} --strip-components=1 -xzf -').format(MVN_DIR, maven_url))

  # Get JAVA_HOME
  out, _ = vm.RemoteCommand('readlink -f `which java`')
  out = out.strip()
  java_home = out[:out.index('/jre')]

  # Set env variables for maven
  maven_env = MVN_ENV.format(java_home=java_home, maven_home=MVN_DIR)
  cmd = 'echo "{0}" | sudo tee -a {1}'.format(maven_env, MVN_ENV_PATH)
  vm.RemoteCommand(cmd)


def Uninstall(vm):
  vm.Uninstall("openjdk")
  vm.RemoteCommand("rm -rf {0}".format(MVN_DIR), ignore_failure=True)
  vm.RemoteCommand("sudo rm -f {0}".format(MVN_ENV_PATH), ignore_failure=True)
