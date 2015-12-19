# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing Apache Tomcat installation and cleanup functions.

Installing Tomcat via this module makes some changes to the default settings:

  * Http11Nio2Protocol is used (non-blocking).
  * Request logging is disabled.
  * The session timeout is decreased to 1 minute.

https://tomcat.apache.org/
"""
import posixpath
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util


TOMCAT_URL = ('https://archive.apache.org/dist/tomcat/tomcat-8/v8.0.28/bin/'
              'apache-tomcat-8.0.28.tar.gz')
TOMCAT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'tomcat')
TOMCAT_HTTP_PORT = 8080

flags.DEFINE_string('tomcat_url', TOMCAT_URL, 'Tomcat 8 download URL.')

FLAGS = flags.FLAGS

# Start / stop scripts
_TOMCAT_START = posixpath.join(TOMCAT_DIR, 'bin', 'startup.sh')
_TOMCAT_STOP = posixpath.join(TOMCAT_DIR, 'bin', 'shutdown.sh')
_TOMCAT_SERVER_CONF = posixpath.join(TOMCAT_DIR, 'conf', 'server.xml')
_TOMCAT_LOGGING_CONF = posixpath.join(TOMCAT_DIR, 'conf', 'logging.properties')
_TOMCAT_WEB_CONF = posixpath.join(TOMCAT_DIR, 'conf', 'web.xml')

_TOMCAT_PROTOCOL = 'org.apache.coyote.http11.Http11Nio2Protocol'


def _Install(vm):
  vm.Install('openjdk7')
  vm.Install('curl')
  vm.RemoteCommand(
      ('mkdir -p {0} && curl -L {1} | '
       'tar -C {0} --strip-components 1 -xzf -').format(TOMCAT_DIR,
                                                        FLAGS.tomcat_url))

  # Use a non-blocking protocool, and disable access logging (which isn't very
  # helpful during load tests).
  vm.RemoteCommand(
      ("""sed -i.bak -e '/Connector port="8080"/ """
       's/protocol="[^"]\\+"/protocol="{0}"/\' '
       '-e "/org.apache.catalina.valves.AccessLogValve/,+3d" '
       '{1}').format(
           _TOMCAT_PROTOCOL, _TOMCAT_SERVER_CONF))
  # Quiet down localhost logs.
  vm.RemoteCommand(
      ("sed -i.bak "
       r"-e 's/\(2localhost.org.apache.*.level\)\s\+=.*$/\1 = WARN/' "
       ' {0}').format(_TOMCAT_LOGGING_CONF))

  # Expire sessions quickly.
  vm.RemoteCommand(
      ("sed -i.bak "
       r"-e 's,\(<session-timeout>\)30\(</session-timeout>\),\11\2,' "
       " {0}").format(_TOMCAT_WEB_CONF))


def YumInstall(vm):
  """Installs the Tomcat package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Tomcat package on the VM."""
  _Install(vm)


def Start(vm):
  """Starts Tomcat on "vm"."""
  # CentOS7 uses systemd as an init system
  vm.RemoteCommand('bash ' + _TOMCAT_START)


def Stop(vm):
  """Stops Tomcat on "vm"."""
  vm.RemoteCommand('bash ' + _TOMCAT_STOP)
