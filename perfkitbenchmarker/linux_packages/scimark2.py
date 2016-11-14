# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing scimark2 installation and cleanup functions."""
from perfkitbenchmarker.linux_packages import INSTALL_DIR


# Use this directory for all data stored in the VM for this test.
PATH = '{0}/scimark2'.format(INSTALL_DIR)

# Download location for both the C and Java tests.
BASE_URL = 'http://math.nist.gov/scimark2'

# Java-specific constants.
JAVA_JAR = 'scimark2lib.jar'
JAVA_MAIN = 'jnt.scimark2.commandline'

# C-specific constants.
C_ZIP = 'scimark2_1c.zip'
C_SRC = '{0}/src'.format(PATH)
# SciMark2 does not set optimization flags, it leaves this to the
# discretion of the tester. The following gets good performance and
# has been used for LLVM and GCC regression testing, see for example
# https://llvm.org/bugs/show_bug.cgi?id=22589 .
C_CFLAGS = '-O3 -march=native'


def Install(vm):
  """Installs scimark2 on the vm."""
  vm.Install('build_tools')
  vm.Install('wget')
  vm.Install('openjdk')
  vm.Install('unzip')
  cmds = [
      'rm -rf {0} && mkdir {0}'.format(PATH),
      'wget {0}/{1} -O {2}/{1}'.format(BASE_URL, JAVA_JAR, PATH),
      'wget {0}/{1} -O {2}/{1}'.format(BASE_URL, C_ZIP, PATH),
      '(mkdir {0} && cd {0} && unzip {1}/{2})'.format(C_SRC, PATH, C_ZIP),
      '(cd {0} && make CFLAGS="{1}")'.format(C_SRC, C_CFLAGS),
  ]
  for cmd in cmds:
    vm.RemoteCommand(cmd, should_log=True)


def Uninstall(vm):
  """Uninstalls scimark2 from the vm."""
  vm.RemoteCommand('rm -rf {0}'.format(PATH))
