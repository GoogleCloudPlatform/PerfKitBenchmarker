#!/usr/bin/env python
# Copyright 2014 Google Inc. All rights reserved.
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

"""Set of utility functions for working with virtual machines."""

import os
import subprocess

import gflags as flags

FLAGS = flags.FLAGS

PRIVATE_KEYFILE = 'perfkitbenchmarker_keyfile'
PUBLIC_KEYFILE = 'perfkitbenchmarker_keyfile.pub'
CERT_FILE = 'perfkitbenchmarker.pem'
TEMP_DIR = '/tmp/perfkitbenchmarker'


def GetTempDir():
  """Returns the tmp dir of the current run."""
  return os.path.join(TEMP_DIR, 'run_{0}'.format(FLAGS.run_uri))


def PrependTempDir(file_name):
  """Returns the file name prepended with the tmp dir of the current run."""
  return '%s/%s' % (GetTempDir(), file_name)


def GenTempDir():
  """Creates the tmp dir for the current run if it does not already exist."""
  create_cmd = ['mkdir', '-p', GetTempDir()]
  create_process = subprocess.Popen(create_cmd)
  create_process.wait()


def SSHKeyGen():
  """Create PerfKitBenchmarker SSH keys in the tmp dir of the current run."""
  if not os.path.isdir(GetTempDir()):
    GenTempDir()

  if not os.path.isfile(GetPrivateKeyPath()):
    create_cmd = ['/usr/bin/ssh-keygen',
                  '-t',
                  'rsa',
                  '-N',
                  '',
                  '-q',
                  '-f',
                  PrependTempDir(PRIVATE_KEYFILE)]
    create_process = subprocess.Popen(create_cmd)
    create_process.wait()

  if not os.path.isfile(GetCertPath()):
    create_cmd = ['/usr/bin/openssl',
                  'req',
                  '-x509',
                  '-new',
                  '-out',
                  PrependTempDir(CERT_FILE),
                  '-key',
                  PrependTempDir(PRIVATE_KEYFILE)]
    create_process = subprocess.Popen(create_cmd,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      stdin=subprocess.PIPE)
    create_process.communicate(input='\n' * 7)


def GetPrivateKeyPath():
  return PrependTempDir(PRIVATE_KEYFILE)


def GetPublicKeyPath():
  return PrependTempDir(PUBLIC_KEYFILE)


def GetCertPath():
  return PrependTempDir(CERT_FILE)
