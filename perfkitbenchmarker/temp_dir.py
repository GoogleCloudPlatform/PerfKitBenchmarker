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
r"""Functions related to PerfKit Benchmarker's temporary directory.

PerfKit Benchmarker creates files under a temporary directory (typically in
/tmp/perfkitbenchmarker or C:\TEMP\perfkitbenchmarker - see tempfile.tempdir for
more information).
"""

import functools
import os
import platform
import tempfile

from absl import flags
from perfkitbenchmarker import version

_PERFKITBENCHMARKER = 'perfkitbenchmarker'
_RUNS = 'runs'
_VERSIONS = 'versions'


def _GetPlatformDependentTempDir():
  """Gets temporary directory based on platform."""
  if platform.system() == 'Darwin':
    # MacOS by default has extremely long temp directory path,
    # resulting failure during ssh due to "too long for Unix domain socket".
    return '/tmp'
  else:
    return tempfile.gettempdir()


_TEMP_DIR = os.path.join(_GetPlatformDependentTempDir(), _PERFKITBENCHMARKER)

flags.DEFINE_string('temp_dir', _TEMP_DIR, 'Temp directory PKB uses.')
FLAGS = flags.FLAGS


def GetAllRunsDirPath():
  """Gets path to the directory containing the states of all PKB runs."""
  return os.path.join(FLAGS.temp_dir, _RUNS)


# Caching this will have the effect that even if the
# run_uri changes, the temp dir will stay the same.
@functools.lru_cache()
def GetRunDirPath():
  """Gets path to the directory containing files specific to a PKB run."""
  return os.path.join(FLAGS.temp_dir, _RUNS, str(flags.FLAGS.run_uri))


def GetSshConnectionsDir():
  """Returns the directory for SSH ControlPaths (for connection reuse)."""
  return os.path.join(GetRunDirPath(), 'ssh')


def GetVersionDirPath(version=version.VERSION):
  """Gets path to the directory containing files specific to a PKB version."""
  return os.path.join(FLAGS.temp_dir, _VERSIONS, version)


def CreateTemporaryDirectories():
  """Creates the temporary sub-directories needed by the current run."""
  for path in (GetRunDirPath(), GetVersionDirPath(), GetSshConnectionsDir()):
    try:
      os.makedirs(path)
    except OSError:
      if not os.path.isdir(path):
        raise
