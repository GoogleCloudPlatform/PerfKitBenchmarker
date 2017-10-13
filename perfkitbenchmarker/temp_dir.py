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
"""Functions related to PerfKit Benchmarker's temporary directory.

PerfKit Benchmarker creates files under a temporary directory (typically in
/tmp/perfkitbenchmarker or C:\TEMP\perfkitbenchmarker - see tempfile.tempdir for
more information).
"""

import functools32
import os
import tempfile

from perfkitbenchmarker import flags
from perfkitbenchmarker import version


_PERFKITBENCHMARKER = 'perfkitbenchmarker'
_RUNS = 'runs'
_VERSIONS = 'versions'

_TEMP_DIR = os.path.join(tempfile.gettempdir(), _PERFKITBENCHMARKER)

flags.DEFINE_string('temp_dir', _TEMP_DIR, 'Temp directory PKB uses.')
FLAGS = flags.FLAGS


def GetAllRunsDirPath():
  """Gets path to the directory containing the states of all PKB runs."""
  return os.path.join(FLAGS.temp_dir, _RUNS)


# Caching this will have the effect that even if the
# run_uri changes, the temp dir will stay the same.
@functools32.lru_cache()
def GetRunDirPath():
  """Gets path to the directory containing files specific to a PKB run."""
  return os.path.join(
      FLAGS.temp_dir, _RUNS, str(flags.FLAGS.run_uri))


def GetVersionDirPath(version=version.VERSION):
  """Gets path to the directory containing files specific to a PKB version."""
  return os.path.join(FLAGS.temp_dir, _VERSIONS, version)


def CreateTemporaryDirectories():
  """Creates the temporary sub-directories needed by the current run."""
  for path in (GetRunDirPath(), GetVersionDirPath()):
    try:
      os.makedirs(path)
    except OSError:
      if not os.path.isdir(path):
        raise
