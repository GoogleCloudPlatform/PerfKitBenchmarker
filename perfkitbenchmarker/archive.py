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

import datetime
import logging
import os
import posixpath
import subprocess
import tarfile

from perfkitbenchmarker.providers.aws.util import AWS_PATH


def ArchiveRun(run_temp_directory, target_bucket,
               prefix='',
               gsutil_path='gsutil',
               aws_path=AWS_PATH):
  """Archive a run directory to GCS or S3.

  Args:
    run_temp_directory: str. directory to archive.
    target_bucket: str. Either a gs:// or s3:// path to an extant bucket.
    prefix: str. prefix for the file.
    gsutil_path: str. Path to the gsutil tool.
    aws_path: str. Path to the aws command line tool.
  """
  if not os.path.isdir(run_temp_directory):
    raise ValueError('{0} is not a directory.'.format(run_temp_directory))

  tar_file_name = '{}{}.tar.gz'.format(
      prefix, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

  prefix_len = 5
  prefixes = {
      's3://': [aws_path, 's3', 'cp'],
      'gs://': [gsutil_path, 'cp']
  }

  assert all(len(key) == prefix_len for key in prefixes), prefixes

  try:
    cmd = (prefixes[target_bucket[:prefix_len]] +
           ['-', posixpath.join(target_bucket, tar_file_name)])
  except KeyError:
    raise ValueError('Unsupported bucket name: {0}'.format(target_bucket))

  logging.info('Streaming %s to %s\n%s', run_temp_directory, tar_file_name,
               ' '.join(cmd))
  p = subprocess.Popen(cmd, stdin=subprocess.PIPE)

  with p.stdin:
    with tarfile.open(mode='w:gz', fileobj=p.stdin) as tar:
      tar.add(run_temp_directory, os.path.basename(run_temp_directory))

  status = p.wait()
  if status:
    raise subprocess.CalledProcessError(status, cmd)
