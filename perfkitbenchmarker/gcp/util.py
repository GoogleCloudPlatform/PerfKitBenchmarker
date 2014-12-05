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
"""Utilities for working with Google Cloud Platform resources."""

import gflags as flags

flags.DEFINE_string('gcloud_path',
                    'gcloud',
                    'The path for the gcloud utility.')

flags.DEFINE_list('additional_gcloud_flags',
                  [],
                  'Additional flags to pass to gcloud.')

FLAGS = flags.FLAGS


def GetDefaultGcloudFlags(resource):
  """Return common set of gcloud flags, including (format, project, zone).

  Args:
    resource: A GCE resource of type BaseResource.

  Returns:
    A common set of gcloud options.
  """
  options = [
      '--project', resource.project,
      '--format', 'json',
      '--quiet'
  ]
  if hasattr(resource, 'zone'):
    options.extend(['--zone', resource.zone])
  options.extend(FLAGS.additional_gcloud_flags)

  return options
