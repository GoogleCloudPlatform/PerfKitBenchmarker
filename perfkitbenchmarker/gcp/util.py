"""Utilities for working with Google Cloud Platform resources."""

import gflags as flags

flags.DEFINE_string('gcloud_path',
                    'gcloud',
                    'The path for the gcloud utility.')

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

  return options
