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

"""An interface to object storage services."""


import abc
import logging
import os
import pathlib
from typing import Optional

from absl import flags
from perfkitbenchmarker import errors
import six

flags.DEFINE_string('object_storage_credential_file', None,
                    'Directory of credential file.')
flags.DEFINE_string('boto_file_location', None,
                    'The location of the boto file.')

FLAGS = flags.FLAGS

DEFAULT_BOTO_LOCATION_USER = '~/.boto'
DEFAULT_BOTO_LOCATION_MACHINE = '/etc/boto.cfg'
BOTO_LIB_VERSION = 'boto_lib_version'

_OBJECT_STORAGE_REGISTRY = {}


class AutoRegisterObjectStorageMeta(abc.ABCMeta):
  """Metaclass for auto registration."""
  STORAGE_NAME = None

  def __init__(cls, name, bases, dct):
    super(AutoRegisterObjectStorageMeta, cls).__init__(name, bases, dct)
    if cls.STORAGE_NAME in _OBJECT_STORAGE_REGISTRY:
      logging.info(
          "Duplicate storage implementations for name '%s'. "
          'Replacing %s with %s', cls.STORAGE_NAME,
          _OBJECT_STORAGE_REGISTRY[cls.STORAGE_NAME].__name__, cls.__name__)
    _OBJECT_STORAGE_REGISTRY[cls.STORAGE_NAME] = cls


class ObjectStorageService(
    six.with_metaclass(AutoRegisterObjectStorageMeta, object)):
  """Base class for ObjectStorageServices."""

  # Keeping the location in the service object is not very clean, but
  # a nicer solution would be more complex, and we only use different
  # locations in a very limited way. Also, true multi-location
  # providers would require another abstraction for Azure service
  # accounts, which would add more complexity.

  # Service object lifecycle

  def PrepareService(self, location):
    """Get ready to use object storage.

    This method should be called before any other method of this
    class. Once it is called, all of this class' methods should work
    with data in the given location.

    Args:
      location: where to place our data.
    """
    pass

  def CleanupService(self):
    """Clean up what we did.

    No other method of this class should be called after
    CleanupProvider.
    """
    pass

  # Bucket management

  @abc.abstractmethod
  def MakeBucket(self, bucket, raise_on_failure=True):
    """Make an object storage bucket.

    Args:
      bucket: the name of the bucket to create.
      raise_on_failure: Whether to raise errors.Benchmarks.BucketCreationError
          if the bucket fails to be created.
    """
    pass

  @abc.abstractmethod
  def Copy(self, src_url, dst_url, recursive=False):
    """Copy files, objects and directories.

    Note: Recursive copy behavior mimics gsutil cp -r where:
    Copy(/foo/bar, /baz, True) copies the directory bar into /baz/bar whereas
    aws s3 cp --recursive would copy the contents of bar into /baz.

    Args:
      src_url: string, the source url path.
      dst_url: string, the destination url path.
      recursive: whether to copy directories.
    """
    pass

  @abc.abstractmethod
  def CopyToBucket(self, src_path, bucket, object_path):
    """Copy a local file to a bucket.

    Args:
      src_path: string, the local source path.
      bucket: string, the destination bucket.
      object_path: string, the object's path in the bucket.
    """
    pass

  @abc.abstractmethod
  def MakeRemoteCliDownloadUrl(self, bucket, object_path):
    """Creates a download url for an object in a bucket.

    This is used by GenerateCliDownloadFileCommand().

    Args:
      bucket: string, the name of the bucket.
      object_path: string, the path of the object in the bucket.
    """
    pass

  @abc.abstractmethod
  def GenerateCliDownloadFileCommand(self, src_url, local_path):
    """Generates a CLI command to copy src_url to local_path.

    This is suitable for use in scripts e.g. startup scripts.

    Args:
      src_url: string, the source url path.
      local_path: string, the local path.
    """
    pass

  @abc.abstractmethod
  def List(self, bucket):
    """List providers, buckets, or objects.

    Args:
      bucket: the name of the bucket to list the contents of.
    """
    pass

  def ListTopLevelSubfolders(self, bucket):
    """Lists the top level folders (not files) in a bucket.

    Args:
      bucket: Name of the bucket to list the top level subfolders of.

    Returns:
      A list of top level subfolder names. Can be empty if there are no folders.
    """
    return []

  @abc.abstractmethod
  def DeleteBucket(self, bucket):
    """Delete an object storage bucket.

    This method should succeed even if bucket contains objects.

    Args:
      bucket: the name of the bucket to delete.
    """
    pass

  @abc.abstractmethod
  def EmptyBucket(self, bucket):
    """Empty an object storage bucket.

    Args:
      bucket: the name of the bucket to empty.
    """
    pass

  # Working with a VM

  def PrepareVM(self, vm):
    """Prepare a VM to use object storage.

    Args:
      vm: the VM to prepare.
    """
    pass

  def CleanupVM(self, vm):
    """Clean up a VM that was used in this benchmark.

    Args:
      vm: the VM to clean up.
    """
    pass

  # CLI commands

  @abc.abstractmethod
  def CLIUploadDirectory(self, vm, directory, file_names, bucket):
    """Upload directory contents to a bucket through the CLI.

    The VM must have had PrepareVM called on it first. The command
    will be wrapped in 'time ...'.

    The caller must ensure that file_names is a full list of files in
    the directory, so the provider implementation can either use a
    generic "upload directory" command or use the file names. This
    method *must* pass all file names to the CLI at once if possible,
    not in a loop, to give it the chance to share connections and
    overlap uploads.

    Args:
      vm: the VM to run commands on directory: the directory to
      directory: the directory to upload files from
      file_names: a list of paths (relative to directory) to upload
      bucket: the bucket to upload the file to

    Returns:
      A tuple of the (stdout, stderr) of the command.
    """
    pass

  @abc.abstractmethod
  def CLIDownloadBucket(self, vm, bucket, objects, dest):
    """Download bucket contents to a folder.

    The VM must have had PrepareVM called on it first. The command
    will be wrapped in 'time ...'.

    The caller must ensure that objects is a full list of objects in
    the bucket, so the provider implementation can either use a
    generic "download bucket" command or use the object names. This
    method *must* pass all object names to the CLI at once if
    possible, not in a loop, to give it the chance to share
    connections and overlap downloads.

    Args:
      vm: the VM to run commands on
      bucket: the name of the bucket to download from
      objects: a list of names of objects to download
      dest: the name of the folder to download to

    Returns:
      A tuple of the (stdout, stderr) of the command.
    """
    pass

  # General methods

  def Metadata(self, vm):
    """Provider-specific metadata for collected samples.

    Args:
      vm: the VM we're running on.

    Returns:
      A dict of key, value pairs to add to our sample metadata.
    """

    return {}

  def UpdateSampleMetadata(self, samples):
    """Updates metadata of samples with provider specific information.

    Args:
      samples: the samples that need the metadata to be updated with provider
        specific information.
    """
    pass

  def GetDownloadUrl(self,
                     bucket: str,
                     object_name: str,
                     use_https=True) -> str:
    """Get the URL to download objects over HTTP(S).

    Args:
      bucket: name of bucket
      object_name: name of object
      use_https: whether to use HTTPS or else HTTP

    Returns:
      The URL to download objects over.
    """
    raise NotImplementedError

  def GetUploadUrl(self, bucket: str, object_name: str, use_https=True) -> str:
    """Get the URL to upload objects over HTTP(S).

    Args:
      bucket: name of bucket
      object_name: name of object
      use_https: whether to use HTTPS or else HTTP

    Returns:
      The URL to upload objects over.
    """
    return self.GetDownloadUrl(bucket, object_name, use_https)

  # Different services require uploads to be POST or PUT.
  UPLOAD_HTTP_METHOD: Optional[str] = None

  def MakeBucketPubliclyReadable(self, bucket: str, also_make_writable=False):
    """Make a bucket readable and optionally writable by everyone."""
    raise NotImplementedError

  def APIScriptArgs(self):
    """Extra arguments for the API test script.

    The service implementation has two parts - one that runs in the
    PKB controller, and one that runs on worker VMs. This method is
    how the controller communicates service-specific information to
    the workers.

    Returns:
      A list of strings, which will be passed as arguments to the API
      test script.
    """

    return []

  @classmethod
  def APIScriptFiles(cls):
    """Files to upload for the API test script.

    Returns:
      A list of file names. These files will be uploaded to the remote
      VM if this service's API is being benchmarked.
    """

    return []


def GetObjectStorageClass(storage_name) -> type(ObjectStorageService):
  """Return the ObjectStorageService subclass corresponding to storage_name."""

  return _OBJECT_STORAGE_REGISTRY[storage_name]


# TODO(user): Move somewhere more generic
def FindCredentialFile(default_location):
  """Return the path to the credential file."""

  credential_file = (
      FLAGS.object_storage_credential_file or default_location)
  credential_file = os.path.expanduser(credential_file)
  if not (os.path.isfile(credential_file) or
          os.path.isdir(credential_file)):
    raise errors.Benchmarks.MissingObjectCredentialException(
        'Credential cannot be found in %s' % credential_file)

  return credential_file


def FindBotoFile():
  """Return the path to the boto file."""
  paths_to_check = [
      FLAGS.boto_file_location,
      DEFAULT_BOTO_LOCATION_USER,
      DEFAULT_BOTO_LOCATION_MACHINE,
  ]

  for path in paths_to_check:
    if not path:
      continue
    if pathlib.Path(path).exists():
      return path

  raise errors.Benchmarks.MissingObjectCredentialException(
      'Boto file cannot be found in %s.' % paths_to_check)
