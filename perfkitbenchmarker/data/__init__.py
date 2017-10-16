# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""This module defines an interface for finding named resources.

Due to license restrictions, not all software dependences can be shipped with
PerfKitBenchmarker.
Those that can be included in perfkitbenchmarker/data, or
 perfkitbenchmarker/scripts and are loaded via a PackageResourceLoader.

Users can specify additional paths to search for required data files using the
`--data_search_paths` flag.
"""

import abc
import logging
import os
import shutil

import pkg_resources

import perfkitbenchmarker

from perfkitbenchmarker import flags
from perfkitbenchmarker import temp_dir

FLAGS = flags.FLAGS

flags.DEFINE_multi_string('data_search_paths', ['.'],
                          'Additional paths to search for data files. '
                          'These paths will be searched prior to using files '
                          'bundled with PerfKitBenchmarker.')

_RESOURCES = 'resources'


class ResourceNotFound(ValueError):
  """Error raised when a resource could not be found on the search path."""
  pass


class ResourceLoader(object):
  """An interface for loading named resources."""

  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def ResourceExists(self, name):
    """Checks for existence of the resource 'name'.

    Args:
      name: string. Name of the resource. Typically a file name.

    Returns:
      A boolean indicating whether the resource 'name' can be loaded by this
      object.
    """
    pass

  @abc.abstractmethod
  def ResourcePath(self, name):
    """Gets the path to the resource 'name'.

    Args:
      name: string. Name of the resource. Typically a file name.

    Returns:
      A full path to 'name' on the filesystem.

    Raises:
      ResourceNotFound: If 'name' was not found.
    """
    pass


class FileResourceLoader(ResourceLoader):
  """Loads resources from a directory in the filesystem.

  Attributes:
    path: string. Root path to load resources from.
  """

  def __init__(self, path):
    self.path = path

    if not os.path.isdir(path):
      logging.warn('File resource loader root %s is not a directory.', path)

  def __repr__(self):
    return '<{0} path="{1}">'.format(type(self).__name__, self.path)

  def _Join(self, *args):
    return os.path.join(self.path, *args)

  def ResourceExists(self, name):
    return os.path.exists(self._Join(name))

  def ResourcePath(self, name):
    if not self.ResourceExists(name):
      raise ResourceNotFound(name)
    return self._Join(name)


class PackageResourceLoader(ResourceLoader):
  """Loads resources from a Python package.

  Attributes:
    package: string. Name of the package containing resources.
  """
  def __init__(self, package):
    self.package = package

  def __repr__(self):
    return '<{0} package="{1}">'.format(type(self).__name__, self.package)

  def ResourceExists(self, name):
    return pkg_resources.resource_exists(self.package, name)

  def ResourcePath(self, name):
    if not self.ResourceExists(name):
      raise ResourceNotFound(name)
    try:
      path = pkg_resources.resource_filename(self.package, name)
    except NotImplementedError:
      # This can happen if PerfKit Benchmarker is executed from a zip file.
      # Extract the resource to the version-specific temporary directory.
      path = os.path.join(temp_dir.GetVersionDirPath(), _RESOURCES, name)
      if not os.path.exists(path):
        dir_path = os.path.dirname(path)
        try:
          os.makedirs(dir_path)
        except OSError:
          if not os.path.isdir(dir_path):
            raise
        with open(path, 'wb') as extracted_file:
          shutil.copyfileobj(pkg_resources.resource_stream(self.package, name),
                             extracted_file)
    return path


DATA_PACKAGE_NAME = 'perfkitbenchmarker.data'
YCSB_WORKLOAD_DIR_NAME = os.path.join(
    os.path.dirname(perfkitbenchmarker.__file__), 'data/ycsb')
SCRIPT_PACKAGE_NAME = 'perfkitbenchmarker.scripts'
CONFIG_PACKAGE_NAME = 'perfkitbenchmarker.configs'
DEFAULT_RESOURCE_LOADERS = [PackageResourceLoader(DATA_PACKAGE_NAME),
                            FileResourceLoader(YCSB_WORKLOAD_DIR_NAME),
                            PackageResourceLoader(SCRIPT_PACKAGE_NAME),
                            PackageResourceLoader(CONFIG_PACKAGE_NAME)]


def _GetResourceLoaders():
  """Gets a list of registered ResourceLoaders.

  Returns:
    List of ResourceLoader instances. FileResourceLoaders for paths in
    FLAGS.data_search_paths will be listed first, followed by
    DEFAULT_RESOURCE_LOADERS.
  """
  loaders = []

  # Add all paths to list if they are specified on the command line (will warn
  # if any are invalid).
  # Otherwise add members of the default list iff they exist.
  if FLAGS['data_search_paths'].present:
    for path in FLAGS.data_search_paths:
      loaders.append(FileResourceLoader(path))
  else:
    for path in FLAGS.data_search_paths:
      if os.path.isdir(path):
        loaders.append(FileResourceLoader(path))
  loaders.extend(DEFAULT_RESOURCE_LOADERS)
  return loaders


def ResourcePath(resource_name, search_user_paths=True):
  """Gets the filename of a resource.

  Loaders are searched in order until the resource is found.
  If no loader provides 'resource_name', an exception is thrown.

  If 'search_user_paths' is true, the directories specified by
  "--data_search_paths" are consulted before the default paths.

  Args:
    resource_name: string. Name of a resource.
    search_user_paths: boolean. Whether paths from "--data_search_paths" should
      be searched before the default paths.
  Returns:
    A path to the resource on the filesystem.
  Raises:
    ResourceNotFound: When resource was not found.
  """
  if search_user_paths:
    loaders = _GetResourceLoaders()
  else:
    loaders = DEFAULT_RESOURCE_LOADERS
  for loader in loaders:
    if loader.ResourceExists(resource_name):
      return loader.ResourcePath(resource_name)

  raise ResourceNotFound(
      '{0} (Searched: {1})'.format(resource_name, loaders))
