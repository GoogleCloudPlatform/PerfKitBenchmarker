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

"""Module containing abstract class for reliable resources.

The Resource class wraps unreliable create and delete commands in retry loops
and checks for resource existence so that resources can be created and deleted
reliably.
"""

import abc

from perfkitbenchmarker import errors
from perfkitbenchmarker import perfkitbenchmarker_lib


class BaseResource(object):
  """An object representing a cloud resource."""

  __metaclass__ = abc.ABCMeta

  def __init__(self):
    self.created = False
    self.deleted = False

  @abc.abstractmethod
  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

  @abc.abstractmethod
  def _Delete(self):
    """Deletes the underlying resource."""
    raise NotImplementedError()

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    raise NotImplementedError()

  def _PostCreate(self):
    """Method that will be called once after _CreateReource is called.

    Supplying this method is optional. If it is supplied, it will be called
    once, after the resource is confirmed to exist. It is intended to allow
    data about the resource to be collected or for the resource to be tagged.
    """
    pass

  def _CreateDependencies(self):
    """Method that will be called once before _CreateResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in creating resource dependencies separately from _Create().
    """
    pass

  def _DeleteDependencies(self):
    """Method that will be called once after _DeleteResource() is called.

    Supplying this method is optional. It is intended to allow additional
    flexibility in deleting resource dependencies separately from _Delete().
    """
    pass

  @perfkitbenchmarker_lib.Retry()
  def _CreateResource(self):
    """Reliably creates the underlying resource."""
    if self.created:
      return
    self._Create()
    try:
      if not self._Exists():
        raise errors.Error('Creation of %s failed.' % type(self).__name__)
    except NotImplementedError:
      pass
    self.created = True

  @perfkitbenchmarker_lib.Retry()
  def _DeleteResource(self):
    """Reliably deletes the underlying resource."""
    if self.deleted or not self.created:
      return
    self._Delete()
    try:
      if self._Exists():
        raise errors.Error('Deletion of %s failed.' % type(self).__name__)
    except NotImplementedError:
      pass
    self.deleted = True

  def Create(self):
    """Creates a resource and its dependencies."""
    self._CreateDependencies()
    self._CreateResource()
    self._PostCreate()

  def Delete(self):
    """Deletes a resource and its dependencie."""
    self._DeleteResource()
    self._DeleteDependencies()
