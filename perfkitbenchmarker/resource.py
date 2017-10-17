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

"""Module containing abstract class for reliable resources.

The Resource class wraps unreliable create and delete commands in retry loops
and checks for resource existence so that resources can be created and deleted
reliably.
"""

import abc
import time

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util


class BaseResource(object):
  """An object representing a cloud resource.

  Attributes:
    created: True if the resource has been created.
    pkb_managed: Whether the resource is managed (created and deleted) by PKB.
  """

  __metaclass__ = abc.ABCMeta

  def __init__(self, user_managed=False):
    super(BaseResource, self).__init__()
    self.created = user_managed
    self.deleted = user_managed
    self.user_managed = user_managed

    # Creation and deletion time information
    # that we may make use of later.
    self.create_start_time = None
    self.delete_start_time = None
    self.create_end_time = None
    self.delete_end_time = None
    self.resource_ready_time = None
    self.metadata = dict()

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata about the resource."""
    return self.metadata.copy()

  @abc.abstractmethod
  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

  @abc.abstractmethod
  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    raise NotImplementedError()

  def _Exists(self):
    """Returns true if the underlying resource exists.

    Supplying this method is optional. If it is not implemented then the
    default is to assume success when _Create and _Delete do not raise
    exceptions.
    """
    raise NotImplementedError()

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    return True

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

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def _CreateResource(self):
    """Reliably creates the underlying resource."""
    if self.created:
      return
    # Overwrite create_start_time each time this is called,
    # with the assumption that multple calls to Create() imply
    # that the resource was not actually being created on the
    # backend during previous failed attempts.
    self.create_start_time = time.time()
    self._Create()
    try:
      if not self._Exists():
        raise errors.Resource.RetryableCreationError(
            'Creation of %s failed.' % type(self).__name__)
    except NotImplementedError:
      pass
    self.created = True
    self.create_end_time = time.time()

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableDeletionError,))
  def _DeleteResource(self):
    """Reliably deletes the underlying resource."""
    if self.deleted:
      return
    if not self.delete_start_time:
      self.delete_start_time = time.time()
    self._Delete()
    try:
      if self._Exists():
        raise errors.Resource.RetryableDeletionError(
            'Deletion of %s failed.' % type(self).__name__)
    except NotImplementedError:
      pass
    self.deleted = True
    self.delete_end_time = time.time()

  def Create(self):
    """Creates a resource and its dependencies."""

    # A more general solution would allow the retry interval to be set as a
    # property of the class.  We don't currently need that.
    @vm_util.Retry(poll_interval=5, fuzz=0,
                   retryable_exceptions=(
                       errors.Resource.RetryableCreationError,))
    def WaitUntilReady():
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Not yet ready')

    if self.user_managed:
      return
    self._CreateDependencies()
    self._CreateResource()
    WaitUntilReady()
    if not self.resource_ready_time:
      self.resource_ready_time = time.time()
    self._PostCreate()

  def Delete(self):
    """Deletes a resource and its dependencies."""
    if self.user_managed:
      return
    self._DeleteResource()
    self._DeleteDependencies()
