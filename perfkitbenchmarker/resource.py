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
import logging
import time
from typing import Any, List, TypeVar

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import auto_registry

FLAGS = flags.FLAGS

_RESOURCE_REGISTRY = {}
RegisteredType = TypeVar('RegisteredType')
ResourceType = type[RegisteredType]

# Constants used for annotating resources with timeout metadata:
# GCP labels only allow hyphens (-), underscores (_), lowercase characters, and
# numbers and International characters.
# metadata allow all characters and numbers.
METADATA_TIME_FORMAT = '%Y%m%dt%H%M%Sz'
TIMEOUT_METADATA_KEY = 'timeout_utc'


def GetResourceClass(base_class: ResourceType, **kwargs) -> ResourceType:
  """Returns the subclass with the corresponding attributes.

  Args:
    base_class: The base class of the resource to return (e.g.
      BaseVirtualMachine).
    **kwargs: Every attribute/value of the subclass's REQUIRED_ATTRS that were
      used to register the subclass.

  Raises:
    Exception: If no class could be found with matching attributes.
  """
  return auto_registry.GetRegisteredClass(
      _RESOURCE_REGISTRY, base_class, None, **kwargs
  )


class AutoRegisterResourceMeta(abc.ABCMeta):
  """Metaclass which allows resources to automatically be registered."""

  # See BaseResource
  RESOURCE_TYPE: str
  REQUIRED_ATTRS: List[str]

  def __init__(cls, name, bases, dct):
    auto_registry.RegisterClass(
        _RESOURCE_REGISTRY, cls, cls.REQUIRED_ATTRS, cls.RESOURCE_TYPE
    )
    super().__init__(name, bases, dct)

  @classmethod
  def GetAttributes(mcs) -> list[tuple[Any, ...]]:
    """Override to manually set the attributes for registering the class."""
    return []


class BaseResource(metaclass=AutoRegisterResourceMeta):
  """An object representing a cloud resource.

  Attributes:
    created: True if the resource has been created.
    deleted: True if the resource has been deleted.
    user_managed: Whether Create() and Delete() should be skipped.
    restored: True if the resource has been restored.
    enable_freeze_restore: Whether the resource should use freeze/restore when
      the option is specified on the command line. Different benchmarks may want
      different resources to have freeze/restore enabled.
    create_on_restore_error: Whether to create the resource if there is an issue
      while restoring.
    delete_on_freeze_error: Whether to delete the resource if there is an issue
      while freezing.
    create_start_time: The start time of the last create.
    delete_start_time: The start time of the last delete.
    create_end_time: The end time of the last create.
    delete_end_time: The end time of the last delete.
    resource_ready_time: The time when the resource last became ready.
    metadata: Dictionary of resource metadata.
  """

  # The name of the base class (e.g. BaseVirtualMachine) that will be extended
  # with auto-registered subclasses.
  RESOURCE_TYPE = None
  # A list of attributes that are used to register Resource subclasses
  # (e.g. CLOUD).
  REQUIRED_ATTRS = ['CLOUD']

  # Timeout in seconds for resource to be ready.
  READY_TIMEOUT = None
  # Time between retries.
  POLL_INTERVAL = 5

  def __init__(
      self,
      user_managed=False,
      enable_freeze_restore=False,
      create_on_restore_error=False,
      delete_on_freeze_error=False,
  ):
    super().__init__()
    # Class level attributes does not persist after pickle
    # Copy required attributes to the object
    for attribute in self.REQUIRED_ATTRS:
      setattr(self, attribute, getattr(self, attribute, None))
    self.created = user_managed
    self.deleted = user_managed
    self.user_managed = user_managed
    self.restored: bool = False
    self.enable_freeze_restore = enable_freeze_restore
    self.create_on_restore_error = create_on_restore_error
    self.delete_on_freeze_error = delete_on_freeze_error

    # Creation and deletion time information
    # that we may make use of later.
    self.create_start_time: float = None
    self.delete_start_time = None
    self.create_end_time: float = None
    self.delete_end_time = None
    self.resource_ready_time: float = None
    self.metadata = dict()

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata about the resource."""
    return self.metadata.copy()

  @abc.abstractmethod
  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

  def _Restore(self) -> None:
    """Restores the underlying resource from a file.

    This method is required if using Restore() with a resource.
    """
    raise NotImplementedError()

  def _Freeze(self) -> None:
    """Freezes the underlying resource to a long-term, sustainable state.

    This method is required if using Restore() with a resource.
    """
    raise NotImplementedError()

  def _UpdateTimeout(self, timeout_minutes: int) -> None:
    """Updates the underlying resource's timeout after a successful freeze.

    This method is required if using Freeze()/Restore() with a resource.

    Args:
      timeout_minutes: The number of minutes past the current time at which the
        resource should be considered expired.
    """
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

  def _WaitUntilRunning(self):
    """Waits until the resource is (or was) running.

    Supplying this method is optional. Use it when a resource is created using
    an asynchronous create command and its status is verified as running via
    repeatedly polling the resource with 'describe' commands.
    """
    pass

  def _IsReady(self):
    """Return true if the underlying resource is ready.

    Supplying this method is optional.  Use it when a resource can exist
    without being ready.  If the subclass does not implement
    it then it just returns true.

    Returns:
      True if the resource was ready in time, False if the wait timed out.
    """
    return True

  def _IsDeleting(self):
    """Return true if the underlying resource is getting deleted.

    Supplying this method is optional.  Potentially use when the resource has an
    asynchronous deletion operation to avoid rerunning the deletion command and
    track the deletion time correctly. If the subclass does not implement it
    then it just returns false.

    Returns:
      True if the resource was being deleted, False if the resource was in a non
      deleting state.
    """
    return False

  def _PreDelete(self):
    """Method that will be called once before _DeleteResource() is called.

    Supplying this method is optional. If it is supplied, it will be called
    once, before attempting to delete the resource. It is intended to allow
    data about the resource to be collected right before it is deleted.
    """
    pass

  def _UserManagedSetup(self):
    """Method that will be called once during Create() when user_managed = True.

    Supplying this method is optional. If it is supplied, it will be called
    once during Create() if the resource is user_managed. It is intended to
    provide a place to populate important attributes (i.e. from describe) or
    perform setup that are needed for benchmarking but do not affect the
    resource lifecycle.
    """
    pass

  def _PostCreate(self):
    """Method that will be called once after _CreateResource() is called.

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
            'Creation of %s failed.' % type(self).__name__
        )
    except NotImplementedError:
      pass
    self._WaitUntilRunning()
    self.created = True
    self.create_end_time = time.time()

  @vm_util.Retry(
      retryable_exceptions=(errors.Resource.RetryableDeletionError,),
      timeout=3600,
  )
  def _DeleteResource(self):
    """Reliably deletes the underlying resource."""

    # Retryable method which allows waiting for deletion of the resource.
    @vm_util.Retry(
        poll_interval=self.POLL_INTERVAL,
        fuzz=0,
        timeout=3600,
        retryable_exceptions=(errors.Resource.RetryableDeletionError,),
    )
    def WaitUntilDeleted():
      if self._IsDeleting():
        raise errors.Resource.RetryableDeletionError('Not yet deleted')

    if self.deleted or not self.created:
      return
    if not self.delete_start_time:
      self.delete_start_time = time.time()
    self._Delete()
    WaitUntilDeleted()
    try:
      if self._Exists():
        raise errors.Resource.RetryableDeletionError(
            'Deletion of %s failed.' % type(self).__name__
        )
    except NotImplementedError:
      pass

  def Restore(self) -> None:
    """Restores a resource instead of creating it.

    Raises:
      RestoreError: Generic error encompassing restore failures.
    """
    # TODO(user): Add usage lock with labels to prevent multiple
    # benchmarks from using the same resource concurrently.
    logging.info('Restoring resource %s.', repr(self))

    try:
      self._Restore()
    except NotImplementedError as e:
      raise errors.Resource.RestoreError(
          f'Class {self.__class__} does not have _Restore() implemented but a '
          'restore file was provided.'
      ) from e
    except Exception as e:
      raise errors.Resource.RestoreError(
          f'Error restoring resource {repr(self)}'
      ) from e

    self.restored = True
    self.UpdateTimeout(FLAGS.timeout_minutes)

  def Create(self, restore: bool = False) -> None:
    """Creates a resource and its dependencies.

    Args:
      restore: Whether to restore the resource instead of creating. If
        enable_freeze_restore is false, this proceeds with creation.

    Raises:
      RestoreError: If there is an error while restoring.
    """

    if self.user_managed:
      self._UserManagedSetup()
      return

    if restore and self.enable_freeze_restore:
      try:
        self.Restore()
        return
      except errors.Resource.RestoreError:
        logging.exception(
            'Encountered an exception while attempting to Restore(). '
            'Creating: %s',
            self.create_on_restore_error,
        )
        if not self.create_on_restore_error:
          raise

    self._CreateDependencies()
    self._CreateResource()
    self._WaitUntilReady()
    if not self.resource_ready_time:
      self.resource_ready_time = time.time()
    self._PostCreate()

  def _WaitUntilReady(self) -> None:
    """Waits & retries until the resource is ready."""
    @vm_util.Retry(
        # Inner function needed to allow for self.POLL_INTERVAL as default.
        poll_interval=self.POLL_INTERVAL,
        fuzz=0,
        timeout=self.READY_TIMEOUT,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _InnerWaitUntilReady() -> None:
      if not self._IsReady():
        raise errors.Resource.RetryableCreationError('Not yet ready')
    _InnerWaitUntilReady()

  def Freeze(self) -> None:
    """Freezes a resource instead of deleting it.

    Raises:
      FreezeError: Generic error encompassing freeze failures.
    """
    logging.info('Freezing resource %s.', repr(self))
    # Attempt to call freeze, failing if unimplemented.
    try:
      self._Freeze()
    except NotImplementedError as e:
      raise errors.Resource.FreezeError(
          f'Class {self.__class__} does not have _Freeze() implemented but '
          'Freeze() was called.'
      ) from e
    except Exception as e:
      raise errors.Resource.FreezeError(
          f'Error freezing resource {repr(self)}'
      ) from e

    # If frozen successfully, attempt to update the timeout.
    self.restored = False
    self.UpdateTimeout(FLAGS.persistent_timeout_minutes)

  def Delete(self, freeze: bool = False) -> None:
    """Deletes a resource and its dependencies.

    Args:
      freeze: Whether to freeze the resource instead of deleting. If
        enable_freeze_restore is false, this proceeds with deletion.

    Raises:
      FreezeError: If there is an error while freezing.
    """
    if self.user_managed:
      return

    # Some resources (specifically VMs) lazily compute their metadata rather
    # than computing it after provisioning and stashing in their metadata dict
    # or static fields as they are supposed to.
    # Asking for metadata before deleting it should cache it and make it
    # available after we tear down resources, which is necessary for attaching
    # metadata in benchmark_spec.GetSamples()
    self.GetResourceMetadata()

    if freeze and self.enable_freeze_restore:
      try:
        self.Freeze()
        return
      except errors.Resource.FreezeError:
        logging.exception(
            'Encountered an exception while attempting to Freeze(). '
            'Deleting: %s',
            self.delete_on_freeze_error,
        )
        if not self.delete_on_freeze_error:
          raise

    self._PreDelete()
    self._DeleteResource()
    self.deleted = True
    self.delete_end_time = time.time()
    self._DeleteDependencies()

  def UpdateTimeout(self, timeout_minutes: int) -> None:
    """Updates the timeout of the underlying resource.

    Args:
      timeout_minutes: The number of minutes past the current time at which the
        resource should be considered expired.

    Raises:
      NotImplementedError: If the resource has not implemented _UpdateTimeout().
    """
    logging.info('Updating timeout for %s.', repr(self))
    try:
      self._UpdateTimeout(timeout_minutes)
    except NotImplementedError:
      logging.exception(
          'Class %s does not have _UpdateTimeout() implemented, which is '
          'needed for Freeze(). Please add an implementation.',
          self.__class__,
      )
      raise

  def GetSamples(self) -> List[sample.Sample]:
    """Get samples relating to the provisioning of the resource."""
    # This should not be necessary. Resources are responsible to wire their
    # GetResourceMetadata into publisher.py, but some do not.
    metadata = self.GetResourceMetadata()
    metadata['resource_type'] = self.RESOURCE_TYPE
    metadata['resource_class'] = self.__class__.__name__

    samples = []
    if self.create_start_time and self.create_end_time:
      samples.append(
          sample.Sample(
              'Time to Create',
              self.create_end_time - self.create_start_time,
              'seconds',
              metadata,
          )
      )
    if self.create_start_time and self.resource_ready_time:
      samples.append(
          sample.Sample(
              'Time to Ready',
              self.resource_ready_time - self.create_start_time,
              'seconds',
              metadata,
          )
      )
    if self.delete_start_time and self.delete_end_time:
      samples.append(
          sample.Sample(
              'Time to Delete',
              self.delete_end_time - self.delete_start_time,
              'seconds',
              metadata,
          )
      )
    return samples

  def CheckPrerequisites(self) -> None:
    """Checks preconditions for the resource.

    Requires resource to be checked in benchmark_spec.CheckPrerequisites()
    Allows for per-provider validation not available in
    benchmark.CheckPrerequisites(config).

    Raises:
      ValueError: If there is a validation issue.
    """
    pass
