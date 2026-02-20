"""Errors for container_service."""

from perfkitbenchmarker import errors


class ContainerError(errors.Error):
  """Exception during the creation or execution of a container."""


class FatalContainerError(
    errors.Resource.CreationError, ContainerError
):
  """Fatal Exception during the creation or execution of a container."""

  pass


class RetriableContainerError(
    errors.Resource.RetryableCreationError, ContainerError
):
  """Retriable Exception during the creation or execution of a container."""

  pass
