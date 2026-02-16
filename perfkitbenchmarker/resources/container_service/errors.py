"""Errors for container_service."""

from perfkitbenchmarker import errors


class ContainerException(errors.Error):
  """Exception during the creation or execution of a container."""


class FatalContainerException(
    errors.Resource.CreationError, ContainerException
):
  """Fatal Exception during the creation or execution of a container."""

  pass


class RetriableContainerException(
    errors.Resource.RetryableCreationError, ContainerException
):
  """Retriable Exception during the creation or execution of a container."""

  pass
