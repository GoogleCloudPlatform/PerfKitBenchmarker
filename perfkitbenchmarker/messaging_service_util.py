"""Messaging service util functions.

General utility functions used on the messaging service benchmark.
"""
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import messaging_service
from perfkitbenchmarker.providers.gcp import gcp_pubsub


def get_instance(
    client: linux_virtual_machine.BaseLinuxVirtualMachine,
    cloud: str) -> messaging_service.MessagingService:
  """Get messaging service cloud specific implementation based on 'cloud'."""
  if cloud == 'GCP':
    instance = gcp_pubsub.GCPCloudPubSub(client)
    return instance
  elif cloud == 'AWS':
    raise NotImplementedError
  elif cloud == 'Azure':
    raise NotImplementedError
  raise NotImplementedError
