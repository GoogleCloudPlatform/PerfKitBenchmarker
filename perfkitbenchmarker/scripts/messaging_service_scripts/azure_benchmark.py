"""Main script for Azure messaging service benchmark."""

from absl import app as absl_app

from perfkitbenchmarker.scripts.messaging_service_scripts.azure import azure_service_bus_client
from perfkitbenchmarker.scripts.messaging_service_scripts.common import app

if __name__ == '__main__':
  absl_app.run(
      app.App.for_client(azure_service_bus_client.AzureServiceBusClient))
