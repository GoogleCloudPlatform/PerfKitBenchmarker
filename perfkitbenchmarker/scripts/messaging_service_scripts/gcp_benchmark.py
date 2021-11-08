"""Main script for GCP messaging service benchmark."""

from absl import app as absl_app

from perfkitbenchmarker.scripts.messaging_service_scripts.common import app
from perfkitbenchmarker.scripts.messaging_service_scripts.gcp import gcp_pubsub_client

if __name__ == '__main__':
  absl_app.run(app.App.for_client(gcp_pubsub_client.GCPPubSubClient))
