"""Locust file to flood the SUT."""

from locust import HttpUser
from locust import task


class Simple(HttpUser):

  @task
  def simple(self):
    # Close the connection after each request (or else users won't get load
    # balanced to new pods.)
    headers = {"Connection": "close"}

    self.client.get("/calculate", headers=headers)
