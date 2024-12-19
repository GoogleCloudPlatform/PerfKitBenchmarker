"""Locust file to flood the SUT."""

import locust


class Simple(locust.HttpUser):

  @locust.task
  def simple(self):
    # Close the connection after each request (or else users won't get load
    # balanced to new pods.)
    headers = {"Connection": "close"}

    self.client.get("/calculate", headers=headers)


class TimedFlatTest(locust.LoadTestShape):
  """Locust LoadTestShape which just runs for 10 minutes."""

  def tick(self):
    run_time = self.get_run_time()

    if run_time > 600:
      return None
    user_count = 1
    spawn_rate = 1
    return (user_count, spawn_rate)
