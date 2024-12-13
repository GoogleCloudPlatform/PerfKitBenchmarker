"""Locust file to simulate a "stepped" rampup of load."""

import locust


class Rampup(locust.HttpUser):
  # Send 1QPS (per user)
  wait_time = locust.constant_throughput(1)

  @locust.task
  def rampup(self):
    # Close the connection after each request (or else users won't get load
    # balanced to new pods.)
    headers = {"Connection": "close"}

    self.client.get("/calculate", headers=headers)


class StagesShape(locust.LoadTestShape):
  """Locust LoadTestShape to simulate a "stepped" rampup."""

  # pyformat: disable
  # pylint: disable=bad-whitespace
  _stages = [
      {"endtime":  60, "users":   1},  #   1 rps for 1m
      {"endtime": 360, "users":  20},  #  20 rps for 5m
      {"endtime": 420, "users":  40},  #  40 rps for 1m
      {"endtime": 480, "users":  60},  #  60 rps for 1m
      {"endtime": 540, "users":  90},  #  90 rps for 1m
      {"endtime": 660, "users": 120},  # 120 rps for 2m
      {"endtime": 780, "users": 150},  # 150 rps for 2m
      {"endtime": 900, "users":   1},  #   1 rps for 2m
      # --------------
      #     Total: 15m
  ]
  # pyformat: enable

  def tick(self):
    run_time = self.get_run_time()

    for stage in self._stages:
      if run_time < stage["endtime"]:
        user_count = stage["users"]
        spawn_rate = 100  # spawn all new users roughly immediately (over 1s)
        return (user_count, spawn_rate)

    return None
