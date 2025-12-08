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

    self.client.get("/calculate", headers=headers, timeout=5.0)


class StagesShape(locust.LoadTestShape):
  """Locust LoadTestShape to simulate a "stepped" rampup.

  NB: Because VPA is (deliberately) slow to react, we need to specify a lengthy
  rampup, measured in hours, rather than seconds.
  """

  # pyformat: disable
  # pylint: disable=bad-whitespace
  _stages = [
      {"endtime":  1*60*60, "users":   1},  #   1 rps for 1h
      {"endtime":  4*60*60, "users":  20},  #  20 rps for 3h
      {"endtime":  5*60*60, "users":  40},  #  40 rps for 1h
      {"endtime":  6*60*60, "users":  60},  #  60 rps for 1h
      {"endtime":  7*60*60, "users":  90},  #  90 rps for 1h
      {"endtime":  9*60*60, "users": 120},  # 120 rps for 2h
      {"endtime": 11*60*60, "users": 150},  # 150 rps for 2h
      {"endtime": 12*60*60, "users":   1},  #   1 rps for 1h
      # --------------
      #     Total: 12h
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
