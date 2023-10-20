"""Tests for http_poller."""

import unittest
from unittest import mock

from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import http_poller


_ENDPOINT = "http://website.com"


class HttpPollerTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.vm = mock.create_autospec(virtual_machine.BaseVirtualMachine)
    self.http_poller = http_poller.HttpPoller()

  def testPollSuccess(self):
    self.vm.RemoteCommand.return_value = (
        "(0.5, 'succeed', 'hello', 'log')",
        "",
    )
    self.assertEqual(
        self.http_poller.Run(self.vm, _ENDPOINT),
        http_poller.PollingResponse(
            success=True, latency=0.5, response="hello"
        ),
    )

  def testPollFailureInvalidResponse(self):
    self.vm.RemoteCommand.return_value = (
        "(0.5, 'invalid_response', 'world', 'log')",
        "",
    )
    self.assertEqual(
        self.http_poller.Run(self.vm, _ENDPOINT, expected_response="hello"),
        http_poller.PollingResponse(
            success=False, latency=0.5, response="world"
        ),
    )

  def testRunsDefaultCommand(self):
    self.vm.RemoteCommand.return_value = (
        "(0.5, 'succeed', 'hello', 'log')",
        "",
    )
    self.http_poller.Run(self.vm, _ENDPOINT)
    self.vm.RemoteCommand.assert_called_once_with(
        "python3 poll_http_endpoint.py --endpoint=http://website.com "
        "--max_retries=0 --retry_interval=0.5 --timeout=600"
    )


if __name__ == "__main__":
  unittest.main()
