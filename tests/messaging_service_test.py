"""Tests for messaging_service.py.

This is the common interface used by the benchmark VM to run the benchmark.
"""

import os
import unittest

import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.messaging_service import MessagingService
from tests import pkb_common_test_case

MESSAGING_SERVICE_DATA_DIR = 'messaging_service'


class MessagingServiceTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.client = mock.Mock()
    self.messaging_service = MessagingService(self.client)

  def _MockIssueCommand(self):
    return self.enter_context(mock.patch.object(vm_util, 'IssueCommand'))

  def testPrepare(self):
    self._MockIssueCommand()

    self.messaging_service.prepare()

    packages = [
        mock.call('sudo apt-get update'),
        mock.call('sudo apt-get install python3'),
        mock.call('sudo apt-get install -y python3-pip'),
        mock.call('sudo pip3 install --upgrade pip'),
        mock.call('sudo pip3 install absl-py'),
        mock.call('sudo pip3 install numpy')
    ]
    self.client.RemoteCommand.assert_has_calls(packages)

    datafile_path = os.path.join(MESSAGING_SERVICE_DATA_DIR,
                                 'messaging_service_client.py')
    self.client.PushDataFile.assert_called_with(datafile_path)

if __name__ == '__main__':
  unittest.main()
