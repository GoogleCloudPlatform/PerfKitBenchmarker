"""Tests for data/http/poll_http_endpoint."""

import subprocess
import unittest
from unittest import mock
from absl.testing import parameterized
from perfkitbenchmarker.data.http import poll_http_endpoint
from tests import pkb_common_test_case


def fake_subprocess_popen_output(fake_out, fake_err):
  def fake_subprocess_popen(command, stdout, stderr, shell):
    del command, shell  # Unused by the fake but needed by the real version.
    stdout.write(fake_out)
    stderr.write(fake_err)
    m = mock.create_autospec(subprocess.Popen, instance=True)
    m.wait.return_value = 0
    return m

  return fake_subprocess_popen


class PollHttpEndpointTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    self.out_file = self.create_tempfile()
    self.err_file = self.create_tempfile()
    self.temp_file_names = (self.out_file.full_path, self.err_file.full_path)
    self.enter_context(
        mock.patch.object(
            poll_http_endpoint,
            '_generate_file_names',
            return_value=self.temp_file_names,
        )
    )
    self.mock_subprocess = self.enter_context(
        mock.patch.object(poll_http_endpoint, '_popen')
    )

  @parameterized.named_parameters(
      (
          '400 request fails',
          '< HTTP/1.1 400 Bad Request',
          'invalid_response_code',
          400,
      ),
      ('200 request passes', '< HTTP/1.1 200 OK'
       , 'succeed', 200),
  )
  def test_call_curl_response_types(
      self, response_stderr, expected_response_type, expected_response_code
  ):
    self.mock_subprocess.side_effect = fake_subprocess_popen_output(
        'hello world<curl_time_total:0.5>', response_stderr
    )
    latency, response_type, response_code, response = (
        poll_http_endpoint.call_curl(
            'http://site.com', expected_response_code=r'2\d\d'
        )
    )
    self.assertEqual(response_type, expected_response_type)
    self.assertEqual(response_code, expected_response_code)
    self.assertAlmostEqual(latency, 0.5)
    self.assertEqual(response, 'hello world<curl_time_total:0.5>')

  @parameterized.named_parameters(
      ('response match', 'succeed', 'hello world'),
      ('response not match', 'invalid_response', 'hi folks'),
  )
  def test_call_curl_matching_expected_response(
      self, response_type, expected_response
  ):
    self.mock_subprocess.side_effect = fake_subprocess_popen_output(
        'hello world<curl_time_total:0.5>', '< HTTP/1.1 200 OK'
    )
    _, response_type, response_code, _ = poll_http_endpoint.call_curl(
        'http://site.com',
        expected_response_code=r'2\d\d',
        expected_response=expected_response,
    )
    self.assertEqual(response_type, response_type)
    self.assertEqual(response_code, 200)

  @parameterized.named_parameters(
      (
          '200 request succeeds',
          '< HTTP/1.1 200 OK',
          'succeed',
          0.5,
          1,
      ),
      (
          '400 request retries',
          '< HTTP/1.1 400 Bad Request',
          'invalid_response_code',
          -1,
          3,
      ),
  )
  def test_poll(
      self,
      response_stderr,
      expected_response_type,
      expected_latency,
      num_retries,
  ):
    self.mock_subprocess.side_effect = fake_subprocess_popen_output(
        'hello world<curl_time_total:0.5>', response_stderr
    )
    _, latency, response_type, response = poll_http_endpoint.poll(
        'http://site.com',
        expected_response_code=r'2\d\d',
        max_retries=2,
    )
    self.assertAlmostEqual(latency, expected_latency)
    self.assertEqual(response_type, expected_response_type)
    self.assertEqual(response, 'hello world<curl_time_total:0.5>')
    self.assertEqual(self.mock_subprocess.call_count, num_retries)

  def test_poll_fails_with_timeout(self):
    mock_curl = self.enter_context(
        mock.patch.object(
            poll_http_endpoint,
            'call_curl',
            return_value=(1, 'invalid_response_code', '', 'hello'),
        )
    )
    self.enter_context(
        mock.patch.object(
            poll_http_endpoint,
            '_get_time',
            side_effect=[10, 300, 600, 610],
        )
    )
    poll_http_endpoint.poll(
        'http://site.com',
        expected_response_code=r'2\d\d',
        max_retries=10,
        timeout=500,
    )
    self.assertEqual(mock_curl.call_count, 2)


if __name__ == '__main__':
  unittest.main()
