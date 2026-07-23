"""Methods to run kubectl commands against a cluster."""

import logging

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubernetes_events


class RetryableKubectlError(errors.VmUtil.IssueCommandTimeoutError):
  pass


# Error messages mostly from transient connection issues.
RETRYABLE_KUBECTL_ERRORS = [
    (
        '"unable to decode an event from the watch stream: http2: client'
        ' connection lost"'
    ),
    'read: connection reset by peer',
    'Unable to connect to the server: dial tcp',
    'Unable to connect to the server: net/http: TLS handshake timeout',
    # These come up in kubectl exec
    'error dialing backend:',
    'connect: connection timed out',
    'error sending request:',
    '(abnormal closure): unexpected EOF',
    'deadline exceeded',
    # kubectl wait/delete timeouts and connection errors
    # (retried in EKS cleanup)
    'timed out',
    'unable to connect to the server',
]

# Indicate the command timed out rather than hitting a retryable error.
_TIMEOUT_ERRORS = [
    'exceeded its progress deadline',
]


def _CheckForQuotaFailure(command: list[str]) -> None:
  """Check for quota failure errors in the stderr of a kubectl command."""
  cmd_str = ' '.join(command)
  if (
      not kubernetes_events.K8S_EVENT_POLLER or 'get events' in cmd_str
  ):  # Avoid infinite loop if get events times out.
    return
  try:
    events = kubernetes_events.K8S_EVENT_POLLER.GetAndLogFailureEvents()
    kubernetes_events.K8S_EVENT_POLLER.CheckForQuotaFailure(events)
  except vm_util.RetryError:
    logging.warning(
        'Failed to get events from K8s event poller. Proceeding'
        ' with original error.'
    )


def RunKubectlCommand(
    command: list[str],
    check_for_quota_failure: bool = True,
    **kwargs,
) -> tuple[str, str, int]:
  """Runs a kubectl command.

  Args:
    command: The kubectl command to run.
    check_for_quota_failure: Whether to check for quota failures when
      encountering suspect retryable/timeout error messages.
    **kwargs: Keyword arguments to pass to vm_util.IssueCommand.

  Returns:
    A tuple of (stdout, stderr, retcode).
  """
  kwargs = vm_util.IncrementStackLevel(**kwargs)
  cmd = [
      flags.FLAGS.kubectl,
      '--kubeconfig',
      flags.FLAGS.kubeconfig,
  ] + command

  orig_suppress_failure = None
  if 'suppress_failure' in kwargs:
    orig_suppress_failure = kwargs['suppress_failure']

  def _DetectTimeoutViaSuppressFailure(stdout, stderr, retcode):
    # Raise timeout error regardless of raise_on_failure - as the intended
    # semantics is to ignore expected errors caused by invoking the
    # command not errors from PKB infrastructure.
    raise_on_timeout = (
        kwargs['raise_on_timeout'] if 'raise_on_timeout' in kwargs else True
    )
    if retcode != 0 and raise_on_timeout:
      stderr_lower = stderr.lower()
      raisable_error_type = None
      for error_substring in RETRYABLE_KUBECTL_ERRORS:
        if error_substring.lower() in stderr_lower:
          raisable_error_type = RetryableKubectlError
          break
      for error_substring in _TIMEOUT_ERRORS:
        if error_substring.lower() in stderr_lower:
          raisable_error_type = errors.VmUtil.IssueCommandTimeoutError
          break
      if raisable_error_type:
        raise raisable_error_type(stderr)
    # Else, revert to user supplied kwargs values.
    if orig_suppress_failure is not None:
      return orig_suppress_failure(stdout, stderr, retcode)
    if 'raise_on_failure' in kwargs:
      return not kwargs['raise_on_failure']
    return False

  kwargs['suppress_failure'] = _DetectTimeoutViaSuppressFailure

  try:
    return vm_util.IssueCommand(cmd, **kwargs)
  except errors.VmUtil.IssueCommandTimeoutError:
    # Check for quota failure to catch both string errors above & timeouts from
    # within IssueCommand.
    if check_for_quota_failure:
      _CheckForQuotaFailure(command)
    raise


def RunRetryableKubectlCommand(
    run_cmd: list[str], timeout: int | None = None, **kwargs
) -> tuple[str, str, int]:
  """Runs a kubectl command, retrying somewhat expected errors."""
  if 'raise_on_timeout' in kwargs and kwargs['raise_on_timeout']:
    raise ValueError(
        'RunRetryableKubectlCommand does not allow `raise_on_timeout=True`'
        ' (since timeouts are retryable).'
    )

  kwargs = vm_util.IncrementStackLevel(**kwargs)

  @vm_util.Retry(
      timeout=timeout,
      retryable_exceptions=(RetryableKubectlError,),
  )
  def _RunRetryablePart(run_cmd: list[str], **kwargs) -> tuple[str, str, int]:
    """Inner function retries command so timeout can be passed to decorator."""
    kwargs['stack_level'] += 1
    return RunKubectlCommand(
        run_cmd, check_for_quota_failure=False, raise_on_timeout=True, **kwargs
    )

  try:
    return _RunRetryablePart(run_cmd, timeout=timeout, **kwargs)
  except (errors.VmUtil.IssueCommandTimeoutError, vm_util.RetryError):
    # Wait till the end to check for quota failures rather than during retries.
    _CheckForQuotaFailure(run_cmd)
    raise

