"""Methods to run kubectl commands against a cluster."""

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util

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
]


def RunKubectlCommand(command: list[str], **kwargs) -> tuple[str, str, int]:
  """Run a kubectl command."""
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
    # Check for kubectl timeout. If found, treat it the same as a regular
    # timeout.
    if retcode != 0:
      for error_substring in RETRYABLE_KUBECTL_ERRORS:
        if error_substring in stderr:
          # Raise timeout error regardless of raise_on_failure - as the intended
          # semantics is to ignore expected errors caused by invoking the
          # command not errors from PKB infrastructure.
          raise_on_timeout = (
              kwargs['raise_on_timeout']
              if 'raise_on_timeout' in kwargs
              else True
          )
          if raise_on_timeout:
            raise errors.VmUtil.IssueCommandTimeoutError(stderr)
    # Else, revert to user supplied kwargs values.
    if orig_suppress_failure is not None:
      return orig_suppress_failure(stdout, stderr, retcode)
    if 'raise_on_failure' in kwargs:
      return not kwargs['raise_on_failure']
    return False

  kwargs['suppress_failure'] = _DetectTimeoutViaSuppressFailure

  return vm_util.IssueCommand(cmd, **kwargs)


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
      retryable_exceptions=(errors.VmUtil.IssueCommandTimeoutError,),
  )
  def _RunRetryablePart(run_cmd: list[str], **kwargs) -> tuple[str, str, int]:
    """Inner function retries command so timeout can be passed to decorator."""
    kwargs['stack_level'] += 1
    return RunKubectlCommand(run_cmd, raise_on_timeout=True, **kwargs)

  return _RunRetryablePart(run_cmd, timeout=timeout, **kwargs)
