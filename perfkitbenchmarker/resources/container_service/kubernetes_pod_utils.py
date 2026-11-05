"""Kubernetes Pod utility functions."""

import logging
import os
import posixpath
import stat
from typing import Optional

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl

FLAGS = flags.FLAGS


def _RunPodCommand(
    pod_name: str, namespace: str, cmd: str
) -> tuple[str, str, int]:
  """Runs a shell command on the pod and returns (stdout, stderr, retcode)."""
  exec_cmd = ['exec', pod_name]
  if namespace:
    exec_cmd.extend(['-n', namespace])
  exec_cmd.extend(['--', 'bash', '-c', cmd])
  return kubectl.RunKubectlCommand(exec_cmd, raise_on_failure=False)


def RemoteHostCopy(
    pod_name: str,
    namespace: str,
    file_path: str,
    remote_path: str = '',
    copy_to: bool = True,
    retries: Optional[int] = None,
) -> None:
  """Copies a file to or from the pod."""
  ns_prefix = f'{namespace}/' if namespace else ''

  if copy_to:
    file_name = posixpath.basename(file_path)
    src_spec, dest_spec = (
        file_path,
        f'{ns_prefix}{pod_name}:{remote_path or file_name}',
    )
  else:
    stdout, _, _ = _RunPodCommand(
        pod_name, namespace, f'readlink -f {remote_path}'
    )
    remote_path_resolved = stdout.strip()
    file_name = posixpath.basename(remote_path_resolved)
    try:
      if stat.S_ISDIR(os.stat(file_path).st_mode):
        file_path = os.path.join(file_path, file_name)
    except FileNotFoundError:
      pass
    src_spec, dest_spec = (
        f'{ns_prefix}{pod_name}:{remote_path_resolved}',
        file_path,
    )

  if retries is None:
    retries = getattr(FLAGS, 'ssh_retries', 3)

  stdout, stderr, retcode = '', '', -1
  cmd = []
  for _ in range(retries):
    cmd = [FLAGS.kubectl, f'--kubeconfig={FLAGS.kubeconfig}']
    if namespace:
      cmd.extend(['-n', namespace])
    cmd.extend(['cp', src_spec, dest_spec])

    stdout, stderr, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode == 0:
      break

    if (
        retcode == 137
        or 'error: error upgrading connection' in stderr
        or 'error: Upgrade request required' in stderr
        or 'http2: server sent GOAWAY and closed the connection' in stderr
    ):
      logging.info('Retrying ephemeral connection issue\n:%s', stderr)
      continue
    break

  if retcode:
    error_text = (
        'Got non-zero return code (%s) executing %s\nSTDOUT: %sSTDERR: %s'
        % (retcode, ' '.join(cmd), stdout, stderr)
    )
    raise errors.VmUtil.IssueCommandError(error_text)

  if copy_to:
    remote_path = remote_path or file_name
    _RunPodCommand(
        pod_name,
        namespace,
        'mv %s %s; chmod 755 %s' % (file_name, remote_path, remote_path),
    )

  if not stat.S_ISDIR(os.stat(file_path).st_mode):
    local_size = os.path.getsize(file_path)
    stdout, _, _ = _RunPodCommand(
        pod_name, namespace, f'stat -c %s {remote_path}'
    )
    remote_size = int(stdout.strip())
    if local_size != remote_size:
      raise errors.VmUtil.IssueCommandError(
          f'Failed to copy {file_name}. '
          f'Remote size {remote_size} != local size {local_size}'
      )


def PushFile(
    pod_name: str, namespace: str, local_path: str, remote_path: str = ''
) -> None:
  """Pushes a file from the local machine to the pod."""
  RemoteHostCopy(pod_name, namespace, local_path, remote_path, copy_to=True)


def PushDataFile(
    pod_name: str, namespace: str, data_filename: str, remote_path: str = ''
) -> str:
  """Pushes a file from the data directory to the remote host."""
  local_path = data.ResourcePath(data_filename)
  if not remote_path:
    remote_path = posixpath.join(
        vm_util.VM_TMP_DIR, os.path.basename(data_filename)
    )
  PushFile(pod_name, namespace, local_path, remote_path)
  return remote_path
