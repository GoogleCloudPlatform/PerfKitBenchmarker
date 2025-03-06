"""Base class for representing a job / instance."""

import threading
from typing import List, Type, TypeVar

from absl import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker.resources import jobs_setter


FLAGS = flags.FLAGS
# TODO: b/357673905 - Add Presubmit reminders to force image rebuild
# if container is modified.


class BaseJob(resource.BaseResource):
  """Base class for representing a cloud job / instance.

  Handles lifecycle management (creation & deletion) for a given job.

  Attributes:
    RESOURCE_TYPE: The class name.
    REQUIRED_ATTRS: Required field for subclass to override (one constant per
      subclass).
    region: Region the job should be in.
    backend: Amount of memory allocated.
    samples: Samples with lifecycle metrics (e.g. create time) recorded.
    container_image: Image to use for the job.
  """

  RESOURCE_TYPE: str = 'BaseJob'
  REQUIRED_ATTRS: List[str] = ['SERVICE']
  SERVICE: str
  _POLL_INTERVAL: int = 1
  _job_counter: int = 0
  _job_counter_lock: threading.Lock = threading.Lock()

  def __init__(
      self, base_job_spec: jobs_setter.BaseJobSpec, container_registry
  ):
    super().__init__()
    self.name: str = self._GenerateName()
    self.region: str = base_job_spec.job_region
    self.backend: str = base_job_spec.job_backend
    self.container_registry = container_registry
    self.job_spec = base_job_spec
    self.job_gpu_type: str = base_job_spec.job_gpu_type
    self.job_gpu_count: int = base_job_spec.job_gpu_count
    self.task_count: int = base_job_spec.task_count

    # update metadata
    self.metadata.update({
        'backend': self.backend,
        'region': self.region,
        'job_gpu_type': self.job_gpu_type,
        'job_gpu_count': self.job_gpu_count,
        'task_count': self.task_count,
        # 'concurrency': 'default',
    })
    self.samples: List[sample.Sample] = []

  def _GenerateName(self) -> str:
    """Generates a unique name for the job.

    Locking the counter variable allows for each created job name to be
    unique within the python process.

    Returns:
      The newly generated name.
    """
    with self._job_counter_lock:
      self.job_number: int = self._job_counter
      name: str = f'pkb-{FLAGS.run_uri}'
      name += f'-{self.job_number}'
      BaseJob._job_counter += 1
      return name

  def Execute(self):
    """Executes the job."""
    pass

  def _CreateDependencies(self):
    assert self.container_registry is not None
    self.container_image = (
        self.container_registry.GetOrBuild(self.job_spec.image_directory)
        + ':latest'
    )


JobChild = TypeVar('JobChild', bound='BaseJob')


def GetJobClass(service: str) -> Type[JobChild] | None:
  """Returns class of the given Job type.

  Args:
    service: String which matches an inherited BaseJob's required SERVICE value.
  """
  return resource.GetResourceClass(BaseJob, SERVICE=service)
