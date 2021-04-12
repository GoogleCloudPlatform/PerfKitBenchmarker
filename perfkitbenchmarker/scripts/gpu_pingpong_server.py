"""TensorFlow Server for GPU PingPong test."""

from typing import Sequence
from absl import app
import tensorflow as tf

JOB_NAME = 'worker'


def RunServer(server1: str, server2: str, task_index: int) -> None:
  cluster_spec = tf.train.ClusterSpec({JOB_NAME: [server1, server2]})
  server = tf.distribute.Server(
      cluster_spec, job_name=JOB_NAME, task_index=task_index)
  server.join()


def main(argv: Sequence[str]) -> None:
  RunServer(argv[1], argv[2], int(argv[3]))


if __name__ == '__main__':
  app.run(main)
