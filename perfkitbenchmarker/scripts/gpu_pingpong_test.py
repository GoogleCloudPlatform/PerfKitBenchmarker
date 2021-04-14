"""GPU PingPong test script."""
import logging
from typing import Sequence, Tuple
from absl import app
import tensorflow as tf


def Run(server: str) -> None:
  """tests the latency between 2 GPU in 2 VMs using TensorFlow gPRC server."""
  server = f'grpc://{server}'

  with tf.compat.v1.Session(server) as sess:
    devices = sess.list_devices()
  gpus = [device for device in devices if device.name.endswith('device:GPU:0')]
  sender, recver = gpus[0], gpus[1]

  g = tf.Graph()

  with g.as_default():
    def Connect(tensor: tf.Tensor,
                iteration: tf.Tensor) -> Tuple[tf.Tensor, tf.Tensor]:

      with tf.device(recver.name):
        recv_tensor = tf.multiply(tensor, 2.0)

      with tf.device(sender.name):
        send_tensor = tf.multiply(recv_tensor, 2.0)

      return send_tensor, tf.add(iteration, 1)

    with tf.device(sender.name):
      send_tensor = tf.constant(1.0)

    recv_pong, _ = tf.while_loop(
        lambda recv_tensor, iteration: tf.less(iteration, 1000),
        Connect,
        [send_tensor, 0])

  with tf.compat.v1.Session(server, graph=g) as sess:
    run_options = tf.compat.v1.RunOptions(
        trace_level=tf.compat.v1.RunOptions.FULL_TRACE)
    run_metadata = tf.compat.v1.RunMetadata()
    sess.run([recv_pong], options=run_options, run_metadata=run_metadata)
    logging.info(run_metadata)


def main(argv: Sequence[str]) -> None:
  Run(argv[1])


if __name__ == '__main__':
  app.run(main)
