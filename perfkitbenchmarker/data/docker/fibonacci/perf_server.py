"""Toy flask app to inefficiently calculate Fibonacci numbers."""

import socket
import time
from flask import Flask

app = Flask(__name__)
hostname = socket.gethostname()


def calculate_fibonacci(n):
  """Returns the nth Fibonacci number (inefficient for the sake of CPU load).

  Args:
    n: nth Fibonacci number to be calculated.
  """
  if n <= 1:
    return n
  else:
    return calculate_fibonacci(n - 1) + calculate_fibonacci(n - 2)


@app.route('/calculate')
def do_calculation():
  start_time = time.time()
  result = calculate_fibonacci(30)  # Adjust the Fibonacci number for load
  end_time = time.time()

  return [{
      'result': result,
      'calculation_time': end_time - start_time,
      'timestamp': start_time,
      'pod_id': hostname,
  }]


if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0', port=5000)
