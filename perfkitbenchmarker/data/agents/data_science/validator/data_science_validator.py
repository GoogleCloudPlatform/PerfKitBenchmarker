"""Evaluates the results of the data science task."""

import argparse
import os
import sys


def evaluate(answer_path):
  """Evaluates the results of the data science task."""
  if not os.path.exists(answer_path):
    print(f"FAILED: answer.txt not found at {answer_path}")
    return False

  # Verify content of answer.txt
  with open(answer_path, "r") as f:
    content = f.read()

  print(f"Info: answer.txt content:\n{content}")

  # We expect 'SuperWidget' and quantity '8' (or at least SuperWidget)
  if "SuperWidget" in content and "8" in content:
    print("PASSED: Data Science evaluation successful. Found expected answer.")
    return True
  elif "SuperWidget" in content:
    # TODO(odiego): Return an intermediate score, signaling partial pass.
    print(
        "FAILED: Data Science evaluation partially successful. Found expected"
        " product but maybe not quantity."
    )
    return False
  else:
    print("FAILED: answer.txt does not contain expected answer 'SuperWidget'.")
    return False


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--answer", required=True, help="Path to answer.txt")
  args = parser.parse_args()

  if evaluate(args.answer):
    sys.exit(0)
  else:
    sys.exit(1)
