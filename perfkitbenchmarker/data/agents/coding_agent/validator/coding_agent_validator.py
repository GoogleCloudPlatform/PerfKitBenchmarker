"""Validator for the coding agent task."""

import argparse
import json
import os
import subprocess
import sys
from typing import Any


class ValidatorBrokenError(Exception):
  """The validator workflow is broken."""


def run_shell_command(
    cmd: str, cwd: str = "."
) -> subprocess.CompletedProcess[str]:
  """Runs a shell command and returns the result."""
  return subprocess.run(
      cmd, shell=True, capture_output=True, text=True, cwd=cwd, check=False
  )


def _load_datum(datum_path: str) -> dict[str, Any]:
  """Loads and returns the datum JSON."""
  if not os.path.exists(datum_path):
    raise ValidatorBrokenError(f"datum file not found at {datum_path}")

  with open(datum_path, "r") as f:
    return json.load(f)


def _clone_repo(repo: str, repo_dir: str) -> None:
  """Clones the repository."""
  if os.path.exists(repo_dir):
    subprocess.run(["rm", "-rf", repo_dir], check=True)

  print(f"Cloning {repo}...")
  key_path = os.path.expanduser("~/coding_agent_repo_access_missing_colon")
  if os.path.exists(key_path):
    print(f"Using deploy key at {key_path}")
    res = run_shell_command(
        f'GIT_SSH_COMMAND="ssh -i {key_path} -o StrictHostKeyChecking=no" git'
        f" clone git@github.com:{repo}.git {repo_dir}"
    )
  else:
    print("Deploy key not found, falling back to HTTPS clone")
    res = run_shell_command(
        f"git clone https://github.com/{repo}.git {repo_dir}"
    )

  if res.returncode != 0:
    raise ValidatorBrokenError(f"clone failed: {res.stderr}")


def _checkout_commit(repo_dir: str, commit: str) -> None:
  """Checks out the specified commit."""
  print(f"Checking out {commit}...")
  res = run_shell_command(f"git checkout {commit}", cwd=repo_dir)
  if res.returncode != 0:
    raise ValidatorBrokenError(f"checkout failed: {res.stderr}")


def _apply_agent_patch(repo_dir: str, patch_path: str) -> bool:
  """Applies the agent's patch."""
  print(f"Applying agent's patch from {patch_path}...")
  abs_patch_path = os.path.abspath(patch_path)
  res = run_shell_command(f"git apply {abs_patch_path}", cwd=repo_dir)
  if res.returncode != 0:
    print(f"FAILED: failed to apply agent's patch: {res.stderr}")
    return False
  return True


def _apply_test_patch(repo_dir: str, test_patch: str) -> None:
  """Applies the test patch."""
  if not test_patch:
    raise ValidatorBrokenError("test_patch is empty")

  print("Applying test patch...")
  with open(os.path.join(repo_dir, "test.patch"), "w") as f:
    f.write(test_patch)
  res = run_shell_command("git apply test.patch", cwd=repo_dir)
  if res.returncode != 0:
    raise ValidatorBrokenError(f"failed to apply test patch: {res.stderr}")


def _setup_environment(repo_dir: str) -> str:
  """Sets up venv and returns pytest path."""
  print("Setting up venv...")
  res = subprocess.run(
      [sys.executable, "-m", "venv", "venv"],
      cwd=repo_dir,
      capture_output=True,
      text=True,
      check=False,
  )
  if res.returncode != 0:
    raise ValidatorBrokenError(f"FAILED to create venv: {res.stderr}")

  pip_path = os.path.join(repo_dir, "venv", "bin", "pip")

  print("Installing dependencies...")
  # Try to install pytest in venv
  res = subprocess.run(
      [pip_path, "install", "--index-url", "https://pypi.org/simple", "pytest"],
      cwd=repo_dir,
      capture_output=True,
      text=True,
      check=False,
  )
  if res.returncode != 0:
    raise ValidatorBrokenError(
        f"FAILED to install pytest in venv: {res.stderr}"
    )

  if os.path.exists(os.path.join(repo_dir, "requirements.txt")):
    print("Installing from requirements.txt...")
    res = subprocess.run(
        [
            pip_path,
            "install",
            "--index-url",
            "https://pypi.org/simple",
            "-r",
            "requirements.txt",
        ],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
    )
  elif os.path.exists(os.path.join(repo_dir, "setup.py")) or os.path.exists(
      os.path.join(repo_dir, "pyproject.toml")
  ):
    print("Installing editable...")
    res = subprocess.run(
        [
            pip_path,
            "install",
            "--index-url",
            "https://pypi.org/simple",
            "-e",
            ".",
        ],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
    )
  else:
    print(
        "No standard dependency file found. Skipping dependency installation."
    )
    res = None

  if res and res.returncode != 0:
    raise ValidatorBrokenError(f"Dependency installation failed: {res.stderr}")

  # Determine pytest path with fallback
  venv_pytest = os.path.join(repo_dir, "venv", "bin", "pytest")
  if os.path.exists(venv_pytest):
    pytest_path = venv_pytest
    print(f"Using pytest from workspace venv: {pytest_path}")
  else:
    pytest_path = os.path.join(os.path.dirname(sys.executable), "pytest")
    print(f"Using system/runner pytest: {pytest_path}")
  return pytest_path


def _run_tests(repo_dir: str, pytest_path: str, tests: list[str]) -> bool:
  """Runs the specified tests."""
  all_passed = True
  for test in tests:
    print(f"Running test: {test}")
    res = subprocess.run(
        [pytest_path, test],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
      print(f"FAILED: test {test} failed\n{res.stdout}\n{res.stderr}")
      all_passed = False
    else:
      print(f"PASSED: test {test} passed")

  if all_passed:
    print("PASSED: All SWE-bench tests passed with the applied patch.")
    return True
  else:
    print("FAILED: Some tests failed.")
    return False


def evaluate(patch_path: str, datum_path: str) -> bool:
  """Evaluates the agent's patch against SWE-bench tests.

  Args:
    patch_path: Path to the agent's patch file.
    datum_path: Path to the SWE-bench datum JSON file.

  Returns:
    True if the validation workflow completed smoothly (even if the agent's
      patch failed).
    False if the workflow was broken (infrastructure or validator failure).
  """
  datum = _load_datum(datum_path)

  repo = datum.get("repo")
  base_commit = datum.get("base_commit")
  test_patch = datum.get("test_patch")
  fail_to_pass = datum.get("FAIL_TO_PASS", [])
  pass_to_pass = datum.get("PASS_TO_PASS", [])

  repo_dir = os.path.abspath("workspace_repo")

  _clone_repo(repo, repo_dir)
  _checkout_commit(repo_dir, base_commit)

  if not _apply_agent_patch(repo_dir, patch_path):
    print("FAILED: failed to apply agent's patch, treat as agent failure")
    return True

  _apply_test_patch(repo_dir, test_patch)
  pytest_path = _setup_environment(repo_dir)

  _run_tests(repo_dir, pytest_path, fail_to_pass + pass_to_pass)
  return True


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--patch_path", required=True, help="Path to the agent's patch file."
  )
  parser.add_argument(
      "--datum_path", required=True, help="Path to the SWE-bench datum JSON."
  )
  args = parser.parse_args()

  try:
    evaluate(args.patch_path, args.datum_path)
    sys.exit(0)
  except ValidatorBrokenError as e:
    print(f"VALIDATOR BROKEN: {e}")
    sys.exit(1)
