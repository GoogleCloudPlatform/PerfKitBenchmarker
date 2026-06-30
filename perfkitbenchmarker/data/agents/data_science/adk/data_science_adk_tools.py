"""Tools for the ADK data science agent."""

import io
import json
import os
import sqlite3
import subprocess
import sys

import duckdb
from google.cloud import storage

# pylint: disable=broad-exception-caught

WORKSPACE_BASE = os.environ.get("WORKSPACE_BASE", os.getcwd())


def _get_db_paths():
  data_dir = os.getcwd()
  duckdb_path = os.path.join(data_dir, "logistics_analytical.db")
  sqlite_path = os.path.join(data_dir, "logistics_transactional.db")
  return duckdb_path, sqlite_path


def query_sqlite(query: str) -> str:
  """Executes a SQL query on the SQLite database (Transactional Data).

  Args:
      query: The SQL query to execute.

  Returns:
      JSON string of the query results or error message.
  """

  _, sqlite_path = _get_db_paths()

  try:
    if not os.path.exists(sqlite_path):
      return f"Error: SQLite database file not found at {sqlite_path}."

    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    cur.execute(query)

    if cur.description:
      columns = [col[0] for col in cur.description]
      results = [dict(zip(columns, row)) for row in cur.fetchall()]
      res_str = json.dumps(results, default=str)
    else:
      conn.commit()
      res_str = "Query executed successfully (no results)."

    cur.close()
    conn.close()
    return res_str
  except Exception as e:
    return f"SQLite Query Error: {e}"


def query_duckdb(query: str) -> str:
  """Executes a SQL query on the DuckDB database (Analytical Data).

  Args:
      query: The SQL query to execute.

  Returns:
      JSON string of the query results or error message.
  """
  if not duckdb:
    return "Error: duckdb library not installed. Cannot query DuckDB."

  try:
    duckdb_path, _ = _get_db_paths()
    if not os.path.exists(duckdb_path):
      return f"Error: DuckDB database file not found at {duckdb_path}."

    conn = duckdb.connect(duckdb_path)
    res = conn.execute(query)

    if res.description:
      columns = [col[0] for col in res.description]
      results = [dict(zip(columns, row)) for row in res.fetchall()]
      res_str = json.dumps(results, default=str)
    else:
      res_str = "Query executed successfully (no results)."

    conn.close()
    return res_str
  except Exception as e:
    return f"DuckDB Query Error: {e}"


def python_repl(code: str) -> str:
  """Executes Python code in a local REPL (Sandboxed for data science task).

  Use this to run Pandas, Matplotlib, or Seaborn code. You should save plots to
  the 'adk_data_science_results' directory and return the path.

  Args:
      code: The Python code to execute.

  Returns:
      Standard output and error from the execution.
  """

  setup_code = """
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Ensure results directory exists
os.makedirs('adk_data_science_results', exist_ok=True)
"""
  full_code = setup_code + "\n" + code

  old_stdout = sys.stdout
  old_stderr = sys.stderr
  redirected_output = io.StringIO()
  redirected_error = io.StringIO()
  sys.stdout = redirected_output
  sys.stderr = redirected_error

  try:
    exec(full_code, {})  # pylint: disable=exec-used
    stdout = redirected_output.getvalue()
    stderr = redirected_error.getvalue()
    return f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
  except Exception as e:
    stdout = redirected_output.getvalue()
    stderr = redirected_error.getvalue()
    return f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}\nExecution Error: {e}"
  finally:
    sys.stdout = old_stdout
    sys.stderr = old_stderr


def run_shell_command(cmd: str, cwd: str = ".") -> str:
  """Run shell commands.

  Strictly enforced to be within the allowed workspace.

  Args:
    cmd: The shell command to execute.
    cwd: The directory in which to execute the command.

  Returns:
    A string containing the standard output, standard error, and exit code of
    the command, or an error message.
  """

  try:
    abs_cwd = os.path.abspath(cwd)
    workspace_base_abs = os.path.abspath(WORKSPACE_BASE)
    if os.path.commonpath([abs_cwd, workspace_base_abs]) != workspace_base_abs:
      return (
          f"Security error: Command execution outside '{WORKSPACE_BASE}' is"
          " forbidden."
      )

    res = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        cwd=abs_cwd,
        check=False,
    )
    return (
        f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}\nEXIT CODE:"
        f" {res.returncode}"
    )
  except Exception as e:
    return f"Error running shell command: {e}"


def download_from_gcs(
    bucket_name: str, source_blob_name: str, destination_file_name: str
) -> str:
  """Downloads a blob from a bucket."""

  try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    return f"Blob {source_blob_name} downloaded to {destination_file_name}."
  except Exception as e:
    return f"Error downloading from GCS: {e}"


def upload_to_gcs(
    bucket_name: str, source_file_name: str, destination_blob_name: str
) -> str:
  """Uploads a file to the bucket."""

  try:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    return f"File {source_file_name} uploaded to {destination_blob_name}."
  except Exception as e:
    return f"Error uploading to GCS: {e}"


ALL_TOOLS = [
    query_sqlite,
    query_duckdb,
    python_repl,
    run_shell_command,
    download_from_gcs,
    upload_to_gcs,
]
