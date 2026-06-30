"""Tools for the coding agent."""

import glob
import os
import re
import subprocess
import urllib.request

from google.cloud import storage

WORKSPACE_BASE = os.environ.get("WORKSPACE_BASE", os.getcwd())


def _ensure_strict_cwd(cwd: str) -> str:
  """Verifies that the target cwd is within the allowed workspace."""
  abs_cwd = os.path.abspath(cwd)
  if not abs_cwd.startswith(WORKSPACE_BASE):
    raise ValueError(
        f"Security error: Command execution outside '{WORKSPACE_BASE}' is"
        f" forbidden. Attempted: {abs_cwd}"
    )
  return abs_cwd


def read_file(path: str) -> str:
  """Reads a file."""
  with open(path, "r") as f:
    return f.read()


def create_file(path: str, content: str) -> str:
  """Creates a new file."""
  os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
  with open(path, "w") as f:
    f.write(content)
  return "File created successfully."


def edit_code(path: str, search_string: str, replace_string: str) -> str:
  """Edits code by replacing a search string with a replace string."""
  with open(path, "r") as f:
    content = f.read()
  if search_string not in content:
    return "Search string not found in file."
  with open(path, "w") as f:
    f.write(content.replace(search_string, replace_string, 1))
  return "File edited successfully."


def rename_file(old_path: str, new_path: str) -> str:
  """Renames and reorganizes a file."""
  os.makedirs(os.path.dirname(os.path.abspath(new_path)), exist_ok=True)
  os.rename(old_path, new_path)
  return "File renamed successfully."


def find_files(pattern: str, cwd: str = ".") -> str:
  """Finds files by glob pattern."""
  cwd = _ensure_strict_cwd(cwd)
  files = glob.glob(f"{cwd}/**/{pattern}", recursive=True)
  return "\n".join(files) if files else "No files found."


def search_content(regex: str, cwd: str = ".") -> str:
  """Searches content with regex."""
  cwd = _ensure_strict_cwd(cwd)
  res = subprocess.run(
      ["grep", "-rnE", regex, cwd],
      capture_output=True,
      text=True,
      check=False,
  )
  return res.stdout if res.stdout else "No matches found."


def run_shell_command(cmd: str, cwd: str = ".") -> str:
  """Runs shell commands, starts servers, runs tests, uses git.

  Strictly enforced to be within the allowed workspace.

  Args:
    cmd: The command to run.
    cwd: The working directory for the command.

  Returns:
    The stdout, stderr and exit code of the command.
  """
  cwd = _ensure_strict_cwd(cwd)
  res = subprocess.run(
      cmd, shell=True, capture_output=True, text=True, cwd=cwd, check=False
  )
  return (
      f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}\nEXIT CODE:"
      f" {res.returncode}"
  )


def search_web(query: str) -> str:
  """Fetches web snippets using DuckDuckGo."""
  from duckduckgo_search import DDGS  # pylint: disable=g-import-not-at-top

  with DDGS() as ddgs:
    results = ddgs.text(query, max_results=5)
    output = []
    for r in results:
      output.append(
          f"Title: {r['title']}\nURL: {r['href']}\nSnippet: {r['body']}\n---"
      )
    return "\n".join(output) if output else "No results found."


def fetch_webpage(url: str) -> str:
  """Browses a web page by URL to fetch its content."""
  req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
  with urllib.request.urlopen(req) as response:
    html = response.read().decode("utf-8")

    try:
      import bs4  # pylint: disable=g-import-not-at-top

      soup = bs4.BeautifulSoup(html, "html.parser")
      for element in soup(["script", "style", "header", "footer", "nav"]):
        element.decompose()
      text = soup.get_text(separator=" ", strip=True)
      return text[:5000]
    except ImportError:
      text = re.sub(r"<[^>]+>", " ", html)
      return text[:5000]


def upload_to_gcs(
    bucket_name: str, source_file_name: str, destination_blob_name: str
) -> str:
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)
  blob.upload_from_filename(source_file_name)
  return f"File {source_file_name} uploaded to {destination_blob_name}."


ALL_TOOLS = (
    read_file,
    create_file,
    edit_code,
    rename_file,
    find_files,
    search_content,
    run_shell_command,
    search_web,
    fetch_webpage,
    upload_to_gcs,
)
