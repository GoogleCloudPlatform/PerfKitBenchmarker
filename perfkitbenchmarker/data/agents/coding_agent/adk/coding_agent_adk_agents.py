"""Defines the coding agent for SWE-bench."""

import textwrap
from typing import Any
import coding_agent_adk_tools
from google.adk.agents import llm_agent

Agent = llm_agent.Agent


def on_tool_error(
    tool: Any, args: dict[str, Any], tool_context: Any, error: Any
) -> str:
  """Global error handler for all tools to prevent container crashes."""
  del args, tool_context  # Unused
  return f"ERROR: Tool '{tool.name}' failed with: {error}"


def create_coding_agent(model_name: str) -> Agent:
  """Creates the coding agent."""
  return Agent(
      name="coding_agent",
      description=(
          "A coding agent that solves issues from the SWE-bench dataset."
      ),
      instruction=textwrap.dedent("""
          You are a coding agent with actual system execution permissions.
          Crucial: Your work area is the `workspace_repo` directory inside the
          current workspace. The root workspace is the host repo for the agent,
          and its history must NOT be polluted by your work. Always use the
          repository folder `workspace_repo` for all git operations, code
          searches, file edits, and test runs ONCE IT IS CREATED. When using
          tools like `run_shell_command`, specify `cwd="workspace_repo"` or `cd`
          into it (e.g., `cd workspace_repo && ...`).

          Your first priority is to set up the environment as instructed in your
          initial prompt. DO NOT attempt to solve the issue until the
          environment is fully set up. Note: You do NOT need to apply any test
          patches; that is handled by the evaluator.

          Your recursive master loop operates as follows:
          1. Initialize (or append to) `execution_history.md` to document every
             tool call you make and every loop iteration. This acts as your
             trace log.
          2. IF the environment is not yet set up, perform the setup steps
             (clone, checkout, install) exactly as described in your initial
             prompt.
          3. Search for the relevant codebase files using `find_files` or
             `search_content` inside the workspace (use
             `cwd='workspace_repo'`).
          4. Write the required code for the fix (using `create_file` or
             `edit_code`, prepending the `workspace_repo` directory to file
             paths if necessary).
          5. **Crucial:** Add or run automated tests for the fix (using
             `run_shell_command` with `cwd="workspace_repo"`).
          6. Update `execution_history.md` with the tool call and output.
          7. Make a Git commit with a descriptive message (using
             `run_shell_command("git commit -m ...", cwd="workspace_repo")`).
          8. AFTER you have solved the issue and verified it with tests, you
             MUST generate a patch file.
             - Use `git diff base_commit..HEAD > fix.patch` (replace
               base_commit with the actual base commit ID).
             - Print `PATCH_CREATED: fix.patch` in your final response.
          """),
      model=model_name,
      tools=coding_agent_adk_tools.ALL_TOOLS,
      on_tool_error_callback=on_tool_error,
  )
