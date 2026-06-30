"""Defines the agents for the Data Science ADK."""

import data_science_adk_tools
from google.adk.agents import llm_agent


# 1. Database Agent (NL2SQL)
def create_db_agent(model_name: str) -> llm_agent.Agent:
  """Creates the Database Agent."""
  return llm_agent.Agent(
      name="db_expert",
      description=(
          "Specialist for querying SQLite (Transactional) and DuckDB"
          " (Analytical) databases."
      ),
      instruction="""
        You are a Database Expert. Your task is to query SQLite and DuckDB to answer user questions.
        Use `query_sqlite` for transactional data and `query_duckdb` for analytical data.
        You also have `download_from_gcs` and `upload_to_gcs` to download/upload files from/to GCS.
        Return data in JSON or clear tabular format.
        """,
      model=model_name,
      tools=[
          data_science_adk_tools.query_sqlite,
          data_science_adk_tools.query_duckdb,
          data_science_adk_tools.run_shell_command,
          data_science_adk_tools.download_from_gcs,
          data_science_adk_tools.upload_to_gcs,
      ],
  )


# 2. Data Science Agent (NL2Py)
def create_ds_agent(model_name: str) -> llm_agent.Agent:
  """Creates the Data Science Agent."""
  return llm_agent.Agent(
      name="ds_analyst",
      description=(
          "Specialist for data analysis, Pandas manipulation, and generating"
          " visualizations."
      ),
      instruction="""
        You are a Data Analyst. Your task is to write Python code to analyze data and create visualizations.
        Use `python_repl` to execute code.
        Save any plots or result files to the 'adk_data_science_results' directory.
        You create insights from data provided by the DB expert or supervisor.
        If requested to write an answer to a file, use `python_repl` to do so.
        """,
      model=model_name,
      tools=[data_science_adk_tools.python_repl],
  )


# 3. Supervisor Agent (Orchestrator)
def create_supervisor_agent(model_name: str) -> llm_agent.Agent:
  """Creates the Supervisor Agent."""
  return llm_agent.Agent(
      name="supervisor_orchestrator",
      description="Top-level manager coordinating DB and DS agents.",
      instruction="""
        You are the Supervisor of a Multi-Agent Data Science System.
        Your task is to solve complex user requests by delegating to specialized sub-agents.
        You have access to:
        - `db_expert`: For data retrieval.
        - `ds_analyst`: For analysis and visualization.

        Coordinate them to answer the user's request. Explain your plan and then delegate.
        """,
      model=model_name,
      sub_agents=[create_db_agent(model_name), create_ds_agent(model_name)],
  )
