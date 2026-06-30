"""Script to generate mock data for DuckDB and SQLite databases."""

import argparse
import os
import sqlite3
import duckdb


def generate_mock_data(data_dir):
  """Generates mock data for DuckDB and SQLite databases."""
  os.makedirs(data_dir, exist_ok=True)

  duckdb_path = os.path.join(data_dir, "logistics_analytical.db")
  sqlite_path = os.path.join(data_dir, "logistics_transactional.db")

  print(f"=== Generating DuckDB Data at {duckdb_path} ===")
  conn = duckdb.connect(duckdb_path)

  # Products
  conn.execute("DROP TABLE IF EXISTS products")
  conn.execute("""
          CREATE TABLE products (
              id INTEGER PRIMARY KEY,
              name VARCHAR,
              category VARCHAR
          )
      """)
  conn.execute("""
          INSERT INTO products VALUES
          (101, 'SuperWidget', 'Electronics'),
          (102, 'MegaGizmo', 'Electronics'),
          (103, 'HyperPad', 'Electronics'),
          (104, 'SmartCup', 'Home'),
          (105, 'EcoBag', 'Lifestyle')
      """)

  # Order Details
  conn.execute("DROP TABLE IF EXISTS order_details")
  conn.execute("""
          CREATE TABLE order_details (
              order_id INTEGER,
              product_id INTEGER,
              quantity INTEGER
          )
      """)
  conn.execute("""
          INSERT INTO order_details VALUES
          (1, 101, 2),
          (1, 102, 1),
          (2, 103, 1),
          (3, 101, 5),
          (4, 104, 3),
          (5, 105, 10),
          (6, 101, 1),
          (7, 102, 2)
      """)

  conn.close()
  print("DuckDB data generated successfully.")

  print(
      "\n=== Generating Simulated PostgreSQL Data (SQLite) at"
      f" {sqlite_path} ==="
  )
  # We use SQLite to simulate Postgres for easy local testing
  conn = sqlite3.connect(sqlite_path)
  cur = conn.cursor()

  # Users
  cur.execute("DROP TABLE IF EXISTS users")
  cur.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            region TEXT
        )
    """)
  cur.execute("""
        INSERT INTO users (id, name, age, region) VALUES
        (1, 'Alice', 25, 'North'),
        (2, 'Bob', 35, 'South'),
        (3, 'Charlie', 22, 'North'),
        (4, 'David', 28, 'North'),
        (5, 'Eva', 45, 'West')
    """)

  # Orders
  cur.execute("DROP TABLE IF EXISTS orders")
  cur.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            order_date TEXT
        )
    """)
  cur.execute("""
        INSERT INTO orders (id, user_id, order_date) VALUES
        (1, 1, '2026-01-01'),
        (2, 2, '2026-01-02'),
        (3, 3, '2026-01-03'),
        (4, 4, '2026-01-04'),
        (5, 5, '2026-01-05'),
        (6, 1, '2026-01-06'),
        (7, 3, '2026-01-07')
    """)

  conn.commit()
  conn.close()
  print("Simulated PostgreSQL data (SQLite) generated successfully.")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Generate mock data.")
  parser.add_argument(
      "--data_dir",
      type=str,
      required=True,
      help="Directory to store the generated mock data.",
  )
  args = parser.parse_args()
  generate_mock_data(args.data_dir)
