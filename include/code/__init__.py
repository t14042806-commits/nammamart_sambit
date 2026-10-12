"""
include/code
-----------------
Helper code for the NammaMart ETL and ELT assignment (Sambit).

The two DAG files in dags/ only *wire tasks together*. All real logic lives
here as plain Python functions, so you can:
  - read and test it without Airflow (see run_local.py)
  - reuse the same rules in both pipelines

Start reading with README.md in this folder.
"""
