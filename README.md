# NammaMart ETL & ELT: code guide (Sambit)

This guide covers how the code is organised, the order to read it in, and how to run it.

## 1. Where everything is

`nammamart_sambit/` is a **standalone Astro project**: it has its own Dockerfile and requirements, and you
start it from this folder. The trainer's project in the parent folder (`meridian_pipeline.py`) is untouched.
Airflow orchestrates everything: every step is an `@task` in the two DAG files.

```
nammamart_sambit/
 Dockerfile                    Astro Runtime image (same version as the class project)
 requirements.txt              duckdb (+ optional anthropic / openai). No pandas.
 packages.txt                  OS packages (none needed)
 .astro/config.yaml            Astro project name: nammamart-sambit
 .dockerignore, .gitignore     keep output (PII) out of the image and out of Git
 README.md                     this guide
 SUBMISSION.md                 write-up (issues log, answers, experiments, recommendation)
 dags/                         <- Airflow reads DAGs from here (live-synced)
  nammamart_etl_sambit.py      ETL DAG  - only wires tasks together
  nammamart_elt_sambit.py      ELT DAG  - only wires tasks together
 include/                      <- live-synced to /usr/local/airflow/include
  code/
   config.py                   paths, file names, allowed values, limits  (change rules HERE)
   checks_catalog.py           every quality check: name, dimension, severity
   csv_utils.py                read/write CSV with the built-in csv module (no pandas)
   rules.py                    ETL quality rules in plain Python
   masking.py                  mask_email / mask_phone / mask_name
   warehouse.py                DuckDB connection, run .sql files, dq_results, pipeline_runs
   etl_steps.py                ETL logic, one function per task
   elt_steps.py                ELT logic, one function per task (mostly "run this SQL file")
   llm.py                      swappable LLM (Claude / OpenAI / Azure / Ollama / ...)
   alerts.py                   end-of-run message -> optional LLM wording -> Slack or log
   run_local.py                test helper only: calls the same functions without Airflow
   profile_data.py             Part 0 data profiling report
   make_experiment_data.py     Part F broken-data copies
  sql/
   common/audit_tables.sql     dq_results, pipeline_runs
   etl/create_tables.sql       ETL star schema + rejected rows + reconciliation
   etl/reports.sql             etl_daily_zone_revenue, etl_category_margin
   elt/stg_*.sql               staging layer (typing, cleaning, masking, checks)
   elt/stg_orders_merge.sql    MERGE of the increment file
   elt/mart_*.sql              5 mart tables
   business_questions.sql      the 6 business questions, for both pipelines
  data/                        the 6 source CSVs (never edit by hand)
  data_experiments/            broken copies for Part F (created by make_experiment_data.py, gitignored)
  output/                      created by the pipelines (gitignored):
   warehouse_sambit.duckdb     the DuckDB warehouse
   quarantine/                 <file>_failed_<run_id>.csv
   work/                       intermediate files between ETL tasks
  slack_notifier.py            copy of the trainer's Slack helper (used by alerts.py)
 plugins/                      empty (standard Astro folder)
 tests/dags/test_dag_example.py  Astro's DAG checks: no import errors, tags set, retries >= 2
```

## 2. Suggested reading order

1. `include/code/config.py` and `checks_catalog.py`: the rules in plain English.
2. `include/code/rules.py`: how one row is checked (`validate_orders` is the main one).
3. `include/code/etl_steps.py` together with `dags/nammamart_etl_sambit.py`: each `@task` calls one function.
4. `include/sql/elt/stg_customers.sql`: the SQL version of the same idea. All other `stg_*.sql` files follow the same 4-step pattern.
5. `include/code/elt_steps.py` together with `dags/nammamart_elt_sambit.py`.
6. `include/code/llm.py` and `alerts.py`.

## 3. Key ideas used in the code

| Idea | Where | Why |
|---|---|---|
| Every step is an Airflow `@task` | both DAGs | Airflow handles order, parallel runs, retries, schedule, logs |
| Tasks pass **file paths**, not data | `etl_steps.py` | XCom stays small; intermediate CSVs can be opened in VS Code |
| `.override(task_id=...)` | both DAGs | one function → several parallel tasks (5 extracts, 5 marts) |
| **Idempotent** load: `DELETE` + `INSERT` in one transaction | `etl_steps.load` | re-running never double-counts; a failure rolls back |
| Raw is **append-only** | `elt_steps.load_raw` | audit trail; deletes only the *same* run's rows on retry |
| `chk_<name>` columns | `stg_*.sql` | one TRUE/FALSE per check → `dq_results` counts them |
| Same check names in Python and SQL | `checks_catalog.py` | lets us prove ETL and ELT agree |
| `trigger_rule="all_done"` | `audit`, `notify`, `final_status` | the run audit is written even when something failed |
| `final_status` task | both DAGs | makes the DAG run red when the pipeline failed (otherwise the green audit task would hide it) |
| `run_ts` = run start in IST | `run_info()` in DAGs | every task in a run uses the same "now" for future-date checks |

## 4. Run it in Airflow (the real way)

Open a terminal **inside `nammamart_sambit/`** (the folder with the Dockerfile):

```bash
cd nammamart_sambit
astro dev start                                 # builds the image and starts Airflow
astro dev run dags list-import-errors           # should list nothing
astro dev run dags test nammamart_etl_sambit    # one run from the terminal (optional)
astro dev pytest                                # runs tests/dags (import errors, tags, retries)
```

- Edits to `dags/` and `include/` sync into the container automatically, with no restart needed.
- Run `astro dev restart` only after changing `requirements.txt`, `packages.txt` or the `Dockerfile`.
- The warehouse, quarantine and work files appear on your laptop in `include/output/`.
- If the class project in the parent folder is also running, stop it first (`astro dev stop` there),
  because both use port 8080.

In the UI (`http://localhost:8080`), turn both DAGs on (they then run at 9:00 IST, Mon–Sat), or use
**▶ Trigger DAG w/ config**:

| Goal | Config |
|---|---|
| normal run | `{}` |
| load the next-day file | `{"load_increment": true}` |
| a Part F experiment | `{"data_dir": "/usr/local/airflow/include/data_experiments/exp05_bad_rows"}` |

## 5. Quick checks without Docker (optional)

`include/code/run_local.py` is **only a test helper**. It calls the same functions the Airflow tasks call, in
the same order, so you can check numbers on a laptop without Docker. It doesn't schedule, retry or run in
parallel; Airflow does all of that. From inside `nammamart_sambit/` (the venv is one folder up):

```powershell
..\.venv\Scripts\python -m include.code.profile_data            # Part 0 profile
..\.venv\Scripts\python -m include.code.run_local               # both pipelines + ETL vs ELT comparison
..\.venv\Scripts\python -m include.code.run_local --increment   # + next-day file
..\.venv\Scripts\python -m include.code.make_experiment_data    # Part F broken copies
..\.venv\Scripts\python -m include.code.run_local --data-dir include/data_experiments/exp05_bad_rows
```

## 6. Switching the LLM (no code change)

**Admin → Variables** in the Airflow UI:

| Variable | Example |
|---|---|
| `LLM_PROVIDER` | `none` (default, template message only) · `anthropic` · `openai` · `azure_openai` · `openai_compatible` |
| `LLM_MODEL` | `claude-opus-5-5` (Anthropic default) · your OpenAI model · Azure deployment name · `llama3.1` (Ollama) |
| `LLM_API_KEY` | the provider's key. It's a secret, so it lives only here and never in Git. |
| `LLM_BASE_URL` | Azure endpoint, or e.g. `http://host.docker.internal:11434/v1` for Ollama |
| `SLACK_WEBHOOK_URL` | optional; without it, alerts are printed in the `notify` task log |

To add a provider that isn't listed, write one `_call_<name>()` function in `llm.py` and add it to `PROVIDERS`.
The pipeline never fails because of the LLM: on any error it falls back to the template message. Only counts and
check names are sent to the LLM, never customer rows.

## 7. Notes

- **DuckDB allows one writer at a time.** Close SQLTools/DBeaver connections to
  `include/output/warehouse_sambit.duckdb` while DAGs run. `warehouse.connect()` waits up to about 1 minute
  if the file is busy.
- `include/output/` and `include/data_experiments/` are in `.gitignore` and `.dockerignore`.
  The warehouse and quarantine files contain raw customer PII and must not go to GitHub.
- Paths differ slightly from the assignment's suggestions (`include/data/nammamart/`,
  `include/quarantine/<name>/`, `include/warehouse_<name>.duckdb`) because the whole assignment is its own
  project. Mention this in your submission.
- On native Windows, Airflow itself can't run tasks. Use Astro/Docker (or WSL).
