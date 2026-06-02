# Apache Airflow — Interview Notes (Data Engineer, 5 YOE)

> Reference notes covering architecture, DAGs, scheduling, operators, sensors, XCom, executors, failure handling, Snowflake integration, and the topics interviewers commonly probe. Syntax examples target **Airflow 2.x** (the modern standard).

---

## 1. Airflow Architecture

Airflow is a platform to **programmatically author, schedule, and monitor workflows** as code. Workflows are defined as DAGs (Directed Acyclic Graphs) in Python.

### Core Components

| Component | Responsibility |
|---|---|
| **Scheduler** | The heart of Airflow. Parses DAG files, decides *what* to run and *when*, creates DAG runs and task instances, and submits them to the executor. Runs continuously. |
| **Executor** | Defines *how* and *where* tasks run (locally, in a queue, on Celery/Kubernetes). The executor is a config of the scheduler, not a separate process for Local/Sequential. |
| **Workers** | The processes that actually execute task code (relevant for Celery/Kubernetes executors). |
| **Webserver** | Flask-based UI to inspect DAGs, trigger runs, view logs, manage Variables/Connections. |
| **Metadata Database** | Backbone state store (Postgres/MySQL recommended; SQLite only for dev). Stores DAG runs, task instances, Variables, Connections, XComs, etc. |
| **DAG Directory** | Folder (`dags/`) where DAG `.py` files live. Scheduler + workers parse these. |
| **Triggerer** | (Airflow 2.2+) Runs `asyncio` event loop for **deferrable operators**, freeing worker slots while waiting. |

### How it fits together (flow)

1. Scheduler parses the DAG files in the DAG directory on a loop.
2. Based on the `schedule`, it creates a **DAG Run** and the corresponding **Task Instances** (with state `scheduled`).
3. Scheduler hands runnable task instances to the **Executor**.
4. Executor places them on **Workers** (or runs locally).
5. Workers execute and write **state + logs** back to the **metadata DB**.
6. **Webserver** reads the metadata DB to render UI/logs.

### Key architectural points to mention in interviews

- Everything is **stateless except the metadata DB** — that's the single source of truth.
- Scheduler and workers each independently parse DAG files, so DAG code must be **importable cleanly and fast** (top-level code runs on every parse).
- For HA you can run **multiple schedulers** (Airflow 2.0+).

---

## 2. DAGs — Fundamentals

A **DAG** = collection of tasks with dependencies, directed (tasks flow one way) and acyclic (no loops).

### Three ways to declare a DAG

**Context manager (most common):**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="my_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2},
    tags=["example"],
) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
```

**Standard constructor:**
```python
dag = DAG(dag_id="my_dag", ...)
t1 = BashOperator(task_id="t1", dag=dag, ...)
```

**`@dag` decorator (TaskFlow API):**
```python
from airflow.decorators import dag, task

@dag(schedule="@daily", start_date=pendulum.datetime(2024,1,1), catchup=False)
def my_pipeline():
    @task
    def extract(): return {"x": 1}
    @task
    def load(data): print(data)
    load(extract())

my_pipeline()
```

### `default_args`

A dict of arguments applied to **all tasks** in the DAG (each task can override). Common keys:

```python
default_args = {
    "owner": "data_eng",
    "depends_on_past": False,      # don't wait for prior run's same task to succeed
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": my_alert_func,
    "sla": timedelta(hours=2),
}
```

### Defining dependencies

```python
t1 >> t2 >> t3              # t1 then t2 then t3
t3 << t2                    # t2 before t3
t1 >> [t2, t3]              # fan-out
[t2, t3] >> t4              # fan-in
t1.set_downstream(t2)       # method form
```

### DAG-level important params

- `dag_id` — must be unique.
- `start_date` — first logical date considered. **Use a static date**, never `datetime.now()` (changes every parse and breaks scheduling).
- `schedule` — cron, preset (`@daily`), `timedelta`, or a Dataset list (Airflow 2.4+).
- `catchup` — whether to backfill missed runs (see §11).
- `max_active_runs` — concurrent DAG runs allowed.
- `max_active_tasks` (a.k.a. concurrency) — concurrent task instances in this DAG.
- `dagrun_timeout` — kill a DAG run if it runs too long.

---

## 3. Scheduling & Execution Date (the classic gotcha)

### The data-interval model

Airflow schedules based on **data intervals**. A DAG run for a daily DAG covers a period (e.g., Jan 1 00:00 → Jan 2 00:00) and **runs at the END of that interval**.

> **Key interview line:** A DAG scheduled `@daily` with run for `2024-01-01` actually **executes after midnight on 2024-01-02**, because Airflow waits for the interval to *close* before processing it. This is intentional — most ETL processes a *completed* period of data.

### Terminology (Airflow 2.2+)

| Term | Meaning |
|---|---|
| `logical_date` / `execution_date` (deprecated name) | The **start** of the data interval — a logical label, NOT wall-clock run time. |
| `data_interval_start` | Start of the interval the run covers. |
| `data_interval_end` | End of the interval; usually equals the next run's start. |
| Actual run time | Wall-clock moment the scheduler triggers it (after `data_interval_end`). |

### Common template macros

```text
{{ ds }}                 # logical date  YYYY-MM-DD
{{ ds_nodash }}          # YYYYMMDD
{{ data_interval_start }}
{{ data_interval_end }}
{{ prev_ds }} / {{ next_ds }}
{{ ts }}                 # ISO timestamp
{{ dag_run.run_id }}
{{ params.my_param }}
{{ var.value.my_var }}   # Airflow Variable
{{ conn.my_conn.host }}  # Connection field
```

### Schedule examples

```python
schedule="@daily"                      # preset (midnight)
schedule="0 6 * * *"                   # cron: 6 AM daily
schedule=timedelta(hours=4)            # every 4 hours
schedule=None                          # only triggered manually / externally
schedule="@once"                       # run a single time
```

---

## 4. Operators

An **Operator** is a template for a single task — defines *what* gets done. When instantiated in a DAG it becomes a **Task**.

### Categories

- **Action operators** — perform an action: `BashOperator`, `PythonOperator`, `SnowflakeOperator`, `EmailOperator`.
- **Transfer operators** — move data between systems: `S3ToSnowflakeOperator`, `GenericTransfer`.
- **Sensor operators** — wait for a condition (see §5).

### Common operators with syntax

**BashOperator**
```python
from airflow.operators.bash import BashOperator
run = BashOperator(task_id="run_script", bash_command="python /opt/etl.py {{ ds }}")
```

**PythonOperator**
```python
from airflow.operators.python import PythonOperator

def my_func(name, **context):
    print(name, context["ds"])

t = PythonOperator(
    task_id="py",
    python_callable=my_func,
    op_kwargs={"name": "etl"},
)
```

**TaskFlow `@task` (the modern Pythonic way)**
```python
from airflow.decorators import task

@task
def transform(value: int) -> int:
    return value * 2
# XCom passing is automatic via return values + function args
```

**EmptyOperator** (formerly DummyOperator — grouping/branch join points)
```python
from airflow.operators.empty import EmptyOperator
start = EmptyOperator(task_id="start")
```

**BranchPythonOperator** (conditional paths)
```python
from airflow.operators.python import BranchPythonOperator

def choose(**ctx):
    return "task_a" if ctx["ds"].endswith("01") else "task_b"

branch = BranchPythonOperator(task_id="branch", python_callable=choose)
branch >> [task_a, task_b]
```

> Note: tasks **not** returned by the branch get skipped — affects downstream trigger rules (see §9).

### Hooks (related concept)

A **Hook** is the interface to an external system (DB, cloud) used *inside* operators. Operators typically wrap hooks. You use hooks directly inside a PythonOperator when no dedicated operator exists.
```python
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
df = hook.get_pandas_df("SELECT * FROM my_table LIMIT 10")
```

---

## 5. Sensors

A **Sensor** is a special operator that **waits for a condition** to become true (file arrives, partition lands, time passes, external task finishes).

### Sensor modes (important!)

- **`poke`** (default) — keeps a **worker slot occupied** the whole time, checking every `poke_interval`. Simple but resource-hungry.
- **`reschedule`** — releases the worker slot between checks; the task goes back to `up_for_reschedule`. Use for long waits to avoid blocking slots.
- **Deferrable / async** — uses the **triggerer** + asyncio; most efficient for long waits (e.g., `*Async` sensors / `deferrable=True`).

```python
sensor = MySensor(
    task_id="wait",
    mode="reschedule",
    poke_interval=60,      # check every 60s
    timeout=60 * 60 * 2,   # fail after 2h
    soft_fail=False,       # if True -> skip instead of fail on timeout
    exponential_backoff=True,
)
```

### Common sensors

**FileSensor**
```python
from airflow.sensors.filesystem import FileSensor
FileSensor(task_id="wait_file", filepath="/data/input.csv", poke_interval=30)
```

**S3KeySensor**
```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
S3KeySensor(task_id="wait_s3", bucket_name="my-bucket",
            bucket_key="raw/{{ ds }}/data.parquet", aws_conn_id="aws_default")
```

**ExternalTaskSensor** (cross-DAG dependency)
```python
from airflow.sensors.external_task import ExternalTaskSensor
ExternalTaskSensor(
    task_id="wait_other_dag",
    external_dag_id="upstream_dag",
    external_task_id="final_task",
    allowed_states=["success"],
    mode="reschedule",
)
```

**SqlSensor** (wait until a query returns rows/truthy value)
```python
from airflow.providers.common.sql.sensors.sql import SqlSensor
SqlSensor(task_id="row_check", conn_id="snowflake_default",
          sql="SELECT COUNT(*) FROM staging WHERE load_date = '{{ ds }}'")
```

---

## 6. XCom (Cross-Communication)

**XCom** lets tasks pass **small** pieces of data to each other. Stored in the metadata DB.

### Push / Pull

```python
# Explicit push
def push_fn(**ctx):
    ctx["ti"].xcom_push(key="row_count", value=42)

# Explicit pull
def pull_fn(**ctx):
    n = ctx["ti"].xcom_pull(task_ids="push_task", key="row_count")
```

- A PythonOperator's **return value** is auto-pushed under key `return_value`.
- With **TaskFlow**, passing return values between `@task` functions uses XCom automatically:
```python
@task
def a(): return 5
@task
def b(x): print(x)   # x comes from a()'s XCom
b(a())
```

### Critical limitations (interview gold)

- XCom is for **small metadata** (filenames, counts, IDs) — **NOT** for large datasets. Default backend stores in metadata DB; values are typically size-limited (e.g., ~48KB on MySQL). Passing DataFrames bloats the DB.
- For large data, **write to S3/GCS/Snowflake and pass the path/reference via XCom** instead.
- **Custom XCom backends** can redirect storage to S3/GCS for larger payloads.

---

## 7. Variables vs Connections vs XCom

| Feature | Purpose | Scope/Lifetime | Set by | Example |
|---|---|---|---|---|
| **Variable** | Global config / constants reusable across DAGs | Persistent, global | Admin/UI/CLI/env | `Variable.get("env")` → `"prod"` |
| **Connection** | Credentials & endpoints to external systems | Persistent, global | Admin/UI/CLI/env/secrets backend | DB host, user, password, schema |
| **XCom** | Pass runtime data **between tasks** in a run | Tied to a DAG run/task | Tasks at runtime | row count from one task to next |

### Variables
```python
from airflow.models import Variable
env = Variable.get("environment")                 # string
cfg = Variable.get("etl_config", deserialize_json=True)  # JSON
# Template: {{ var.value.environment }} or {{ var.json.etl_config.threshold }}
```
> Tip: each `Variable.get` is a DB hit at parse time. Store grouped config as **one JSON variable** rather than many.

### Connections
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("snowflake_default")
print(conn.host, conn.login, conn.password, conn.schema, conn.extra_dejson)
```
- Defined via UI (Admin → Connections), CLI, `AIRFLOW_CONN_*` env var, or a **secrets backend** (AWS Secrets Manager, Vault, etc.).
- Operators reference them by `*_conn_id`.

**Rule of thumb:** Variable = config, Connection = credentials/endpoints, XCom = task-to-task runtime data.

---

## 8. Retries and Failure Handling

### Retry settings (task or default_args)
```python
PythonOperator(
    task_id="t",
    python_callable=fn,
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=30),
    execution_timeout=timedelta(hours=1),
)
```

### Callbacks
```python
def alert(context):
    ti = context["task_instance"]
    print(f"FAILED: {ti.task_id} on {context['ds']}")

PythonOperator(
    task_id="t",
    python_callable=fn,
    on_failure_callback=alert,
    on_success_callback=...,
    on_retry_callback=...,
    sla=timedelta(hours=2),          # SLA miss callback at DAG level: sla_miss_callback
)
```

### Task states to know
`none → scheduled → queued → running → success / failed / up_for_retry / up_for_reschedule / skipped / upstream_failed / removed`.

### Failure handling best practices

- Make tasks **idempotent** — rerunning the same logical date must produce the same result (e.g., `DELETE WHERE date = {{ ds }}` then `INSERT`). This is the most important principle.
- Use **retries with backoff** for transient errors (network, rate limits).
- Use **`on_failure_callback`** to push alerts to Slack/PagerDuty.
- **`depends_on_past=True`** stops a task if its previous run failed — use for sequential, stateful pipelines.
- **`trigger_rule`** to control cleanup/notification tasks even on upstream failure (see §9).
- **Clear & rerun**: in UI you can clear a task to re-trigger from a point; **mark success/failed** manually.
- **`AirflowSkipException`** / **`AirflowFailException`** (fail without retry) for explicit control inside Python:
```python
from airflow.exceptions import AirflowSkipException, AirflowFailException
```

---

## 9. Trigger Rules

A task's **trigger_rule** decides whether it runs based on the state of its **upstream** tasks. Default is `all_success`.

| Trigger Rule | Runs when... |
|---|---|
| `all_success` (default) | All upstream succeeded |
| `all_failed` | All upstream failed |
| `all_done` | All upstream finished (any state) — great for cleanup |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed — great for alerting |
| `none_failed` | No upstream failed (success or skipped allowed) |
| `none_failed_min_one_success` | No failures and ≥1 success — common join after branching |
| `none_skipped` | No upstream was skipped |
| `all_skipped` | All upstream skipped |
| `always` | Always run, ignore upstream |

```python
cleanup = BashOperator(
    task_id="cleanup",
    bash_command="rm -rf /tmp/staging",
    trigger_rule="all_done",
)
[load_a, load_b] >> cleanup

# Join after a branch:
join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
```

> Classic question: *"After a BranchPythonOperator, the join task gets skipped — why?"* Because skipped branches propagate `skipped` and default `all_success` won't run. Fix with `none_failed_min_one_success`.

---

## 10. Executors

The **Executor** determines how/where tasks run. Set via `executor` in `airflow.cfg`.

| Executor | Parallelism | Use case |
|---|---|---|
| **SequentialExecutor** | One task at a time | Default with SQLite; **dev only**. |
| **LocalExecutor** | Parallel on a single machine (subprocesses) | Small/medium single-node deployments. |
| **CeleryExecutor** | Distributed via a message broker (Redis/RabbitMQ) + worker pool | Horizontal scaling, classic production. |
| **KubernetesExecutor** | Each task runs in its **own K8s pod** | Dynamic, isolated, resource-efficient; pods spin up/down per task. |
| **CeleryKubernetesExecutor** | Hybrid — route tasks to Celery or K8s | Mixed workloads. |
| **LocalKubernetesExecutor** | Hybrid Local + K8s | — |

### Concurrency knobs (interviewers love these)

- `parallelism` — max task instances across the **whole Airflow install**.
- `max_active_tasks_per_dag` (dag_concurrency) — per DAG.
- `max_active_runs_per_dag` — concurrent runs of one DAG.
- **Pools** — limit concurrency for a shared resource:
```python
PythonOperator(task_id="t", python_callable=fn, pool="snowflake_pool", priority_weight=5)
```
A pool with N slots caps how many tasks using it run at once — e.g., protect a DB from too many parallel connections.

---

## 11. Catchup & Backfill

### Catchup

If `catchup=True` (historic default) and `start_date` is in the past, the scheduler **creates a run for every missed interval** from `start_date` to now. If `catchup=False`, it only runs the **latest** interval going forward.

```python
with DAG(..., start_date=pendulum.datetime(2024,1,1), schedule="@daily", catchup=False):
    ...
```

> **Best practice:** set `catchup=False` for most pipelines to avoid a flood of historical runs when you deploy a DAG with an old start_date. Enable it deliberately when you *want* to process history.

Use `max_active_runs` to throttle catchup so you don't overwhelm downstream systems.

### Backfill

**Backfill** = manually run a DAG over a **past date range**, regardless of catchup. Done via CLI:
```bash
airflow dags backfill my_dag \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```
Use for: reprocessing after a bug fix, loading history for a new pipeline. Tasks must be **idempotent** for safe backfilling.

> **Catchup** is automatic (scheduler fills the gap on deploy). **Backfill** is a deliberate manual command over a chosen range.

---

## 12. Integration with Snowflake

Airflow orchestrates **when** and **in what order** Snowflake transformations run; Snowflake does the heavy compute. Airflow is the conductor, Snowflake is the warehouse.

### Setup

1. Install provider: `pip install apache-airflow-providers-snowflake`
2. Create a **Connection** (`Admin → Connections`), type *Snowflake*:
   - `Account`, `Login`, `Password`, `Warehouse`, `Database`, `Schema`, `Role` (in Extra/fields).
3. Reference by `snowflake_conn_id`.

### Operators / hooks

**SQLExecuteQueryOperator** (the modern, recommended operator — `SnowflakeOperator` is deprecated in newer providers):
```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

transform = SQLExecuteQueryOperator(
    task_id="transform",
    conn_id="snowflake_default",
    sql="sql/transform.sql",        # file in your dags/sql folder (Jinja-templated)
)
```

**SnowflakeHook** (for custom Python logic):
```python
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
count = hook.get_first("SELECT COUNT(*) FROM raw.events WHERE dt='{{ ds }}'")[0]
```

**Loading from S3** with `CopyFromExternalStageToSnowflakeOperator` or a `COPY INTO` statement.

---

### Use Case: Daily S3 → Snowflake ELT Pipeline

**Scenario:** An upstream system drops daily event files into S3 (`s3://my-bucket/raw/<date>/events.parquet`). You need them landed in Snowflake, cleaned, and aggregated into a reporting table every morning — with alerting and reliability.

**Why Airflow + Snowflake here:**
- The upstream file lands at an *unpredictable* time → use a **sensor** to wait instead of guessing.
- The job is a **multi-step dependency chain** (wait → copy → validate → transform → aggregate) → Airflow models dependencies natively.
- You need **retries, alerting, backfill for history, and idempotency** → Airflow's core strengths.
- Snowflake does the actual `COPY`/SQL compute (scalable warehouse); Airflow just orchestrates — you don't pull data into Airflow workers.

**DAG:**
```python
import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

default_args = {
    "owner": "data_eng",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="s3_to_snowflake_daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 6 * * *",          # 6 AM daily
    catchup=False,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/sql",
    tags=["snowflake", "elt"],
) as dag:

    # 1. Wait for the file to land in S3
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="my-bucket",
        bucket_key="raw/{{ ds }}/events.parquet",
        aws_conn_id="aws_default",
        mode="reschedule",          # free the worker slot while waiting
        poke_interval=300,
        timeout=60 * 60 * 4,        # give up after 4h
    )

    # 2. Idempotent load: clear today's partition, then COPY INTO
    load_to_staging = SQLExecuteQueryOperator(
        task_id="load_to_staging",
        conn_id="snowflake_default",
        sql="""
            DELETE FROM staging.events WHERE event_date = '{{ ds }}';
            COPY INTO staging.events
            FROM @my_s3_stage/raw/{{ ds }}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )

    # 3. Data quality gate — fail the run if no rows loaded
    validate = SqlSensor(
        task_id="validate_load",
        conn_id="snowflake_default",
        sql="SELECT COUNT(*) FROM staging.events WHERE event_date = '{{ ds }}'",
        timeout=300,
        poke_interval=30,
        mode="reschedule",
    )

    # 4. Transform + 5. Aggregate (idempotent MERGE/INSERT keyed on date)
    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id="snowflake_default",
        sql="transform_events.sql",     # file in template_searchpath
    )
    aggregate = SQLExecuteQueryOperator(
        task_id="aggregate_daily_report",
        conn_id="snowflake_default",
        sql="aggregate_report.sql",
    )

    wait_for_file >> load_to_staging >> validate >> transform >> aggregate
```

**What makes this production-grade:**
- **Idempotency**: `DELETE ... WHERE event_date = {{ ds }}` before `COPY` means reruns/backfills never double-load. Same for the transform/aggregate using `MERGE` or delete-then-insert keyed on `{{ ds }}`.
- **Backfill**: because every step keys on the logical date, `airflow dags backfill s3_to_snowflake_daily --start-date ... --end-date ...` safely reloads history after a bug fix.
- **Sensor in reschedule mode** avoids wasting a worker slot for hours.
- **Validation gate** stops bad data from propagating to reporting.
- **Retries + exponential backoff** handle transient Snowflake/S3 hiccups.
- Compute happens **in Snowflake**, not on Airflow workers — Airflow only orchestrates and passes SQL.

---

## 13. Important Topics You Should Also Know (commonly asked)

### TaskFlow API
The decorator-based style (`@dag`, `@task`) for cleaner Python pipelines with automatic XCom passing. Increasingly the expected default in Airflow 2.x interviews.

### TaskGroups
Visually & logically group tasks in the UI (replaced the old SubDAGs, which are deprecated/removed):
```python
from airflow.utils.task_group import TaskGroup
with TaskGroup(group_id="extract_group") as eg:
    e1 = ...; e2 = ...
```

### Dynamic Task Mapping (Airflow 2.3+)
Generate tasks at runtime based on data (like a map operation):
```python
@task
def process(file): ...
process.expand(file=list_of_files)   # one task instance per file
```

### Dynamic DAGs
Generating DAGs in a loop / from config (YAML, DB). Powerful but watch parse-time performance.

### Deferrable Operators & the Triggerer
`deferrable=True` operators/sensors release the worker and resume via the triggerer's asyncio loop — huge efficiency win for long waits. Know *why* they exist (worker slot starvation with poke-mode sensors).

### Datasets / Data-Aware Scheduling (Airflow 2.4+)
Schedule a DAG to run when a **Dataset** is updated by another DAG, instead of time-based — enables cross-DAG, event-driven pipelines:
```python
from airflow.datasets import Dataset
my_ds = Dataset("s3://bucket/data")
# producer: outlets=[my_ds]   consumer: schedule=[my_ds]
```

### Jinja Templating & Macros
Fields are templated (`bash_command`, `sql`, `op_kwargs`, etc.). `template_fields` defines what's templated per operator. Macros: `{{ ds }}`, `{{ macros.ds_add(ds, 7) }}`, `{{ dag_run.conf }}`.

### `dag_run.conf` — parameterized manual triggers
Pass runtime params when triggering: `{{ dag_run.conf['date'] }}`. Useful for ad-hoc reruns.

### Secrets Backends
Store Connections/Variables in AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager instead of the metadata DB.

### Idempotency & Atomicity (philosophy questions)
- **Idempotent**: rerun → same result.
- **Atomic**: a task either fully succeeds or fully fails (no partial state). Avoid one task doing many unrelated things.
- **Don't store state in the DAG file**; don't rely on `now()`; key everything on the logical date.

### Useful CLI commands
```bash
airflow dags list
airflow dags trigger my_dag --conf '{"date":"2024-01-01"}'
airflow dags backfill my_dag -s 2024-01-01 -e 2024-01-31
airflow tasks test my_dag my_task 2024-01-01     # run one task, no DB state
airflow dags test my_dag 2024-01-01              # run whole DAG locally
airflow tasks list my_dag --tree
airflow connections add / airflow variables set
```
> `airflow tasks test` / `airflow dags test` are great for local debugging without scheduling.

---

## Quick Interview Cheat-Sheet

- **Scheduler** decides *what/when*; **Executor** decides *how/where*; **metadata DB** is the source of truth.
- A `@daily` run for date D runs **after** the interval (D+1), and `execution_date`/`logical_date` is the **interval start**, not run time.
- Use **`catchup=False`** by default; **backfill** is the manual CLI version over a range.
- **XCom** = small data between tasks; never large datasets — pass references instead.
- **Variable** = config, **Connection** = credentials, **XCom** = runtime task data.
- Make tasks **idempotent + atomic**; key on `{{ ds }}`.
- Sensors: prefer **reschedule** or **deferrable** mode for long waits.
- Branch join tasks need **`none_failed_min_one_success`**.
- Production executors: **Celery** (broker + workers) or **Kubernetes** (pod per task).
- Airflow orchestrates; Snowflake computes — push SQL down, don't pull data into workers.
