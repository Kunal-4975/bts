# Apache Airflow Interview Notes

## Overview
Apache Airflow is an open-source workflow orchestration platform used to schedule, monitor, manage, and automate data pipelines and other repeatable workflows. It is especially useful when tasks must run in a specific order, depend on each other, or need retries, logging, and visibility.

## What Is Apache Airflow?
Apache Airflow helps teams define workflows as code and run them reliably. A workflow in Airflow is usually represented as a DAG, which means tasks are connected in a directed and acyclic structure. In simple terms, Airflow decides what should run, when it should run, and what should happen if something fails.

### Why It Is Used
Airflow is useful for:
- Scheduling recurring jobs.
- Managing task dependencies.
- Handling retries and failures.
- Providing monitoring through a web UI.
- Supporting alerting and logging.
- Scaling orchestration across multiple systems.

### Example Use Cases
- Orchestrating ETL pipelines.
- Loading files from cloud storage into Snowflake.
- Running SQL transformations after data lands.
- Triggering downstream jobs after a source system finishes.
- Sending alerts when a pipeline fails.

## Core Architecture
Airflow typically includes a scheduler, web server, metadata database, executor, and workers. These components work together to decide, store, display, and execute workflow runs.

### Scheduler
The scheduler monitors DAG definitions and creates task instances according to the schedule. A good interview answer is: the scheduler decides what should run and when.

### Web Server
The web server provides the Airflow UI. It is used to view DAGs, inspect task states, check logs, manually trigger DAGs, and review run history.

### Metadata Database
The metadata database stores DAG metadata, task states, execution history, connection details, user configs, and XCom values. It is the source of truth for Airflow’s runtime state.

### Executor
The executor determines how tasks are sent for execution. Different executors suit different deployment styles and scale requirements.

### Worker
Workers execute the actual task work. Depending on the executor, the worker may be a local process, a Celery worker, or a Kubernetes pod.

## Executors
The executor choice affects scalability, fault tolerance, and deployment complexity.

### SequentialExecutor
Runs one task at a time. It is mainly useful for learning or local development.

### LocalExecutor
Runs tasks in parallel on the same machine. It is better than SequentialExecutor for small to medium workloads.

### CeleryExecutor
Distributes tasks across multiple workers. It is commonly used in production when horizontal scaling is needed.

### KubernetesExecutor
Creates a separate pod for each task. It is a strong choice for cloud-native workloads and teams that want isolated execution.

## DAG Basics
DAG stands for Directed Acyclic Graph. It represents workflow structure, where the order matters, loops are not allowed, and each task has dependencies.

### Simple Example
```python
task1 >> task2 >> task3
```
This means task1 runs first, then task2, then task3.

### Real Example
If a file must be validated before loading into Snowflake, validation becomes an upstream task and load becomes a downstream task.

## Basic DAG Structure
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='sample_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_function
    )
```

### Important Note
In modern Airflow, `schedule` is preferred over the older `schedule_interval` in new codebases. `catchup=False` is commonly used in production unless historical backfill is required.

## Important DAG Parameters

### dag_id
A unique name for the DAG. It must not conflict with another DAG ID.

### start_date
The date from which Airflow starts considering the DAG for scheduling. It does not mean the DAG runs immediately at that time.

### schedule
Defines the run frequency. Examples include `@daily`, `@hourly`, `@weekly`, or a cron expression.

### Cron Example
```python
schedule='0 2 * * *'
```
This means the DAG runs daily at 2:00 AM.

### catchup
If `True`, Airflow tries to run all missed intervals. If `False`, Airflow runs only the latest scheduled interval and skips older missed ones.

### max_active_runs
Limits how many active DAG runs can exist at once. This is useful when overlapping runs could cause duplicate processing.

### tags
Used to organize DAGs in the UI.

## Operators
Operators define the type of task to run. Each operator represents one unit of work.

### PythonOperator
Runs a Python function.

```python
from airflow.operators.python import PythonOperator

PythonOperator(
    task_id='validate_data',
    python_callable=validate_data
)
```

Use it for validation, transformation, or custom logic.

### BashOperator
Runs shell commands.

```python
from airflow.operators.bash import BashOperator

BashOperator(
    task_id='print_message',
    bash_command='echo "hello"'
)
```

Use it for scripts, file operations, or command-line utilities.

### SnowflakeOperator
Runs Snowflake SQL statements. It is often used for `COPY INTO`, `MERGE`, DDL, and stored procedures.

Example:
```sql
COPY INTO target_table
FROM @stage/file.csv
FILE_FORMAT = (TYPE = CSV);
```

### EmailOperator
Sends email notifications for alerts or pipeline status.

### BranchPythonOperator
Implements conditional branching.

Example:
- If file exists, continue load.
- If file is missing, send alert.

### EmptyOperator
A placeholder operator used for grouping, dependency joins, or flow control. It replaces the older `DummyOperator`.

## Task Dependencies
Dependencies define execution order.

### Bitshift Syntax
```python
task1 >> task2
```
This means task1 runs before task2.

```python
task2 << task1
```
This means the same thing in reverse style.

### Parallel Branching
```python
task1 >> [task2, task3]
```
This means task2 and task3 can run in parallel after task1.

### Join Example
```python
from airflow.operators.empty import EmptyOperator

start >> [branch_a, branch_b] >> join
```
This pattern is common when parallel branches later converge.

## Task Lifecycle
Tasks move through states during execution.

Common states:
- none
- scheduled
- queued
- running
- success
- failed
- skipped
- retry

If a task fails, it enters failed state and may retry if retries are configured. If a branch condition excludes a path, that task often becomes skipped.

## Retries and Alerts
Retries improve resilience against temporary issues such as network problems or database timeouts.

```python
retries=3
retry_delay=timedelta(minutes=5)
```

This means the task can retry 3 times with a 5-minute delay between attempts.

### Common Retry Use Cases
- Temporary database connection failure.
- Cloud storage timeout.
- Brief API outage.
- Lock contention in downstream systems.

### Other Reliability Parameters
- `retry_exponential_backoff=True` for increasing retry gaps.
- `email_on_failure=True` for failure notifications.
- `email_on_retry=True` for retry notifications.

## XCom
XCom means cross-communication. It lets tasks share small pieces of information.

### Example
- Task 1 pushes a file path.
- Task 2 pulls that file path and uses it.

### Push and Pull
```python
ti.xcom_push(key='file_path', value='/tmp/data.csv')
value = ti.xcom_pull(task_ids='task1', key='file_path')
```

### Best Practice
Use XCom only for small values such as IDs, file names, flags, or short metadata. Do not use it for large DataFrames or bulk records.

## Sensors
Sensors wait for an external condition to be true before moving forward.

### FileSensor
Waits for a file to arrive.

### ExternalTaskSensor
Waits for a task or DAG in another workflow.

### SqlSensor
Waits for a database condition to be met.

### Example Use Case
A DAG may wait for an inbound file to land in cloud storage before starting validation and load steps.

### Sensor Caveat
Sensors can consume resources if they keep poking too aggressively. In many cases, a deferrable sensor or well-tuned poke interval is better.

## Hooks
Hooks provide a connection interface to external systems.

Examples:
- SnowflakeHook
- S3Hook
- PostgresHook

Hooks are typically used to create reusable connectivity logic and run operations against a system without hardcoding connection details in every task.

### Example
A SnowflakeHook can be used to open a Snowflake connection and execute SQL from a Python task.

## Connections
Connections are stored in the Airflow UI and contain connection-related details.

Typical fields:
- Hostname
- Username
- Password
- Schema
- Port
- Extra configuration

### Example
A Snowflake connection may store account, warehouse, role, database, schema, and authentication details.

## Variables
Variables are global key-value settings used across DAGs.

```python
from airflow.models import Variable
bucket_name = Variable.get('bucket_name')
```

They are useful for environment-specific configuration such as bucket names, schemas, thresholds, or file paths.

### Best Practice
Use Variables for configurable values, but avoid storing secrets unless the platform is configured securely for that purpose.

## Pools
Pools control how many tasks can use a limited resource at the same time.

### Example
If Snowflake has limited warehouse or connection capacity, a pool can prevent too many concurrent load tasks from running together.

### Practical Benefit
Pools help avoid overload, rate limits, and resource contention.

## Trigger Rules
Trigger rules control when a task should run based on upstream task results.

### all_success
The default rule. The task runs only when all upstream tasks succeed.

### one_success
The task runs if at least one upstream task succeeds.

### all_done
The task runs after all upstream tasks finish, regardless of success or failure.

### one_failed
The task runs if at least one upstream task fails.

### all_failed
The task runs only if all upstream tasks fail.

### none_failed
The task runs if no upstream tasks failed.

## SLA
SLA means Service Level Agreement. In Airflow, it defines the expected completion time for a task.

If the task exceeds the SLA, Airflow can raise an SLA miss alert.

### Practical Note
SLAs are useful for reporting and alerting, but they are not the same as hard task timeouts.

## Backfill
Backfill is used to run historical DAG executions.

### Example
If the last 7 days of data were missed, a backfill can reprocess those days.

```bash
airflow dags backfill
```

### Important Interview Point
Backfill should be designed carefully to avoid duplicate loads and inconsistent downstream results. Idempotent pipeline design is important.

## Execution Date
Execution date is a logical date tied to the data interval being processed. It is not the same as actual runtime.

### Example
If a daily DAG runs on Jan 2 at 2 AM, the execution date may represent Jan 1 data depending on the schedule.

### Interview-Safe Explanation
The execution date tells Airflow which logical period the run belongs to, which is why templated dates in SQL and file paths can differ from the current clock time.

## Jinja Templating
Airflow supports Jinja templating in many fields.

### Example
```python
bash_command='echo {{ ds }}'
```

### Common Template Variables
- `{{ ds }}`: execution date in YYYY-MM-DD format.
- `{{ ts }}`: full timestamp.
- `{{ dag_run.run_id }}`: current run identifier.
- `{{ params.some_value }}`: custom parameter.

### Why It Matters
Templating is often used in file names, SQL queries, and dynamic paths.

## Logging
Every task generates logs, and logs are the first place to check when something breaks.

Logs help debug:
- SQL errors
- Timeouts
- Connection failures
- Permission issues
- Dependency problems

### Good Interview Answer
The first troubleshooting step is checking task logs.

## Airflow With Snowflake
Airflow is often used with Snowflake in production ETL and ELT pipelines.

### Typical Flow
1. Detect file arrival with a sensor.
2. Stage or validate the file.
3. Load data using `COPY INTO`.
4. Run transformation SQL.
5. Perform data quality checks.
6. Send notifications.

### Example Snowflake Load
```sql
COPY INTO target_table
FROM @stage/file.csv
FILE_FORMAT = (TYPE = CSV)
```

### Production Patterns
- Use staging tables.
- Use `MERGE` for upserts.
- Deduplicate before final load.
- Keep loads idempotent.
- Separate raw, staging, and curated layers.

## Common Interview Questions

### DAG is stuck. What do you check?
1. Is the scheduler running?
2. Are dependencies correct?
3. Are workers available?
4. Is the queue backed up?
5. Are there errors in logs?
6. Are there connection issues?

### Task keeps failing intermittently. What do you do?
- Read the logs.
- Add retries if the issue is temporary.
- Check external system stability.
- Review resource contention.
- Verify timeouts and pool settings.

### How do you rerun a failed Snowflake load safely?
Use an idempotent design with staging tables, `MERGE`, and deduplication so that reruns do not create duplicate records.

### How do you optimize Airflow performance?
- Choose the right executor.
- Tune parallelism and concurrency.
- Use pools wisely.
- Keep tasks small and focused.
- Reduce scheduler overhead.
- Avoid excessive sensors and overly heavy DAG parsing.

## Common Comparisons

### Airflow vs Cron
Airflow is dependency-aware, visible, and designed for complex orchestration. Cron is a simpler time-based scheduler without native dependency management or UI-driven observability.

### Airflow vs Luigi
Airflow usually offers stronger scheduling, richer UI, and more production-friendly orchestration features. Luigi is lighter and simpler for basic dependency-based workflows.

### PythonOperator vs BashOperator
PythonOperator is best for Python logic and application code. BashOperator is best for shell commands and CLI-driven tasks.

### Hook vs Operator
A hook manages the connection to an external system. An operator defines the task behavior that uses that connection.

## Must-Memorize Answers

### What is a DAG?
A DAG is a collection of tasks with dependencies arranged in a directed acyclic graph.

### What is XCom?
XCom is Airflow’s mechanism for small inter-task communication.

### What is catchup?
Catchup determines whether missed scheduled intervals should be run.

### What is executor?
The executor defines how tasks are executed.

### What is a sensor?
A sensor is a task that waits for a condition or event.

## Airflow Code Examples

### Simple Python Task
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print('hello')

with DAG(
    dag_id='hello_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='greet_task',
        python_callable=greet
    )
```

### Branching Example
```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def choose_path():
    return 'load_task'

branch = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_path
)
load_task = EmptyOperator(task_id='load_task')
alert_task = EmptyOperator(task_id='alert_task')
```

### Retry Example
```python
from datetime import timedelta

PythonOperator(
    task_id='api_call',
    python_callable=call_api,
    retries=3,
    retry_delay=timedelta(minutes=5)
)
```

## Real-World Interview Answer
When asked how Airflow has been used in production, a strong answer is: Airflow was used to orchestrate ETL pipelines where source files landed in cloud storage, validation tasks ran first, Snowflake loads were triggered using `COPY INTO`, transformation SQL executed afterward, data quality checks verified results, and failure alerts were sent automatically. Retries, logging, task dependencies, and idempotent load design made the pipeline reliable.

## Final Revision Tips
For interviews, focus on explaining concepts clearly with examples rather than memorizing only definitions. Be ready to discuss DAGs, operators, sensors, hooks, XCom, retries, backfill, execution date, and Snowflake integration. Production-grade answers usually mention reliability, observability, idempotency, and scalable design.
