# Weather Data Pipeline

### Objective
An end-to-end data engineering pipeline that fetches live weather data from a public API, loads it into **PostgreSQL**, transforms it with **dbt**, and orchestrates the entire workflow with **Apache Airflow**.

### Overview

This project simulates a real-world ELT pattern: raw weather observations are extracted from an API on a schedule, loaded into a **PostgreSQL** warehouse as-is, then modeled into clean, analytics-ready tables using dbt — all coordinated by **Airflow** DAGs running on a defined interval.

### Tech Stack

| **Layer**       |  	**Tool**         |
| --------------- |:--------------------:|
| Language        |  Python 3.11         |
| Data Source     |  WeatherStack API    |
| Database        |  PostgreSQL          |
| Transformation  |  dbt                 |
| Orchestration   |  Apache Airflow      |
| Containerization|  Docker              |

**DBT initialized, Debugged and Running**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/dbt_project_started.png)

**Dockerized PostgreSQL Database**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/postgres_containers_running.png)

**Airflow initialized**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/airflow_initialized.png)

**Define task dependencies based on dbt model dependencies**
- The `dbt-orchestrator.py` file in the dags folder is a scheduler used to define an Airflow DAG that orchestrates the dbt model so that tables can created and updated at a certain frequency.
- Airflow's bitshift operator was implemented to define dependency chain so that:
```dbt_tasks[upstream_node] >> dbt_tasks[node_id]``` 
upstream tasks comes before the downstream tasks

- The `api-orchestrator.py` file is a scheduler used to define an Airflow DAG that triggers API requests for ingesting live data into the database. 


- The `sources.yml` file references our database and schema
### Extraction

- Extracted Live Real-time weather data from WeatherStack API using **requests** and **python-dotenv** libraries in `helper_functions.py` file

### Database Connection

- Connected to the **PostgreSQL** database using **psycopg2-binary** library in `helper_functions.py` file
### Data Modelling

**Schema and Table already existing in PostgreSQL**
- Performed a **DDL(Data Definition Language)** command to create the database object(schema and table).

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/database_and_schema_created_postgresql.png)

- Performed a **DML(Data Manipulation Language)** command to insert data from **WeatherStack API** into the existing **PosrgreSQL** database.

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/inserted_live_weather_data.png)

**Buiding the dbt models**

1. Incremental loading strategies implemented in the mart
- **Delete and insert.**
    - I'm running the `delete insert()` hook before I build the model in `weather_del_ins.sql` so that I can check if the table exist so that I can truncate the records and overwrite with new records

- **Merge and insert.**

**dbt models run completed successfully**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/dbt_models_built.png)


**Dag Fetching Daily Weather Data Running Successfully**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/data_fetch_daily_dag.png)

**PostgreSQL Persisting Daily Weather Data From Dag Run**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/data_fetch_daily_dag_persisted.png)

### ISSUES

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/airflow_dag_error.png)

1. **Wrong path**. Your DAG file lives at:

`/home/eddy/projects/elt/dbt-postgres-airflow/airflow/dags/dbt-orchestrator.py`

but dbt_path is built as:

`dbt_path = os.path.join(HOME, "dbt-postgres-airflow/dbt/my_project")`

which resolves to `/home/eddy/dbt-postgres-airflow/dbt/my_project` — missing the `projects/elt/` part. Your actual dbt project is almost certainly at:

**fix**

`projects/elt/dbt-postgres-airflow/dbt/my_project`

2. **manifest.json doesn't exist yet regardless**, because you haven't run dbt compile (or dbt run) against that project to generate it.

**fix**

Run this on your machine (WSL2) to confirm the project path and generate the manifest:

```ls /home/eddy/projects/elt/dbt-postgres-airflow/dbt/my_project

 
 cd /home/eddy/projects/elt/dbt-postgres-airflow/dbt/my_project
/home/eddy/.pyenv/versions/demo_dbt/bin/dbt compile
```

3.**concurrent-collision issue**

![alt text](https://github.com/kanweitech/Real-World-Weather-Reporting-Data-Pipeline/blob/main/images/airflow%20failure.png)

```**Database Error** in model my_second_dbt_model
  relation "my_second_dbt_model" already exists```

**Root cause confirmed:**
every task calls plain dbt run, which rebuilds all models every time. my_second_dbt_model is defined as a view, and by the time this particular task's dbt run reached step 2, a table/view named my_second_dbt_model already existed in Postgres.

This is a design bug in the DAG, not an environment/WSL2/OOM issue. It'll keep happening intermittently depending on timing.

**The fix**

1. scope each task's dbt command to only its own node, so tasks stop redundantly rebuilding the whole project and colliding with each other:
```
bash_command=(
    f"cd {dbt_path} && "
    f"/home/eddy/.pyenv/versions/demo_dbt/bin/dbt {node_info['resource_type'] if node_info['resource_type']=='test' else 'run'} "
    f"--select {node_info['name']}"
)```
2. Simpler and clearer, split by resource type:

```
if node_info["resource_type"] == "test":
    dbt_command = f"dbt test --select {node_info['name']}"
else:
    dbt_command = f"dbt run --select {node_info['name']}"

dbt_tasks[node_id] = BashOperator(
    task_id=".".join([node_info["resource_type"], node_info["package_name"], node_info["name"]]),
    bash_command=f"cd {dbt_path} && /home/eddy/.pyenv/versions/demo_dbt/bin/dbt {dbt_command.split(' ', 1)[1]}",
)```
  
	
	
	