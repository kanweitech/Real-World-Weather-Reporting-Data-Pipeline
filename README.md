# Weather Data Pipeline

### Objective
An end-to-end data engineering pipeline that fetches live weather data from a public API, loads it into **PostgreSQL**, transforms it with **dbt**, and orchestrates the entire workflow with **Apache Airflow**.

### Overview

This project simulates a real-world ELT pattern: raw weather observations are extracted from an API on a schedule, loaded into a **PostgreSQL** warehouse as-is, then modeled into clean, analytics-ready tables using dbt — all coordinated by **Airflow** DAGs running on a defined interval.

### Tech Stack

| **Layer**       |  	**Tool**         |
| --------------- |:--------------------:|
| Language        |  Python 3.11         |
| Data Source     |  Open Weather API    |
| Database        |  PostgreSQL          |
| Transformation  |  dbt                 |
| Orchestration   |  Apache Airflow      |
| Containerization|  Docker              |

**DBT initialized, Debugged and Running**

![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png)

**Dockerized PostgreSQL Database**

![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png)

**Airflow initialized**

![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png)


- I created a `dbt-orchestrator.py` file in the dags folder to define an Airflow DAG





### ISSUES

![alt text](https://github.com/adam-p/markdown-here/raw/master/src/common/images/icon48.png)

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
	
	
	