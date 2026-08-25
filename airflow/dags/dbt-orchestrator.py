import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Get the HOME directory of the current user (e.g., /home/eddy)
HOME = os.environ["HOME"]

# Path to your dbt project directory
dbt_path = os.path.join(HOME, "projects/elt/dbt-postgres-airflow/dbt/my_project")

# Path to dbt's manifest.json which contains compiled metadata about the dbt models
manifest_path = os.path.join(dbt_path, "target/manifest.json")

# Path to the dbt binary inside the dedicated pyenv virtualenv
DBT_BIN = "/home/eddy/.pyenv/versions/demo_dbt/bin/dbt"

# Load the manifest.json into memory
if not os.path.exists(manifest_path):
    raise FileNotFoundError(
        f"dbt manifest not found at {manifest_path}. "
        f"Run 'dbt compile' in {dbt_path} first."
    )
with open(manifest_path) as f:
    manifest = json.load(f)  # Convert JSON into python dictionary
    nodes = manifest["nodes"]  # Get all model/test/seed/snapshot nodes

# Define an Airflow DAG
with DAG(
    dag_id="dbt_orchestrator",  # This ID shows up in the Airflow UI
    start_date=pendulum.datetime(2026, 8, 17, tz="UTC"),  # Fixed start date (must not be dynamic)
    schedule='@hourly',
    catchup=False,  # Don't backfill past runs
    max_active_tasks=1,
) as dag:

    # Dictionary to hold dynamically created tasks
    dbt_tasks = dict()

    # Loop through all nodes in the manifest and create a BashOperator per node
    for node_id, node_info in nodes.items():
        resource_type = node_info["resource_type"]  # e.g. model, test
        model_name = node_info["name"]  # e.g. my_first_dbt_model

        # Scope each task to only its own node using --select, so tasks running
        # concurrently don't all rebuild the whole project and collide with
        # each other (this was causing "relation already exists" errors).
        if resource_type == "test":
            dbt_subcommand = f"dbt test --select {model_name}"
        else:
            dbt_subcommand = f"dbt run --select {model_name}"

        dbt_tasks[node_id] = BashOperator(
            task_id=".".join(  # Create a readable task_id; e.g., model.my_project.my_model
                [
                    resource_type,  # e.g., model, test
                    node_info["package_name"],  # e.g., my_project
                    model_name,  # e.g., my_first_dbt_model
                ]
            ),
            # Bash command that runs only this node's dbt model/test using full path to avoid pyenv eval issues
            bash_command=(
                f"cd {dbt_path} && "  # Navigate to your dbt project
                f"{DBT_BIN} {dbt_subcommand.split(' ', 1)[1]}"  # Run only this node
            ),
            retries=2,
            retry_delay=pendulum.duration(seconds=30),
        )

    # Define task dependencies based on dbt model dependencies
    for node_id, node_info in nodes.items():
        upstream_nodes = node_info["depends_on"]["nodes"]  # Get a list of upstream node IDs
        if upstream_nodes:  # If there are dependencies...
            for upstream_node in upstream_nodes:
                if upstream_node in dbt_tasks:  # Skip sources/seeds not modeled as tasks
                    # Use Airflow's bitshift operator to define dependency chain
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]

# for testing via CLI (optional)
if __name__ == "__main__":
    dag.cli()
