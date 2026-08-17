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

# Load the manifest.json into memory
if not os.path.exists(manifest_path):
    raise FileNotFoundError(
        f"dbt manifest not found at {manifest_path}. "
        f"Run 'dbt compile' in {dbt_path} first."
    )
with open(manifest_path) as f:
    manifest = json.load(f) # Convert JSON into python dictionary
    nodes = manifest["nodes"] # Get all model/test/seed/snapshot nodes

# Define an Airflow DAG
with DAG(
    dag_id="dbt_orchestrator", # This ID shows up in the Airflow UI
    start_date=pendulum.datetime(2026, 8, 17, tz="UTC"), # DAG becomes active starting today
    catchup=False, # Don't backfill past runs
) as dag:
    
    # Dictionary to hold dynamically created tasks
    dbt_tasks = dict()

    # Loop through all nodes in the manifest and create a BashOperator per node
    for node_id, node_info in nodes.items():
        dbt_tasks[node_id] = BashOperator(
            task_id = ".".join( # Create a readable task_id; e.g., model.my_project.my_model
                [
                    node_info["resource_type"], # e.g., model, test
                    node_info["package_name"], # e.g., my_project
                    node_info["name"], #e.g., raw_data_source
                ]
            ),
            # Bash command that runs dbt model using full path to avoid pyenv eval issues
            bash_command=(
                f"cd {dbt_path} && " # Navigate to your dbt project
                f"/home/eddy/.pyenv/versions/demo_dbt/bin/dbt run" # Run the models 
            ),
        )

    # Define task dependencies based on dbt model dependencies
    for node_id, node_info in nodes.items():
        upstream_nodes = node_info["depends_on"]["nodes"] # Get a list of upstream node IDs
        if upstream_nodes: # If there are dependencies...
            for upstream_node in upstream_nodes:
                if upstream_node in dbt_tasks:  # Ensure the upstream node exists in our tasks
                    # Use Airflow's bitshift operator to define dependency chain
                    dbt_tasks[upstream_node] >> dbt_tasks[node_id]

# for testing via CLI (optional)
if __name__ == "__main__":
    dag.cli()



