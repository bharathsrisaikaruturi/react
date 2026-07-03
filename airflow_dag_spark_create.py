# airflow_dag_mysql_to_gcs_dataproc_minimal.py
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# -------------------------
# CONFIGURATION
# -------------------------
PROJECT_ID = "gcp-dev-project-471104"
REGION = "us-central1"
CLUSTER_NAME = "mysql-to-gcs-minimal"
PYSPARK_URI = "gs://dataproc-09/mysql_to_gcs.py"     # script you upload
JARS = ["gs://dataproc-09/mysql-connector-j-9.4.0.jar"]      # connector JAR you upload
OUTPUT_PATH = "gs://bmw-sales-raw-imports-from-mysql/raw/cars_data_csv"
TABLE = "cars"

# Airflow connection id for MySQL
MYSQL_CONN_ID = "mysql_source"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 1),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "mysql_to_gcs_minimal",
    default_args=default_args,
    schedule_interval="@once",   # run only when triggered manually
    catchup=False,
    tags=["mysql", "dataproc", "gcs", "minimal"],
) as dag:

    # -------------------------
    # 1) Create cluster (single-node)
    # -------------------------
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 50,
                },
            },
            "worker_config": {
                "num_instances": 0,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 50,
                },
            },
            "secondary_worker_config": {
                "num_instances": 0,  # 👈 explicitly no secondary workers
            },
            "software_config": {"image_version": "2.1-debian11"},
        },
    )

    # -------------------------
    # 2) Submit PySpark Job
    # -------------------------
    mysql_conn = BaseHook.get_connection(MYSQL_CONN_ID)
    mysql_host = mysql_conn.host
    mysql_port = mysql_conn.port or 3306
    mysql_user = mysql_conn.login
    mysql_password = mysql_conn.password
    mysql_db = mysql_conn.schema or "bmw"

    job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "jar_file_uris": JARS,
            "args": [
                "--mysql-host", mysql_host,
                "--mysql-port", str(mysql_port),
                "--mysql-db", mysql_db,
                "--mysql-user", mysql_user,
                "--mysql-password", mysql_password,
                "--table", TABLE,
                "--output-path", OUTPUT_PATH,
                "--output-format", "csv",
                "--mode", "overwrite",
            ],
        },
    }

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=job,
    )

    # -------------------------
    # 3) Delete cluster (always runs)
    # -------------------------
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
    )

    create_cluster >> submit_pyspark >> delete_cluster