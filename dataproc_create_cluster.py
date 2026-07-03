from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator

create_cluster = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster",
    project_id="gcp-dev-project-471104",
    region="us-central1",
    cluster_name="my-dataproc-cluster",
    cluster_config={
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2"
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2"
            },
        }
    },
)


