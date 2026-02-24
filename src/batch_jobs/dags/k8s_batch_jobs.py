from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# from batch_jobs.pipelines.bronze_silver.minio_to_minio import run_dedup_timestamp
# from batch_jobs.pipelines.silver_gold.clickhouse_to_neo4j import write_clickhouse_to_neo4j
# from batch_jobs.pipelines.silver_gold.clickhouse_to_pinecone import write_clickhouse_to_pinecone
# from batch_jobs.pipelines.silver_silver.minio_to_clickhouse import write_minio_to_clickhouse

from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="full_batch_jobs",
    schedule="*/30 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["batch_jobs"],
) as dag:

    job_dedup_timestamp = KubernetesPodOperator(
        task_id="job_dedup_timestamp",
        name="job_dedup_timestamp",
        namespace="data-platform",
        image="khoa2k4/entertainment_data_platform:batch_jobs_v1",
        image_pull_policy="Always",
        cmds=["python", "-m", "batch_jobs.pipelines.bronze_silver.minio_to_minio"],
    )

    job_extract_to_clickhouse = KubernetesPodOperator(
        task_id="job_extract_to_clickhouse",
        name="job_extract_to_clickhouse",
        namespace="data-platform",
        image="khoa2k4/entertainment_data_platform:batch_jobs_v1",
        image_pull_policy="Always",
        cmds=["python", "-m", "batch_jobs.pipelines.silver_silver.minio_to_clickhouse"],
    )

    job_write_to_neo4j = KubernetesPodOperator(
        task_id="job_write_to_neo4j",
        name="job_write_to_neo4j",
        namespace="data-platform",
        image="khoa2k4/entertainment_data_platform:batch_jobs_v1",
        image_pull_policy="Always",
        cmds=["python", "-m", "batch_jobs.pipelines.silver_gold.clickhouse_to_neo4j"],
    )

    job_write_to_pinecone = KubernetesPodOperator(
        task_id="job_write_to_pinecone",
        name="job_write_to_pinecone",
        namespace="data-platform",
        image="khoa2k4/entertainment_data_platform:batch_jobs_v1",
        image_pull_policy="Always",
        cmds=["python", "-m", "batch_jobs.pipelines.silver_gold.clickhouse_to_pinecone"],
    )

    job_dedup_timestamp >> job_extract_to_clickhouse >> job_write_to_neo4j >> job_write_to_pinecone
