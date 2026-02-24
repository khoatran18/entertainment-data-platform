# import textwrap
# from datetime import datetime, timedelta
# from airflow.sdk import DAG, dag, task
#
# from batch_jobs.pipelines.bronze_silver.minio_to_minio import run_dedup_timestamp
# from batch_jobs.pipelines.silver_gold.clickhouse_to_neo4j import write_clickhouse_to_neo4j
# from batch_jobs.pipelines.silver_gold.clickhouse_to_pinecone import write_clickhouse_to_pinecone
# from batch_jobs.pipelines.silver_silver.minio_to_clickhouse import write_minio_to_clickhouse
#
#
# # @dag(
# #     schedule="*/10 * * * *",
# #     start_date=datetime(2023, 1, 1),
# #     catchup=False,
# #     tags=['batch_jobs', 'dedup', 'extract', 'minio', 'clickhouse', 'neo4j', 'pinecone']
# # )
# def full_batch_jobs():
#
#     @task()
#     def job_dedup_timestamp() -> None:
#         run_dedup_timestamp()
#
#     @task()
#     def job_extract_to_clickhouse():
#         write_minio_to_clickhouse()
#
#     @task()
#     def job_write_t0_neo4j():
#         write_clickhouse_to_neo4j()
#
#     @task()
#     def job_write_to_pinecone():
#         write_clickhouse_to_pinecone()
#
#     job_dedup_timestamp() >> job_extract_to_clickhouse() >> job_write_t0_neo4j() >> job_write_to_pinecone()
#
#
# full_batch_jobs()
#
