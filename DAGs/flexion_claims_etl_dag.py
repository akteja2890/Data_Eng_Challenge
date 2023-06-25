from airflow import models
from airflow.operators.python import BranchPythonOperator, PythonOperator
import datetime
from airflow.operators.bash import BashOperator
from google.cloud import storage
import json
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
    
    )
import ast
from google.cloud import bigquery


DAG_ID="flexion_claims_etl_dag"
CONFIG_GCS_BUCKET="poc-flexion"
CLAIMS_CONFIG_GCS_PATH="config/claims/claims_config.json"
PATIENTS_CONFIG_GCS_PATH="config/patients/patients_config.json"
PROVIDERS_CONFIG_GCS_PATH="config/providers/providers_config.json"

def _read_query(ti,gcs_bucket,insert_query_path,key):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(insert_query_path)
    query = blob.download_as_string(client=None).decode("utf-8")
    ti.xcom_push(key=key, value=query)

def _load_config(ti, gcs_bucket,gcs_path):
    """
    load Config file from GCS bucket
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    data = json.loads(blob.download_as_string(client=None))
    ti.xcom_push(key="claims_gcs_bucket", value=data["claims_gcs_bucket"])
    ti.xcom_push(key="claims_data_gcs_path", value=data["claims_data_gcs_path"])
    ti.xcom_push(key="rerun_flag", value=data["rerun_flag"])
    ti.xcom_push(key="rerun_dt", value=data["rerun_dt"])
    ti.xcom_push(key="claims_bq_dataset_stg", value=data["claims_bq_dataset_stg"])
    ti.xcom_push(key="claims_bq_table_stg", value=data["claims_bq_table_stg"])
    ti.xcom_push(key="claims_bq_dataset", value=data["claims_bq_dataset"])
    ti.xcom_push(key="claims_bq_table", value=data["claims_bq_table"])
    ti.xcom_push(key="insert_claim_query_path", value=data["insert_claim_query_path"])
    _read_query(ti,gcs_bucket,data["insert_claim_query_path"],"insert_claim_query")
    _read_query(ti,gcs_bucket,data["merge_claim_query_path"],"merge_claim_query")
    return data

def _get_run_dt(ti):
    """
    get List of dates to load Clevertap data
    """
    # config=json.loads(config_str)
    rerun_flag =  ti.xcom_pull(task_ids='read_config_file',key='rerun_flag') #config["rerun_flag"]
    rerun_dt = ti.xcom_pull(task_ids='read_config_file',key='rerun_dt') #config["rerun_dt"]
    if rerun_flag.upper() == "Y" and len(rerun_dt)>0:
        rerun_dt = rerun_dt
    if rerun_flag.upper() == "N":
        rerun_dt = str(datetime.date.today())
    Variable.set("rerun_dt",rerun_dt)



def _table_data_check(bq_dataset,bq_table):
    client = bigquery.Client()
    query_job = client.query(f"""
                select count(*) as knt from {bq_dataset}.{bq_table}
                             """)
    results = query_job.result()  # Waits for job to complete.
    for row in results:
        if row.knt > 0:
            return "merge_task"
        else:
            return "insert_task"

with models.DAG(
    DAG_ID,
    schedule=None,
    orientation='LR',
    default_args={"start_date": datetime.datetime(2022, 1, 1)},
) as dag:
    start_task = BashOperator(
        task_id="start_job",
        bash_command='echo "Hi Starting Batch Job now"'
   )
    
    read_config = PythonOperator(
        task_id = "read_config_file",
        python_callable=_load_config,
        op_kwargs={"gcs_bucket":CONFIG_GCS_BUCKET, "gcs_path":CLAIMS_CONFIG_GCS_PATH},
    )

    get_run_dt = PythonOperator(
        task_id = "get_run_dt",
        python_callable=_get_run_dt,
    )
    

    rerun_dt = Variable.get("rerun_dt","")

    drop_stg_table = BigQueryExecuteQueryOperator(
        task_id="drop_stg_table",
        sql="drop table if exists "+str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_dataset_stg' ) }}")
            +"."+str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_table_stg' ) }}"),
        use_legacy_sql=False,
   )

    load_stg_table = GCSToBigQueryOperator(
        task_id = 'load_stg_table',
        bucket = str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_gcs_bucket' ) }}"),
        source_objects = str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_data_gcs_path' ) }}")+"/"+rerun_dt+"/*.csv",
        destination_project_dataset_table = str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_dataset_stg' ) }}")+"."
                                                +str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_table_stg' ) }}"),
        source_format = "CSV",
        skip_leading_rows = 1,
        field_delimiter = ",",
        write_disposition="WRITE_TRUNCATE"

    )


    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=_table_data_check,
        op_kwargs={"bq_dataset": str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_dataset' ) }}"),
                "bq_table":str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'claims_bq_table' ) }}")},
        dag=dag)


    merge_task = BigQueryExecuteQueryOperator(
        task_id="merge_task",
        sql=str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'merge_claim_query' ) }}"),
        use_legacy_sql=False,
   )
    
    insert_task = BigQueryExecuteQueryOperator(
        task_id="insert_task",
        sql=str("{{ ti.xcom_pull(task_ids='read_config_file', key = 'insert_claim_query' ) }}"),
        use_legacy_sql=False,
   )
    
    end_task = BashOperator(
        task_id="end_job",
        bash_command='echo "Batch Job ended"',
        trigger_rule = "all_done"
   )

start_task >> read_config >> get_run_dt >> drop_stg_table >> load_stg_table >> branch_op >> [merge_task, insert_task]
merge_task >> end_task
insert_task >> end_task