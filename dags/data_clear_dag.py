import uuid
from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,

)


YC_DP_FOLDER_ID = 'YC_DP_FOLDER_ID'
YC_DP_SUBNET_ID = 'YC_DP_SUBNET_ID'
YC_DP_SA_ID = 'YC_DP_SA_ID'
YC_DP_AZ = 'YC_DP_AZ'
YC_DP_SSH_PUBLIC_KEY = Variable.get("YC_DP_SSH_PUBLIC_KEY")
YC_DP_GROUP_ID = 'YC_DP_GROUP_ID'


# Данные для S3
YC_SOURCE_BUCKET = 'dducket'                     # YC S3 bucket for pyspark source files
YC_DP_LOGS_BUCKET = 'dducket/airflow_logs/'      # YC S3 bucket for Data Proc cluster logs


# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(
    conn_id='yc-s3',
    conn_type='s3',
    host='https://storage.yandexcloud.net/',
    extra={
        "aws_access_key_id": Variable.get("S3_KEY_ID"),
        "aws_secret_access_key": Variable.get("S3_SECRET_KEY"),
        "host": "https://storage.yandexcloud.net/"
    }
)


if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id='yc-SA',
    conn_type='yandexcloud',
    extra={
        "extra__yandexcloud__public_ssh_key": Variable.get("DP_PUBLIC_SSH_KEY"),
        "extra__yandexcloud__service_account_json_path": Variable.get("DP_SA_PATH")
    }
)

if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)
    session.commit()

with DAG(
        dag_id = 'DATA_CLEAR',
        start_date=datetime(year = 2024,month = 1,day = 20),
        schedule_interval = timedelta(days=1),
        catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='create-cluster',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-cluster-{uuid.uuid4()}',
        cluster_description='',
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_DP_LOGS_BUCKET,
        service_account_id=YC_DP_SA_ID,
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        zone=YC_DP_AZ,
        cluster_image_version='2.0.43',
        masternode_resource_preset='s3-c2-m8',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        datanode_count=0,
        services=['YARN', 'SPARK'],  
        computenode_resource_preset='s3-c4-m16',
        computenode_disk_type='network-hdd',
        computenode_disk_size=128,
        computenode_count=3,           
        connection_id=ycSA_connection.conn_id,
        dag=ingest_dag
    )

    install_env = DataprocCreatePysparkJobOperator(
        task_id='install_env',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/install_env.py',
        connection_id = ycSA_connection.conn_id,
        dag=ingest_dag
    )
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='pyspark-task',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/data_cleaning.py',
        connection_id = ycSA_connection.conn_id,
        dag=ingest_dag
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='delete-cluster',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag
    )
    create_spark_cluster >> install_env >> poke_spark_processing >> delete_spark_cluster
