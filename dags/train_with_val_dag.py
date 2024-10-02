from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

# Определите параметры DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

dag = DAG(
    'ssh_dag',
    default_args=default_args,
    description='SSH DAG',
    schedule_interval='0 */10 * * *',
    catchup=False
)

ssh_task1 = SSHOperator(
    task_id='data_proc',
    ssh_conn_id='3523',
    command='python3 ./reedit_data.py',
    conn_timeout=None,
    cmd_timeout=None,
    dag=dag,
)

ssh_task2 = SSHOperator(
    task_id='train_model',
    ssh_conn_id='3523',
    command='python3 ./train_with_val.py',
    conn_timeout=None,
    cmd_timeout=None,
    dag=dag,
)


ssh_task1 >> ssh_task2
