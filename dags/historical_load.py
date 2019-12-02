import airflow
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from load import status_type

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),  # TODO change this
    'provide_context': True
}

conn = MySqlHook(mysql_conn_id='mysql_tpcdi').get_conn()

dag = airflow.DAG(
    'historical_load',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)


status_type = PythonOperator(
    task_id='StatusType',
    provide_context=False,
    python_callable=status_type.load,
    op_kwargs={'conn': conn},
    dag=dag)
