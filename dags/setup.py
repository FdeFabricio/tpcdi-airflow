import airflow
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
import logging

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}


def initial_setup():
    logging.info('Creating connections, pool and sql path')

    session = Session()

    # create mysql connection
    new_conn = models.Connection("mysql_tpcdi", "mysql", None, "root", "password", "tpcdi", 3306)
    session.add(new_conn)
    session.commit()

    # setup sql path
    new_var = models.Variable()
    new_var.key = "sql_path"
    new_var.set_val("/Users/fabricio/Documents/BDMA/INFO-H-419-DW/tpcdi-airflow/sql")
    session.add(new_var)
    session.commit()

    # create tpcdi pool
    new_pool = models.Pool()
    new_pool.pool = "tpcdi"
    new_pool.slots = 128
    session.add(new_pool)
    session.commit()

    session.close()


dag = airflow.DAG(
    'setup',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(
    task_id='run',
    python_callable=initial_setup,
    provide_context=False,
    dag=dag)
