import airflow
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator

from load import account, status_type, date, customer, company, dim_broker, holdings, security, prospect, time, watches

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
    template_searchpath="./sql/",
    max_active_runs=1)

dim_date = PythonOperator(
    task_id='DimDate',
    provide_context=False,
    python_callable=date.load,
    dag=dag)

dim_time = PythonOperator(
    task_id='DimTime',
    provide_context=False,
    python_callable=time.load,
    dag=dag)

status_type = PythonOperator(
    task_id='StatusType',
    provide_context=False,
    python_callable=status_type.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_customer = PythonOperator(
    task_id='DimCustomer',
    provide_context=False,
    python_callable=customer.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_company = PythonOperator(
    task_id='DimCompany',
    provide_context=False,
    python_callable=company.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_security = PythonOperator(
    task_id='DimSecurity',
    provide_context=False,
    python_callable=security.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_broker = PythonOperator(
    task_id='DimBroker',
    provide_context=False,
    python_callable=dim_broker.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_account = PythonOperator(
    task_id='DimAccount',
    provide_context=False,
    python_callable=account.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_watches = PythonOperator(
    task_id='FactWatches',
    provide_context=False,
    python_callable=watches.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_holdings = PythonOperator(
    task_id='FactHoldings',
    provide_context=False,
    python_callable=holdings.load,
    op_kwargs={'conn': conn},
    dag=dag)

prospect = PythonOperator(
    task_id='Prospect',
    provide_context=False,
    python_callable=prospect.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
    dag=dag)

dim_broker >> dim_account
dim_customer >> dim_account
dim_customer >> prospect
dim_date >> prospect
dim_company >> dim_security
# fact_holdings << dim_trade
