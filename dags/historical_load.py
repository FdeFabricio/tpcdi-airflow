import airflow
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from pandarallel import pandarallel

from load import account, broker, company, customer, date, financial, holdings, prospect, security, status_type, time, \
    trade, watches

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),  # TODO change this
    'provide_context': True
}

pandarallel.initialize(nb_workers=2)

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
    dag=dag)

dim_company = PythonOperator(
    task_id='DimCompany',
    provide_context=False,
    python_callable=company.load,
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
    python_callable=broker.load,
    dag=dag)

dim_account = PythonOperator(
    task_id='DimAccount',
    provide_context=False,
    python_callable=account.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_trade = PythonOperator(
    task_id='DimTrade',
    provide_context=False,
    python_callable=trade.load,
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

financial = PythonOperator(
    task_id='Financial',
    provide_context=False,
    python_callable=financial.load,
    op_kwargs={'conn': conn},
    dag=dag)

prospect = PythonOperator(
    task_id='Prospect',
    provide_context=False,
    python_callable=prospect.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
    dag=dag)

# first phase

# second phase
dim_account << dim_broker
dim_account << dim_customer
dim_security << dim_company
financial << dim_company
prospect << dim_customer
prospect << dim_date

# third phase
dim_trade << dim_account
dim_trade << dim_date
dim_trade << dim_security
dim_trade << dim_time
# fact_cash_balance << dim_account
# fact_cash_balance << dim_date
# fact_cash_balance << dim_time
# fact_market_history << dim_date
# fact_market_history << dim_security
# fact_market_history << financial
fact_watches << dim_customer
fact_watches << dim_security
fact_watches << dim_date

# fourth phase
fact_holdings << dim_trade
