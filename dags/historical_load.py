import airflow
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from pandarallel import pandarallel

from load import account, audit, broker, company, customer, date, financial, holdings, industry, prospect, security, status_type, \
    tax_rate, time, trade, trade_type, watches

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),  # TODO change this
    'provide_context': True
}

pandarallel.initialize()

conn = MySqlHook(mysql_conn_id='mysql_tpcdi', local_infile=1).get_conn()

dag = airflow.DAG(
    'historical_load',
    schedule_interval="@once",
    default_args=args,
    template_searchpath="./sql/",
    max_active_runs=1)

audit = PythonOperator(
    task_id='Audit',
    provide_context=False,
    python_callable=audit.load,
    dag=dag)

dim_date = PythonOperator(
    task_id='DimDate',
    provide_context=False,
    python_callable=date.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_time = PythonOperator(
    task_id='DimTime',
    provide_context=False,
    python_callable=time.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_customer = PythonOperator(
    task_id='DimCustomer',
    provide_context=False,
    python_callable=customer.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
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
    op_kwargs={'conn': conn},
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

industry = PythonOperator(
    task_id='Industry',
    provide_context=False,
    python_callable=industry.load,
    dag=dag)

prospect = PythonOperator(
    task_id='Prospect',
    provide_context=False,
    python_callable=prospect.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
    dag=dag)

status_type = PythonOperator(
    task_id='StatusType',
    provide_context=False,
    python_callable=status_type.load,
    dag=dag)

tax_rate = PythonOperator(
    task_id='TaxRate',
    provide_context=False,
    python_callable=tax_rate.load,
    dag=dag)

trade_type = PythonOperator(
    task_id='TradeType',
    provide_context=False,
    python_callable=trade_type.load,
    dag=dag)


# first phase
# audit
# industry
# status_type
# tax_rate
# trade_type
# dim_date
# dim_time
# dim_broker
# dim_customer
# dim_company

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
