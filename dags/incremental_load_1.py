import airflow
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from pandarallel import pandarallel
from inc_update import cash_balances, holdings, market_history, watches
from load import account, customer, di_messages, prospect, trade

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

pandarallel.initialize()

conn = MySqlHook(mysql_conn_id='mysql_tpcdi', local_infile=1).get_conn()

dag = airflow.DAG(
    'incremental_load_1',
    schedule_interval="@once",
    default_args=args,
    template_searchpath="./sql/",
    max_active_runs=1)

dim_account = PythonOperator(
    task_id='DimAccount',
    provide_context=False,
    python_callable=account.load,
    op_kwargs={'conn': conn},
    dag=dag)

dim_customer = PythonOperator(
    task_id='DimCustomer',
    provide_context=False,
    python_callable=customer.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
    dag=dag)

dim_trade = PythonOperator(
    task_id='DimTrade',
    provide_context=False,
    python_callable=trade.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_cash_balances = PythonOperator(
    task_id='FactCashBalances',
    provide_context=False,
    python_callable=cash_balances.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_holdings = PythonOperator(
    task_id='FactHoldings',
    provide_context=False,
    python_callable=holdings.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_market_history = PythonOperator(
    task_id='FactMarketHistory',
    provide_context=False,
    python_callable=market_history.load,
    op_kwargs={'conn': conn},
    dag=dag)

fact_watches = PythonOperator(
    task_id='FactWatches',
    provide_context=False,
    python_callable=watches.load,
    op_kwargs={'conn': conn},
    dag=dag)

prospect = PythonOperator(
    task_id='Prospect',
    provide_context=False,
    python_callable=prospect.load,
    op_kwargs={'conn': conn, 'ds': "{{ ds }}"},
    dag=dag)

start = PythonOperator(
    task_id='Start',
    provide_context=False,
    python_callable=di_messages.record_start,
    op_kwargs={'conn': conn, 'batch_id': 2},
    dag=dag)

end = PythonOperator(
    task_id='End',
    provide_context=False,
    python_callable=di_messages.record_end,
    op_kwargs={'conn': conn, 'batch_id': 2},
    dag=dag)

# start
# start

# first phase
dim_customer << start
fact_market_history << start

# second phase
dim_account << dim_customer
prospect << dim_customer

# third phase
dim_trade << dim_account
fact_cash_balances << dim_account
fact_watches << dim_customer

# fourth phase
fact_holdings << dim_trade

# end
end << fact_holdings
