#!/bin/bash

# batch date used as argument by airflow
batch2_ds=$(cat data/Batch2/BatchDate.txt)

# insert historical load start timestap
airflow test historical_load Start $batch2_ds

# phase 1
airflow test historical_load DimCustomer $batch2_ds &&
airflow test historical_load FactMarketHistory $batch2_ds

# phase 2
airflow test historical_load DimAccount $batch2_ds &&
airflow test historical_load Prospect $batch2_ds

# phase 3
airflow test historical_load DimTrade $batch2_ds &&
airflow test historical_load FactCashBalances $batch2_ds &&
airflow test historical_load FactWatches $batch2_ds

# phase 4
airflow test historical_load FactHoldings $batch2_ds

# insert historical load end timestap
airflow test historical_load End $batch2_ds
