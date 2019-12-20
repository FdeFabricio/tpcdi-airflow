#!/bin/bash

# batch date used as argument by airflow
batch3_ds=$(cat data/Batch3/BatchDate.txt)

# insert historical load start timestap
airflow test historical_load Start $batch3_ds

# phase 1
airflow test historical_load DimCustomer $batch3_ds &&
airflow test historical_load FactMarketHistory $batch3_ds

# phase 2
airflow test historical_load DimAccount $batch3_ds &&
airflow test historical_load Prospect $batch3_ds

# phase 3
airflow test historical_load DimTrade $batch3_ds &&
airflow test historical_load FactCashBalances $batch3_ds &&
airflow test historical_load FactWatches $batch3_ds

# phase 4
airflow test historical_load FactHoldings $batch3_ds

# insert historical load end timestap
airflow test historical_load End $batch3_ds
