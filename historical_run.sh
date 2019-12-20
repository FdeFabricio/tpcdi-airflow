#!/bin/bash

# batch date used as argument by airflow
batch1_ds=$(cat data/Batch1/BatchDate.txt)

# data warehouse preparation
sudo mysql -uroot -Dtpcdi < setup.sql

# insert historical load start timestap
airflow test historical_load Start $batch1_ds

# phase 1
airflow test historical_load Audit $batch1_ds &&
airflow test historical_load Industry $batch1_ds &&
airflow test historical_load StatusType $batch1_ds &&
airflow test historical_load TaxRate $batch1_ds &&
airflow test historical_load TradeType $batch1_ds &&
airflow test historical_load DimDate $batch1_ds &&
airflow test historical_load DimTime $batch1_ds &&
airflow test historical_load DimBroker $batch1_ds &&
airflow test historical_load DimCustomer $batch1_ds &&
airflow test historical_load DimCompany $batch1_ds

# phase 2
airflow test historical_load DimAccount $batch1_ds &&
airflow test historical_load DimSecurity $batch1_ds &&
airflow test historical_load Financial $batch1_ds &&
airflow test historical_load Prospect $batch1_ds

# phase 3
airflow test historical_load DimTrade $batch1_ds &&
airflow test historical_load FactCashBalances $batch1_ds &&
airflow test historical_load FactMarketHistory $batch1_ds &&
airflow test historical_load FactWatches $batch1_ds

# phase 4
airflow test historical_load FactHoldings $batch1_ds

# insert historical load end timestap
airflow test historical_load End $batch1_ds
