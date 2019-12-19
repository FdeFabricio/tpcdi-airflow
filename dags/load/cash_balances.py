import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "CashTransaction.txt"


def load(conn):
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    df_ = pd.read_csv(file_path, sep="|", parse_dates={"TS": ["CT_DTS"]}, keep_date_col=True,
                      names=["CT_CA_ID", "CT_DTS", "CT_AMT", "CT_NAME"])
    df_['Date']=df_['TS'].dt.date
    
    df_fact = pd.DataFrame(columns=["SK_CustomerID", "SK_AccountID", "SK_DateID", "Cash", "BatchID"])
    df_fact [['CT_CA_ID', 'Date','DayTotal']] = df_.groupby(['CT_CA_ID', 'Date'])[['CT_AMT']].sum().reset_index()
    
    df_fact["BatchID"] = 1
    df_fact["SK_CustomerID"] = 0
    df_fact["SK_AccountID"] = 0
    df_fact["SK_DateID"] = 0
    df_fact["Cash"] = 0
    
    logging.info("Inserting into MySQL")
    df_fact.to_sql("FactCashBalances", index=False, if_exists="append", con=get_engine())
    
    logging.info("Dropping auxiliary columns")
    cur.execute("DROP TRIGGER tpcdi.ADD_FactCashBalances;")
    cur.execute("ALTER TABLE FactCashBalances DROP COLUMN CT_CA_ID;")
    cur.execute("ALTER TABLE FactCashBalances DROP COLUMN date;")
    cur.execute("ALTER TABLE FactCashBalances DROP COLUMN DayTotal;")

    
    conn.commit()