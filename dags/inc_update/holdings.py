import logging

import pandas as pd

from utils.utils import inc_data_folder_path, get_engine

file_holdings = inc_data_folder_path + "HoldingHistory.txt"


def load(conn):
    logging.info("Begin FactHoldings - Incremental Update")
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    logging.info("Reading input file")
    df_fact_holdings = pd.read_csv(file_holdings,
                                   names=["CDC_FLAG","CDC_DSN","TradeID", "CurrentTradeID", "HH_BEFORE_QTY", "CurrentHolding"], sep="|")
    df_fact_holdings.drop("HH_BEFORE_QTY", inplace=True, axis=1)
    df_fact_holdings.drop("CDC_FLAG", inplace=True, axis=1)
    df_fact_holdings.drop("CDC_DSN", inplace=True, axis=1)
    
    df_fact_holdings["SK_CustomerID"] = 0  # value set by trigger
    df_fact_holdings["SK_AccountID"] = 0  # value set by trigger
    df_fact_holdings["SK_SecurityID"] = 0  # value set by trigger
    df_fact_holdings["SK_CompanyID"] = 0  # value set by trigger
    df_fact_holdings["SK_DateID"] = 0  # value set by trigger
    df_fact_holdings["SK_TimeID"] = 0  # value set by trigger
    df_fact_holdings["CurrentPrice"] = 0  # value set by trigger
    df_fact_holdings["BatchID"] = 2
    
    logging.info("Incrementally updating into MySQL")
    df_fact_holdings.to_sql("FactHoldings", index=False, if_exists="append", con=get_engine())
    
    logging.info("Dropping auxiliary trigger")
    cur.execute("DROP TRIGGER tpcdi.INC_FactHoldings;")