import pandas as pd
import logging

from utils.utils import data_folder_path, get_engine

file_watch_history = data_folder_path + "WatchHistory.txt"


def load(conn):
    logging.info("Begin FactWatches - Historical Load")
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    logging.info("Reading input file")
    df_file = pd.read_csv(file_watch_history, names=["W_C_ID", "W_S_SYMB", "W_DTS", "W_ACTION"], sep="|")
    
    df_fact_watches = pd.DataFrame(
        columns=["SK_CustomerID", "SK_SecurityID", "SK_DateID_DatePlaced",
                 "SK_DateID_DateRemoved", "BatchID"])
    
    df_add = df_file[df_file["W_ACTION"] == "ACTV"]
    df_fact_watches["CustomerID"] = df_add["W_C_ID"]  # value used by trigger
    df_fact_watches["Symbol"] = df_add["W_S_SYMB"]  # value used by trigger
    df_fact_watches["Date"] = df_add["W_DTS"].str[:10]  # value used by trigger
    df_fact_watches["SK_CustomerID"] = 0  # value set by trigger
    df_fact_watches["SK_SecurityID"] = 0  # value set by trigger
    df_fact_watches["SK_DateID_DatePlaced"] = 0  # value set by trigger
    df_fact_watches["BatchID"] = 1
    
    logging.info("Applying updates")
    df_rem = df_file[df_file["W_ACTION"] == "CNCL"]
    df_fact_watches = pd.merge(df_fact_watches, df_rem, how="left",
                               left_on=["CustomerID", "Symbol"],
                               right_on=["W_C_ID", "W_S_SYMB"])
    df_fact_watches["DateRemoved"] = df_fact_watches["W_DTS"].str[:10]
    
    df_fact_watches.drop("W_C_ID", inplace=True, axis=1)
    df_fact_watches.drop("W_S_SYMB", inplace=True, axis=1)
    df_fact_watches.drop("W_DTS", inplace=True, axis=1)
    df_fact_watches.drop("W_ACTION", inplace=True, axis=1)
    
    logging.info("Inserting into MySQL")
    df_fact_watches.to_sql("FactWatches", index=False, if_exists="append", con=get_engine())
    
    logging.info("Dropping auxiliary columns")
    cur.execute("DROP TRIGGER tpcdi.ADD_FactWatches;")
    cur.execute("ALTER TABLE FactWatches DROP COLUMN CustomerID;")
    cur.execute("ALTER TABLE FactWatches DROP COLUMN Symbol;")
    cur.execute("ALTER TABLE FactWatches DROP COLUMN Date;")
    cur.execute("ALTER TABLE FactWatches DROP COLUMN DateRemoved;")
