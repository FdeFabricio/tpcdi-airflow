import logging

import numpy as np
import pandas as pd

from utils.utils import data_folder_path, get_engine

file_status_type = data_folder_path + "StatusType.txt"
file_trade_type = data_folder_path + "TradeType.txt"
file_trade = data_folder_path + "Trade.txt"
file_trade_history = data_folder_path + "TradeHistory.txt"


def load(conn):
    cur = conn.cursor()
    
    df_messages = pd.DataFrame(
        columns=["MessageDateAndTime", "BatchID", "MessageSource", "MessageText", "MessageType", "MessageData"])
    
    # needs to execute setup.sql first
    logging.info("Reading input files")
    status_types = {}
    with open(file_status_type, 'r') as f:
        for line in f:
            split = line.split('|')
            status_types[split[0]] = split[1].strip()
    
    trade_types = {}
    with open(file_trade_type, 'r') as f:
        for line in f:
            split = line.split('|')
            trade_types[split[0]] = split[1].strip()
    
    df_trade = pd.read_csv(file_trade,
                           names=["T_ID", "T_DTS", "T_ST_ID", "T_TT_ID", "T_IS_CASH", "T_S_SYMB", "T_QTY",
                                  "T_BID_PRICE", "T_CA_ID", "T_EXEC_NAME", "T_TRADE_PRICE", "T_CHRG", "T_COMM",
                                  "T_TAX"], sep="|")
    df_trade_history = pd.read_csv(file_trade_history, names=["TH_T_ID", "TH_DTS", "TH_ST_ID"], sep="|")
    
    df_trade_history["TH_DTS_Date"] = df_trade_history["TH_DTS"].str[:10]
    df_trade_history["TH_DTS_Time"] = df_trade_history["TH_DTS"].str[11:19]
    
    df_merged = pd.merge(df_trade, df_trade_history, left_on="T_ID", right_on="TH_T_ID")
    
    df_merged["BatchID"] = 1
    df_merged["Status"] = df_merged["T_ST_ID"].apply(lambda x: status_types[x])
    df_merged["Type"] = df_merged["T_TT_ID"].apply(lambda x: trade_types[x])
    
    logging.info("Applying updates")
    df_merged = df_merged.head(1000).groupby("T_ID").parallel_apply(group_updates)
    
    df_merged.rename(columns={
        "T_ID": "TradeID", "T_IS_CASH": "CashFlag", "T_QTY": "Quantity", "T_BID_PRICE": "BidPrice",
        "T_EXEC_NAME": "ExecutedBy", "T_TRADE_PRICE": "TradePrice", "T_CHRG": "Fee", "T_COMM": "Commission",
        "T_TAX": "Tax", "T_S_SYMB": "Symbol", "T_CA_ID": "AccountID", "TH_DTS_Date": "Date"}, inplace=True)
    
    df_merged["SK_SecurityID"] = 0  # value set by trigger
    df_merged["SK_CompanyID"] = 0  # value set by trigger
    df_merged["SK_AccountID"] = 0  # value set by trigger
    df_merged["SK_CustomerID"] = 0  # value set by trigger
    df_merged["SK_BrokerID"] = 0  # value set by trigger
    df_merged["SK_CreateDateID"] = 0  # value set by trigger
    df_merged["SK_CreateTimeID"] = 0  # value set by trigger
    df_merged["SK_CloseDateID"] = np.nan  # value set by trigger
    df_merged["SK_CloseTimeID"] = np.nan  # value set by trigger
    
    df_merged.drop("T_DTS", inplace=True, axis=1)
    df_merged.drop("T_ST_ID", inplace=True, axis=1)
    df_merged.drop("T_TT_ID", inplace=True, axis=1)
    df_merged.drop("TH_T_ID", inplace=True, axis=1)
    df_merged.drop("TH_DTS", inplace=True, axis=1)
    df_merged.drop("TH_ST_ID", inplace=True, axis=1)
    df_merged.drop("TH_DTS_Time", inplace=True, axis=1)
    
    df_invalid_commission = df_merged[pd.notna(df_merged["Commission"]) & (
        df_merged["Commission"].astype("float") > df_merged["TradePrice"].astype("float") * df_merged[
        "Quantity"].astype("float"))][["Commission", "TradeID"]]
    df_messages["MessageData"] = df_invalid_commission.apply(
        lambda x: "T_ID = " + x["TradeID"].astype(str) + ", T_COMM = " + x["Commission"].astype(str), axis=1)
    df_messages["BatchID"] = 1
    df_messages["MessageSource"] = "DimTrade"
    df_messages["MessageType"] = "Alert"
    df_messages["MessageText"] = "Invalid trade commission"
    
    df_invalid_fee = df_merged[pd.notna(df_merged["Fee"]) & (
            df_merged["Fee"].astype("float") > df_merged["TradePrice"].astype("float") * df_merged["Quantity"].astype(
            "float"))][["Fee", "TradeID"]]
    df_invalid_fee["MessageData"] = df_invalid_fee.apply(
        lambda x: "T_ID = " + x["TradeID"].astype(str) + ", T_CHRG = " + x["Fee"].astype(str), axis=1)
    df_invalid_fee["BatchID"] = 1
    df_invalid_fee["MessageSource"] = "DimTrade"
    df_invalid_fee["MessageType"] = "Alert"
    df_invalid_fee["MessageText"] = "Invalid trade fee"
    df_invalid_fee.drop("Fee", inplace=True, axis=1)
    df_invalid_fee.drop("TradeID", inplace=True, axis=1)
    df_messages = df_messages.append(df_invalid_fee, ignore_index=True)
    
    logging.info("Inserting into MySQL")
    df_merged.to_sql("DimTrade", index=False, if_exists="append", chunksize=1, con=get_engine())
    df_messages.to_sql("DImessages", index=False, if_exists="append", chunksize=1, con=get_engine())
    
    logging.info("Dropping auxiliary columns")
    cur.execute("DROP TRIGGER tpcdi.ADD_DimTrade;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN Date;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN CreateDate;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN CreateTime;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN CloseDate;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN CloseTime;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN Symbol;")
    cur.execute("ALTER TABLE DimTrade DROP COLUMN AccountID;")
    
    conn.commit()


def group_updates(df_group):
    current = df_group.iloc[0].copy()
    for _, row in df_group.iterrows():
        update_dates(row, current)
    return current


def update_dates(row, current):
    if row["TH_ST_ID"] == "PNDG" or row["TH_ST_ID"] == "SBMT" and row["T_TT_ID"] in ["TMB", "TMS"]:
        current["CreateDate"] = row["TH_DTS_Date"]
        current["CreateTime"] = row["TH_DTS_Time"]
    elif row["TH_ST_ID"] in ["CMPT", "CNCL"]:
        current["CloseDate"] = row["TH_DTS_Date"]
        current["CloseTime"] = row["TH_DTS_Time"]
