import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "HR.csv"
date_file_path = data_folder_path + "Date.txt"


def load(conn):
    logging.info("Begin DimBroker - Historical Load")
    cur = conn.cursor()
    min_date = pd.read_csv(date_file_path, header=None, delimiter="|")[1].min()
    df = pd.read_csv(file_path,
                     names=["BrokerID", "ManagerID", "FirstName", "LastName", "MiddleInitial", "EmployeeJobCode",
                            "Branch", "Office", "Phone"])
    df = df[df["EmployeeJobCode"] == 314]
    df.drop("EmployeeJobCode", axis=1, inplace=True)
    
    df["IsCurrent"] = 1
    df["BatchID"] = 1
    df["EffectiveDate"] = min_date
    df["EndDate"] = "9999-12-31"
    df["SK_BrokerID"] = df.index
    
    df.to_sql("DimBroker", index=False, if_exists="append", con=get_engine())
    
    logging.info("Adding index to table")
    cur.execute("ALTER TABLE DimBroker ADD INDEX(BrokerID);")
    conn.commit()
