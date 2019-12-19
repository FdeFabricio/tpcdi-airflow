import logging

import numpy as np
import pandas as pd
from lxml import etree as et

from utils.utils import data_folder_path, get_engine

customer_file_path = data_folder_path + "CustomerMgmt.xml"
NULL = ""


def load(conn):
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    df_accounts = pd.DataFrame(
        columns=["AccountID", "Status", "AccountDesc", "TaxStatus", "IsCurrent", "BatchID", "EffectiveDate", "EndDate"])
    
    tree = et.parse(customer_file_path)
    actions = tree.getroot()
    
    logging.info("Reading input file")
    for action in actions:
        action_type = action.attrib['ActionType']
        customer = action.find('Customer')
        
        if action_type in ["NEW", "ADDACCT"]:
            account = customer.find("Account")
            row = {
                "AccountID": account.attrib["CA_ID"],
                "TaxStatus": account.attrib["CA_TAX_ST"],
                "AccountDesc": get_2l_data(customer, "Account", "CA_NAME"),
                "Status": "ACTIVE",
                "IsCurrent": True,
                "BatchID": 1,
                "EffectiveDate": action.attrib["ActionTS"][:10],
                "EndDate": "9999-12-31",
                
                "SK_CustomerID": customer.attrib['C_ID'],
                "SK_BrokerID": get_2l_data(customer, "Account", "CA_B_ID"),
            }
            
            if row["AccountDesc"] == "":
                row["AccountDesc"] = np.nan
            
            df_accounts = df_accounts.append(row, ignore_index=True)
        
        elif action_type == "UPDACCT":
            action_ts = action.attrib["ActionTS"][:10]
            account = customer.find("Account")
            acc_id = account.attrib["CA_ID"]
            
            new_values = {
                "AccountDesc": get_2l_data(customer, "Account", "CA_NAME"),
                "TaxStatus": account.attrib.get("CA_TAX_ST", None),
                "EffectiveDate": action_ts,
                "SK_BrokerID": get_2l_data(customer, "Account", "CA_B_ID")
            }
            
            old_index = df_accounts[(df_accounts.AccountID == acc_id) & df_accounts.IsCurrent].index.values[0]
            new_row = df_accounts.loc[old_index, :].copy()
            
            df_accounts.loc[old_index, "IsCurrent"] = False
            df_accounts.loc[old_index, "EndDate"] = action_ts
            
            for attrib, value in new_values.items():
                if value is not None:
                    if value == NULL:
                        new_row[attrib] = np.nan
                    else:
                        new_row[attrib] = value
            
            df_accounts = df_accounts.append(new_row, ignore_index=True)
        
        elif action_type == "CLOSEACCT":
            action_ts = action.attrib["ActionTS"][:10]
            acc_id = customer.find("Account").attrib["CA_ID"]
            
            old_index = df_accounts[(df_accounts.AccountID == acc_id) & df_accounts.IsCurrent].index.values[0]
            new_row = df_accounts.loc[old_index, :].copy()
            
            df_accounts.loc[old_index, "IsCurrent"] = False
            df_accounts.loc[old_index, "EndDate"] = action_ts
            
            new_row["Status"] = "INACTIVE"
            new_row["EffectiveDate"] = action_ts
            
            df_accounts = df_accounts.append(new_row, ignore_index=True)
        
        elif action_type == "INACT":
            action_ts = action.attrib["ActionTS"][:10]
            c_id = customer.attrib["C_ID"]
            
            for acc_id in df_accounts[(df_accounts.SK_CustomerID == c_id) & df_accounts.IsCurrent].AccountID:
                old_index = df_accounts[(df_accounts.AccountID == acc_id) & df_accounts.IsCurrent].index.values[0]
                new_row = df_accounts.loc[old_index, :].copy()
                
                df_accounts.loc[old_index, "IsCurrent"] = False
                df_accounts.loc[old_index, "EndDate"] = action_ts
                
                new_row["Status"] = "INACTIVE"
                new_row["EffectiveDate"] = action_ts
                
                df_accounts = df_accounts.append(new_row, ignore_index=True)
    
    logging.info("Inserting into MySQL")
    df_accounts["SK_AccountID"] = df_accounts.index
    df_accounts.to_sql("DimAccount", index=False, if_exists="append", con=get_engine())
    
    cur.execute("DROP TRIGGER tpcdi.ADD_DimAccount;")
    
    conn.commit()


def get_2l_data(customer, first, second):
    try:
        value = customer.find(first).find(second).text
        if value is None:
            return NULL
        return value
    except AttributeError:
        return None
