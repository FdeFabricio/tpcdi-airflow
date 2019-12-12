import logging
from lxml import etree as et
import pandas as pd
import numpy as np
from utils.utils import data_folder_path, to_upper, get_engine

customer_file_path = data_folder_path + "CustomerMgmt.xml"

NULL = ""


def load(conn):
    cur = conn.cursor()
    
    logging.info("Creating table")
    cur.execute("""
        DROP TABLE IF EXISTS DimAccount;
        CREATE TABLE DimAccount (
            SK_AccountID INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
            AccountID INTEGER NOT NULL,
            SK_BrokerID INTEGER NOT NULL,
            SK_CustomerID INTEGER NOT NULL,
            Status CHAR(10) NOT NULL,
            AccountDesc CHAR(50),
            TaxStatus NUMERIC(1) NOT NULL CHECK (TaxStatus IN (0, 1, 2)),
            IsCurrent BOOLEAN NOT NULL,
            BatchID NUMERIC(5) NOT NULL,
            EffectiveDate DATE NOT NULL,
            EndDate DATE NOT NULL
        );
        
        DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_CustomerID;
        DROP TRIGGER IF EXISTS tpcdi.ADD_DimAccount_SK_BrokerID;
        delimiter |
        CREATE TRIGGER `ADD_DimAccount_SK_CustomerID` BEFORE INSERT ON `DimAccount`
        FOR EACH ROW
        BEGIN
            SET NEW.SK_CustomerID = (
                SELECT DimCustomer.SK_CustomerID, DimCustomer.Status
                FROM DimCustomer
                WHERE DimCustomer.CustomerID = NEW.SK_CustomerID AND NEW.EndDate <= DimCustomer.EndDate
                LIMIT 1
            );
        END;
        |
        CREATE TRIGGER `ADD_DimAccount_SK_BrokerID` BEFORE INSERT ON `DimAccount`
        FOR EACH ROW
        BEGIN
            SET NEW.SK_BrokerID = (
                SELECT DimBroker.SK_BrokerID
                FROM DimBroker
                WHERE DimBroker.BrokerID = NEW.SK_BrokerID
            );
        END;
        |
        delimiter ;
    """)
    
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
    df_accounts.to_sql("DimAccount", index=False, if_exists="append", con=get_engine())
    
    cur.execute("""
        DROP TRIGGER tpcdi.ADD_DimAccount_SK_CustomerID;
        DROP TRIGGER tpcdi.ADD_DimAccount_SK_BrokerID;
    """)


def get_2l_data(customer, first, second):
    try:
        value = customer.find(first).find(second).text
        if value is None:
            return NULL
        return value
    except AttributeError:
        return None
