import logging

import numpy as np
import pandas as pd

from utils.utils import data_folder_path, get_engine

prospect_file_path = data_folder_path + "Prospect.csv"
NULL = ""


def load(conn, ds):
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    cur.execute("""
        ALTER TABLE DimCustomer
        ADD COLUMN ProspectKey CHAR(232)
        AS (CONCAT(
            UPPER(IFNULL(FirstName, '')),
            UPPER(IFNULL(LastName, '')),
            UPPER(IFNULL(AddressLine1, '')),
            UPPER(IFNULL(AddressLine2, '')),
            UPPER(IFNULL(PostalCode, ''))
        ));
    """)
    cur.execute("ALTER TABLE DimCustomer ADD INDEX(ProspectKey, Status);")
    
    logging.info("Reading input file")
    df_prospect = pd.read_csv(prospect_file_path, na_values=[""], keep_default_na=False,
                              names=["AgencyID", "LastName", "FirstName", "MiddleInitial", "Gender", "AddressLine1",
                                     "AddressLine2", "PostalCode", "City", "State", "Country", "Phone", "Income",
                                     "NumberCars", "NumberChildren", "MaritalStatus", "Age", "CreditRating",
                                     "OwnOrRentFlag", "Employer", "NumberCreditCards", "NetWorth"])
    
    logging.info("Setting up values")
    df_prospect["BatchID"] = 1
    df_prospect["MarketingNameplate"] = df_prospect.parallel_apply(get_marketing_nameplate, axis=1)
    df_prospect["Gender"] = df_prospect["Gender"].apply(convert_genre)
    df_prospect["Date"] = ds  # value used by the trigger
    df_prospect["SK_RecordDateID"] = 0  # value set by the trigger
    df_prospect["SK_UpdateDateID"] = 0  # value set by the trigger
    df_prospect["IsCustomer"] = 0  # value set by the trigger
    df_prospect["ProspectKey"] = df_prospect.parallel_apply(lambda row: ''.join([
        pd.notna(row["LastName"]) and str(row["LastName"]) or NULL,
        pd.notna(row["FirstName"]) and str(row["FirstName"]) or NULL,
        pd.notna(row["AddressLine1"]) and str(row["AddressLine1"]) or NULL,
        pd.notna(row["AddressLine2"]) and str(row["AddressLine2"]) or NULL,
        pd.notna(row["PostalCode"]) and str(row["PostalCode"]) or NULL]), axis=1)
    df_prospect["ProspectKey"] = df_prospect["ProspectKey"].apply(lambda x: x.upper())
    
    df_message = pd.DataFrame({
        "MessageSource": "Prospect",
        "MessageText": "Inserted rows",
        "MessageData": len(df_prospect),
        "MessageType": "Status",
        "BatchID": 1,
    })
    
    logging.info("Inserting into MySQL")
    df_prospect.to_sql("Prospect", index=False, if_exists="append", con=get_engine())
    df_message.to_sql("DImessage", index=False, if_exists="append", con=get_engine())
    
    cur.execute("DROP TRIGGER tpcdi.ADD_Prospect_DateID;")
    cur.execute("ALTER TABLE DimCustomer DROP COLUMN ProspectKey;")
    cur.execute("ALTER TABLE Prospect DROP COLUMN ProspectKey;")
    cur.execute("ALTER TABLE Prospect DROP COLUMN Date;")
    
    conn.commit()


def get_marketing_nameplate(row):
    net_worth = row["NetWorth"]
    income = row["Income"]
    num_children = row["NumberChildren"]
    num_credit_cards = row["NumberCreditCards"]
    age = row["Age"]
    credit_rating = row["CreditRating"]
    num_cars = row["NumberCars"]
    
    result = []
    
    if net_worth and net_worth > 1000000 or income and income > 200000:
        result.append('HighValue')
    
    if num_children and num_children > 3 or num_credit_cards and num_credit_cards > 5:
        result.append('Expenses')
    
    if age and age > 45:
        result.append('Boomer')
    
    if income and income < 50000 or credit_rating and credit_rating < 600 or net_worth and net_worth < 100000:
        result.append('MoneyAlert')
    
    if num_cars and num_cars > 3 or num_credit_cards and num_credit_cards > 7:
        result.append('Spender')
    
    if age and age < 25 and net_worth and net_worth > 1000000:
        result.append('Inherited')
    
    return '+'.join(result)


def convert_genre(genre):
    if genre is not None and pd.notna(genre):
        if genre == NULL:
            return np.nan
        genre = genre.upper()
        if genre != "F" and genre != "M":
            return "U"
        return genre
