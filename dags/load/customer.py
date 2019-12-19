import logging

import numpy as np
import pandas as pd
from lxml import etree as et

from load.prospect import get_marketing_nameplate
from utils.utils import data_folder_path, to_upper, get_engine

customer_file_path = data_folder_path + "CustomerMgmt.xml"
tax_rate_file_path = data_folder_path + "TaxRate.txt"
prospect_file_path = data_folder_path + "Prospect.csv"

NULL = ""


def load(conn):
    cur = conn.cursor()
    tax_rate = get_tax_rate()
    df_prospect = get_prospect_df()
    
    df_customers = pd.DataFrame(
        columns=["CustomerID", "TaxID", "Status", "LastName", "FirstName", "MiddleInitial", "Gender", "Tier", "DOB",
                 "AddressLine1", "AddressLine2", "PostalCode", "City", "StateProv", "Country", "Phone1", "Phone2",
                 "Phone3", "Email1", "Email2", "NationalTaxRateDesc", "NationalTaxRate", "LocalTaxRateDesc",
                 "LocalTaxRate", "AgencyID", "CreditRating", "NetWorth", "MarketingNameplate", "IsCurrent", "BatchID",
                 "EffectiveDate", "EndDate"])
    
    updates = {}
    
    tree = et.parse(customer_file_path)
    actions = tree.getroot()
    
    logging.info("Reading input file")
    for action in actions:
        action_type = action.attrib['ActionType']
        customer = action.find('Customer')
        
        if action_type in ["NEW", "UPDCUST"]:
            row = {
                "CustomerID": customer.attrib.get('C_ID', None),
                "EffectiveDate": action.attrib["ActionTS"][:10],
                "TaxID": customer.attrib.get('C_TAX_ID', None),
                
                "LastName": get_2l_data(customer, "Name", "C_L_NAME"),
                "FirstName": get_2l_data(customer, "Name", "C_F_NAME"),
                "MiddleInitial": get_2l_data(customer, "Name", "C_M_NAME"),
                
                "Gender": customer.attrib.get('C_GNDR', None),
                "Tier": customer.attrib.get('C_TIER', None),
                "DOB": customer.attrib.get('C_DOB', None),
                
                "AddressLine1": get_2l_data(customer, "Address", "C_ADLINE1"),
                "AddressLine2": get_2l_data(customer, "Address", "C_ADLINE2"),
                "PostalCode": get_2l_data(customer, "Address", "C_ZIPCODE"),
                "City": get_2l_data(customer, "Address", "C_CITY"),
                "StateProv": get_2l_data(customer, "Address", "C_STATE_PROV"),
                "Country": get_2l_data(customer, "Address", "C_CTRY"),
                
                "Phone1": get_phone(1, customer),
                "Phone2": get_phone(2, customer),
                "Phone3": get_phone(3, customer),
                "Email1": get_2l_data(customer, "ContactInfo", "C_PRIM_EMAIL"),
                "Email2": get_2l_data(customer, "ContactInfo", "C_ALT_EMAIL"),
            }
            
            if row["Gender"] is not None and row["Gender"] != NULL:
                row["Gender"] = row["Gender"].upper()
                if row["Gender"] not in ["F", "M"]:
                    row["Gender"] = "U"
            
            nat_tax_id = get_2l_data(customer, "TaxInfo", "C_NAT_TX_ID")
            if nat_tax_id:
                row["NationalTaxRateDesc"] = tax_rate[nat_tax_id]["name"]
                row["NationalTaxRate"] = tax_rate[nat_tax_id]["rate"]
            
            lcl_tax_id = get_2l_data(customer, "TaxInfo", "C_LCL_TX_ID")
            if lcl_tax_id:
                row["LocalTaxRateDesc"] = tax_rate[lcl_tax_id]["name"]
                row["LocalTaxRate"] = tax_rate[lcl_tax_id]["rate"]
            
            if action_type == "NEW":
                row["Status"] = "ACTIVE"
                row["IsCurrent"] = True
                row["BatchID"] = 1
                row["EndDate"] = "9999-12-31"
                df_new = pd.DataFrame(row, index=[0])
                df_new.fillna(np.nan, inplace=True)
                df_customers = df_customers.append(row, ignore_index=True)
            
            elif action_type == "UPDCUST":
                if row["CustomerID"] not in updates:
                    updates[row["CustomerID"]] = []
                updates[row["CustomerID"]].append(row)
        
        elif action_type == "INACT":
            customer_id = customer.attrib.get('C_ID', None)
            action_ts = action.attrib["ActionTS"][:10]
            
            if customer_id not in updates:
                updates[customer_id] = []
            updates[customer_id].append({"CustomerID": customer_id, "EffectiveDate": action_ts, "Status": "INACTIVE"})
    
    logging.info("Applying updates")
    for c_id, upds in updates.items():
        for upda in upds:
            action_ts = upda["EffectiveDate"]
            old_index = df_customers[(df_customers.CustomerID == c_id) & df_customers.IsCurrent].index.values[0]
            new_row = df_customers.loc[old_index, :].copy()
            
            df_customers.loc[old_index, "IsCurrent"] = False
            df_customers.loc[old_index, "EndDate"] = action_ts
            
            for attrib, value in upda.items():
                if value is not None:
                    if value == "":
                        new_row[attrib] = np.nan
                    else:
                        new_row[attrib] = value
            
            df_customers = df_customers.append(new_row, ignore_index=True)
    
    logging.info("Adding prospect data")
    df_customers["ProspectKey"] = df_customers["LastName"].apply(to_upper) + \
                                  df_customers["FirstName"].apply(to_upper) + \
                                  df_customers["AddressLine1"].apply(to_upper) + \
                                  df_customers["AddressLine2"].apply(to_upper) + \
                                  df_customers["PostalCode"].apply(to_upper)
    df_customers["AgencyID"] = df_customers.apply(lambda x: get_prospect(x, df_prospect)[0], axis=1)
    df_customers["CreditRating"] = df_customers.apply(lambda x: get_prospect(x, df_prospect)[1], axis=1)
    df_customers["NetWorth"] = df_customers.apply(lambda x: get_prospect(x, df_prospect)[2], axis=1)
    df_customers["MarketingNameplate"] = df_customers.apply(lambda x: get_prospect(x, df_prospect)[3], axis=1)
    df_customers.drop("ProspectKey", inplace=True, axis=1)
    
    df_customers.replace("", np.nan, inplace=True)
    df_customers["SK_CustomerID"] = df_customers.index
    
    logging.info("Inserting into MySQL")
    df_customers.to_sql("DimCustomer", index=False, if_exists="append", con=get_engine())

    logging.info("Adding index to table")
    cur.execut("ALTER TABLE DimCustomer ADD INDEX(CustomerID, EndDate, EffectiveDate);")
    conn.commit()


def get_2l_data(customer, first, second):
    try:
        value = customer.find(first).find(second).text
        if value is None:
            return NULL
        return value
    except AttributeError:
        return None


def get_phone(i, customer):
    try:
        phone_info = customer.find("ContactInfo").find("C_PHONE_" + str(i))
    except AttributeError:
        return None
    
    try:
        c_cc = phone_info.find("C_CTRY_CODE").text
    except AttributeError:
        c_cc = None
    
    try:
        c_ac = phone_info.find("C_AREA_CODE").text
    except AttributeError:
        c_ac = None
    
    try:
        c_l = phone_info.find("C_LOCAL").text
    except AttributeError:
        c_l = None
    
    try:
        c_e = phone_info.find("C_EXT").text
    except AttributeError:
        c_e = None
    
    if c_cc and c_ac and c_l:
        phone = '+' + c_cc + ' (' + c_ac + ') ' + c_l
    elif c_ac and c_l:
        phone = '(' + c_ac + ') ' + c_l
    elif c_l:
        phone = c_l
    else:
        return ""
    
    if c_e:
        return phone + c_e
    
    return phone


def get_tax_rate():
    tax_rate = {}
    with open(tax_rate_file_path, 'r') as file:
        for line in file:
            arr = line.split('|')
            tax_rate[arr[0]] = {"name": arr[1], "rate": float(arr[2])}
    return tax_rate


def get_prospect_df():
    df = pd.read_csv(prospect_file_path,
                     names=["AgencyID", "LastName", "FirstName", "MiddleInitial", "Gender", "AddressLine1",
                            "AddressLine2", "PostalCode", "City", "State", "Country", "Phone", "Income",
                            "NumberCars", "NumberChildren", "MaritalStatus", "Age", "CreditRating",
                            "OwnOrRentFlag", "Employer", "NumberCreditCards", "NetWorth"])
    
    df["key"] = df.apply(lambda row: ''.join([
        pd.notna(row["LastName"]) and str(row["LastName"]) or NULL,
        pd.notna(row["FirstName"]) and str(row["FirstName"]) or NULL,
        pd.notna(row["AddressLine1"]) and str(row["AddressLine1"]) or NULL,
        pd.notna(row["AddressLine2"]) and str(row["AddressLine2"]) or NULL,
        pd.notna(row["PostalCode"]) and str(row["PostalCode"]) or NULL]), axis=1)
    
    df["key"] = df["key"].apply(lambda x: x.upper())
    df = df.set_index("key")
    
    df["nameplate"] = df.apply(get_marketing_nameplate, axis=1)
    
    return df


def get_prospect(c, df_prospect):
    if c["IsCurrent"] and c["ProspectKey"] in df_prospect.index:
        row = df_prospect.loc[c["ProspectKey"]]
        return [row["AgencyID"], row["CreditRating"], row["NetWorth"], get_marketing_nameplate(row)]
    else:
        return [np.nan, np.nan, np.nan, np.nan]
