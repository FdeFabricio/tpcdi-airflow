import logging
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd

from utils.utils import data_folder_path, get_engine

datetime.date  # force datetime import


def load(conn):
    logging.info("Begin Financial - Historical Load")
    cur = conn.cursor()
    
    finwire_schema = [
        ['PTS', 15],
        ['RecType', 3],
        ['Year', 4],
        ['Quarter', 1],
        ['QtrStartDate', 8],
        ['PostingDate', 8],
        ['Revenue', 17],
        ['Earnings', 17],
        ['EPS', 12],
        ['DilutedEPS', 12],
        ['Margin', 12],
        ['Inventory', 17],
        ['Assets', 17],
        ['Liabilities', 17],
        ['shOut', 13],
        ['DilutedShOut', 13],
        ['CoNameOrCIK', [60, 10]]
    ]
    
    financial_map = {
        'CoNameOrCIK': [False, 'CoNameOrCIK'],
        'FI_YEAR': [False, 'Year'],
        'FI_QTR': [False, 'Quarter'],
        'FI_QTR_START_DATE': [True,
                              'datetime.strptime(record[\'QtrStartDate\'].split(\'-\')[0], \'%Y%m%d\').strftime(\'%Y-%m-%d\')'],
        'FI_REVENUE': [False, 'Revenue'],
        'FI_NET_EARN': [False, 'Earnings'],
        'FI_BASIC_EPS': [False, 'EPS'],
        'FI_DILUT_EPS': [False, 'DilutedEPS'],
        'FI_MARGIN': [False, 'Margin'],
        'FI_INVENTORY': [False, 'Inventory'],
        'FI_ASSETS': [False, 'Assets'],
        'FI_LIABILITY': [False, 'Liabilities'],
        'FI_OUT_BASIC': [False, 'shOut'],
        'FI_OUT_DILUT': [False, 'DilutedShOut'],
        'Date': [True, 'datetime.strptime(record[\'PTS\'], \'%Y%m%d-%H%M%S\').strftime(\'%Y-%m-%d\')'],
    }
    
    logging.info("Reading input file")
    df_financial = pd.DataFrame()
    financials = []
    for file in sorted(glob(data_folder_path + "FINWIRE*")):
        if '_audit' in file:
            continue
        with open(file, 'r') as f:
            for line in f:
                if not line[15:18] == 'FIN':
                    continue
                offset = 0
                record = {}
                financial = {}
                for entry in finwire_schema:
                    if entry[0] == 'CoNameOrCIK':
                        value = line[offset:offset + 10].strip()
                        if not value.isdigit():
                            value = line[offset:offset + 60].strip()
                        record[entry[0]] = value
                        continue
                    value = line[offset:offset + entry[1]].strip()
                    record[entry[0]] = value
                    offset += entry[1]
                for k, v in financial_map.items():
                    if not v[0]:
                        financial[k] = record[v[1]]
                    else:
                        try:
                            financial[k] = eval(v[1])
                        except (ValueError, SyntaxError):
                            financial[k] = np.nan
                financial['SK_CompanyID'] = 0  # value set by trigger
                financials.append(financial)
    
    df_financial = df_financial.append(financials, ignore_index=True)
    df_financial.to_csv("financial.txt", index=False)
    logging.info("Inserting into MySQL")
    df_financial.to_sql("Financial", index=False, if_exists="append", con=get_engine())
    
    logging.info("Dropping auxiliary columns")
    cur.execute("DROP TRIGGER tpcdi.ADD_Financial;")
    cur.execute("ALTER TABLE Financial DROP COLUMN Date;")
    cur.execute("ALTER TABLE Financial DROP COLUMN CoNameOrCIK;")
    
    conn.commit()
