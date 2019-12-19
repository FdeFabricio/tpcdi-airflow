from collections import defaultdict
from datetime import datetime
from glob import glob

import numpy as np
import pandas as pd
from tqdm import tqdm_notebook as tqdm

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.sql import select


from utils.utils import bulk_load


from utils.utils import data_folder_path, get_engine

datetime.date  # force datetime import


def load():

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
        ['CoNameOrCIK', [60,10]]
    ]
    
    financial_map = {
        'SK_CompanyID': [True, ''],
        'FI_YEAR': [False, 'Year'],
        'FI_QTR': [False, 'Quarter'],
        'FI_QTR_START_DATE': [True, 'datetime.strptime(record[\'QtrStartDate\'].split(\'-\')[0], \'%Y%m%d\').strftime(\'%Y-%m-%d\')'],
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
    }
    
    financials = []
    df_financials = pd.DataFrame()
    counter=1
    for file in tqdm(sorted(glob(data_folder_path + "FINWIRE*"))):
        if '_audit' in file:
            continue
        print(file)
        counter+=1
        with open(file, 'r') as f:
            for line in f:
                if not line[15:18] == 'FIN':
                    continue
                offset = 0
                record = {}
                financial = {}
                for entry in finwire_schema:
                    value = None
                    if entry[0] == 'CoNameOrCIK':
                        value = line[offset:offset+10].strip()
                        if not value.isdigit():
                            value = line[offset:offset+60].strip()
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
                            
                ##########################################################################
                engine = get_engine()
                metadata=MetaData()
                dimcompany = Table('dimcompany', metadata, autoload=True, autoload_with=engine)
                s = None
                if record['CoNameOrCIK'].isdigit():
                    s = select([dimcompany.c.SK_CompanyID]).where(dimcompany.c.CompanyID==record['CoNameOrCIK'])
                else:
                    s = select([dimcompany.c.SK_CompanyID]).where(dimcompany.c.Name==record['CoNameOrCIK'])
                financial['SK_CompanyID'] = engine.execute(s).scalar()
               
                financials = financials + [financial]

    df_financials = pd.DataFrame(financials)     
    df_financials.to_sql("Financial", index=False, if_exists="append", con=get_engine())