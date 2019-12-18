from collections import defaultdict
from glob import glob

import pandas as pd
from tqdm import tqdm_notebook as tqdm
from datetime import datetime

from utils.utils import data_folder_path, get_engine

status_type_file_path = data_folder_path + "StatusType.txt"
tmp_file_path = data_folder_path + "dimSecurity.txt"

finwire_schema = [
    ['PTS', 15],
    ['RecType', 3],
    ['Symbol', 15],
    ['IssueType', 6],
    ['Status', 4],
    ['Name', 70],
    ['ExID', 6],
    ['ShOut', 13],
    ['FirstTradeDate', 8],
    ['FirstTradeExchg', 8],
    ['Dividend', 12],
    ['CoNameOrCIK', None]
]

dim_security_map = {
    'SK_SecurityID': [True, ''],
    'Symbol': [False, 'Symbol'],
    'Issue': [False, 'IssueType'],
    'Status': [True, 'status_types[record[\'Status\']]'],
    'Name': [False, 'Name'],
    'ExchangeID': [False, 'ExID'],
    'SK_CompanyID': [False, 'CoNameOrCIK'],
    'SharesOutstanding': [False, 'ShOut'],
    'FirstTrade': [True,
                   'datetime.strptime(record[\'FirstTradeDate\'].split(\'-\')[0], \'%Y%M%d\').strftime(\'%Y-%M-%d\')'],
    'FirstTradeOnExchange': [True,
                             'datetime.strptime(record[\'FirstTradeExchg\'].split(\'-\')[0], \'%Y%M%d\').strftime(\'%Y-%M-%d\')'],
    'Dividend': [False, 'Dividend'],
    'IsCurrent': [True, '\'0\''],
    'BatchID': [True, '\'1\''],
    'EffectiveDate': [True, 'str(datetime.strptime(record[\'PTS\'], \'%Y%m%d-%H%M%S\').strftime(\'%Y-%m-%d\'))'],
    'EndDate': [True, '']
}


def load(conn):
    cur = conn.cursor()

    # needs to execute setup.sql first

    df_security = pd.DataFrame(
        columns=["Symbol", "Issue", "Status", "Name", "ExchangeID", "SK_CompanyID", "SharesOutstanding", "FirstTrade",
                 "FirstTradeOnExchange", "Dividend", "IsCurrent", "BatchID", "EffectiveDate", "EndDate"])
    
    status_types = {}
    with open(status_type_file_path, 'r') as f:
        for line in f:
            split = line.split('|')
            status_types[split[0]] = split[1].strip()
    
    dim_security = defaultdict(list)
    
    for file in tqdm(sorted(glob(data_folder_path + 'FINWIRE*'))):
        if '_audit' in file:
            continue
        with open(file, 'r') as f:
            for line in f:
                if not line[15:18] == 'SEC':
                    continue
                
                is_name = len(line) > 171
                finwire_schema[-1][1] = 60 if is_name else 10
                
                offset = 0
                record = {}
                security = {}
                for entry in finwire_schema:
                    value = line[offset:offset + entry[1]].strip()
                    record[entry[0]] = value
                    offset += entry[1]
                for k, v in dim_security_map.items():
                    if not v[0]:
                        security[k] = record[v[1]]
                    else:
                        try:
                            security[k] = eval(v[1])
                        except SyntaxError:
                            if k == 'EffectiveDate':
                                eval(v[1])
                            security[k] = ''
                
                condition = 'Name = \'{}\'' if is_name else 'CompanyID = {}'
                query = (
                    "SELECT SK_CompanyID, CompanyID FROM DimCompany WHERE {} AND '{}' >= EffectiveDate AND '{}' < EndDate".format(
                        condition.format(security['SK_CompanyID']),
                        security['EffectiveDate'],
                        security['EffectiveDate']))
                _ = cur.execute(query)
                res = cur.fetchone()
                security['SK_CompanyID'] = str(res[0])
                dim_security[security["Symbol"]].append(security)
    
    cur.close()
    conn.close()
    
    for CIK, entries in tqdm(dim_security.items()):
        for (old, new) in zip(entries, entries[1:] + [None]):
            if not new:
                old['IsCurrent'] = '1'
                old['EndDate'] = '9999-12-31'
                continue
            old['EndDate'] = new['EffectiveDate']
    
    for CIK, entries in tqdm(dim_security.items()):
        for entry in entries:
            df_security = df_security.append(entry, ignore_index=True)
    
    df_security["SK_SecurityID"] = df_security.index
    df_security.to_sql("DimSecurity", index=False, if_exists="append", con=get_engine())
