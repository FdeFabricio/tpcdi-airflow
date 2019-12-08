import os
from glob import glob
from collections import defaultdict
from tqdm import tqdm_notebook as tqdm
from utils.utils import data_folder_path, bulk_load

status_type_file_path = data_folder_path + "StatusType.txt"
industry_file_path = data_folder_path + "Industry.txt"
tmp_file_path = data_folder_path + "dimCompany.txt"


def load(conn):
    status_types = {}
    with open(status_type_file_path, 'r') as f:
        for line in f:
            split = line.split('|')
            status_types[split[0]] = split[1].strip()
    
    industries = {}
    with open(industry_file_path, 'r') as f:
        for line in f:
            split = line.split('|')
            industries[split[0]] = split[1].strip()
    
    finwire_schema = [
        ['PTS', 15],
        ['RecType', 3],
        ['CompanyName', 60],
        ['CIK', 10],
        ['Status', 4],
        ['IndustryID', 2],
        ['SPrating', 4],
        ['FoundingDate', 8],
        ['AddrLine1', 80],
        ['AddrLine2', 80],
        ['PostalCode', 12],
        ['City', 25],
        ['StateProvince', 20],
        ['Country', 24],
        ['CEOname', 46],
        ['Description', 150]
    ]

    dim_company_map = {
        'SK_CompanyID': [True, ''],
        'CompanyID': [False, 'CIK'],
        'Status': [True, 'status_types[record[\'Status\']]'],
        'Name': [False, 'CompanyName'],
        'Industry': [True, 'industries[record[\'IndustryID\']]'],
        'SPrating': [False, 'SPrating'],
        'isLowGrade': [True,
                       '\'0\' if record[\'SPrating\'] and (record[\'SPrating\'].startswith(\'A\') or record[\'SPrating\'].startswith(\'BBB\')) else \'1\''],
        'CEO': [False, 'CEOname'],
        'AddressLine1': [False, 'AddrLine1'],
        'AddressLine2': [False, 'AddrLine2'],
        'PostalCode': [False, 'PostalCode'],
        'City': [False, 'City'],
        'StateProv': [False, 'StateProvince'],
        'Country': [False, 'Country'],
        'Description': [False, 'Description'],
        'FoundingDate': [True,
                         'datetime.strptime(record[\'FoundingDate\'].split(\'-\')[0], \'%Y%m%d\').strftime(\'%Y-%m-%d\')'],
        'IsCurrent': [True, '\'0\''],
        'BatchID': [True, '\'0\''],
        'EffectiveDate': [True,
                          'datetime.strptime(record[\'PTS\'], \'%Y%m%d-%H%M%S\').strftime(\'%Y-%m-%d %H:%M:%S\')'],
        'EndDate': [True, 'None']
    }
    
    dim_companies = defaultdict(list)
    
    for file in tqdm(sorted(glob(data_folder_path + "FINWIRE*"))):
        if '_audit' in file:
            continue
        with open(file, 'r') as f:
            for line in f:
                if not line[15:18] == 'CMP':
                    continue
                offset = 0
                record = {}
                company = {}
                for entry in finwire_schema:
                    value = line[offset:offset + entry[1]].strip()
                    record[entry[0]] = value
                    offset += entry[1]
                for k, v in dim_company_map.items():
                    if not v[0]:
                        company[k] = record[v[1]]
                    else:
                        try:
                            company[k] = eval(v[1])
                        except:
                            company[k] = ''
                
                dim_companies[record['CIK']].append(company)
    
    for CIK, entries in tqdm(dim_companies.items()):
        for (old, new) in zip(entries, entries[1:] + [None]):
            if not new:
                old['IsCurrent'] = '1'
                old['EndDate'] = '9999-12-31'
                continue
            old['EndDate'] = new['EffectiveDate']
    
    with open(tmp_file_path, 'w') as out:
        n = 0
        for CIK, entries in tqdm(dim_companies.items()):
            for entry in entries:
                out.write(str(n))
                out.write('|'.join(entry.values()) + '\n')
                n += 1
    
    cur = conn.cursor()
    cur.execute("""
      DROP TABLE IF EXISTS DimCompany;
      CREATE TABLE DimCompany (
        SK_CompanyID INTEGER NOT NULL PRIMARY KEY,
        CompanyID INTEGER NOT NULL,
        Status CHAR(10) Not NULL,
        Name CHAR(60) Not NULL,
        Industry CHAR(50) Not NULL,
        SPrating CHAR(4),
        isLowGrade BOOLEAN,
        CEO CHAR(100) Not NULL,
        AddressLine1 CHAR(80),
        AddressLine2 CHAR(80),
        PostalCode CHAR(12) Not NULL,
        City CHAR(25) Not NULL,
        StateProv CHAR(20) Not NULL,
        Country CHAR(24),
        Description CHAR(150) Not NULL,
        FoundingDate DATE,
        IsCurrent BOOLEAN Not NULL,
        BatchID numeric(5) Not NULL,
        EffectiveDate DATE Not NULL,
        EndDate DATE Not NULL
      );
    """)
    
    bulk_load(conn, 'DimCompany', tmp_file_path, '|')
    
    # os.remove(tmp_file_path)
