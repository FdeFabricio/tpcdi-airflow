from utils.utils import bulk_load
import pandas as pd
import os

file_path = 'data/Batch1/TaxRate.txt'
tmp_file_path = 'data/Batch1/TaxRate_tmp.txt'


def load(conn):
    cur = conn.cursor()    
    cur.execute("""
      DROP TABLE IF EXISTS TaxRate;
        CREATE TABLE TaxRate ( TX_ID CHAR(4) Not NULL,
                                TX_NAME CHAR(50) Not NULL,
                                TX_RATE numeric(6,5) Not NULL
        );
    """)
    df = pd.read_csv(file_path, header=None, delimiter='|')    
    df.to_csv(tmp_file_path, index=False, header=False, sep="|")

    bulk_load(conn, 'TaxRate', tmp_file_path, '|')
    
    os.remove(tmp_file_path)
    