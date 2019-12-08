from utils.utils import bulk_load
import pandas as pd

file_path = 'data/Batch1/TaxRate.txt'


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

    bulk_load(conn, 'TaxRate', file_path, '|')

    