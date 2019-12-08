from utils.utils import bulk_load
import pandas as pd
import numpy as np
import os

file_path = 'data/Batch1/HR.csv'
tmp_file_path = 'data/Batch1/DimBroker_tmp.txt'


def load(conn):
    cur = conn.cursor()
    
    cur.execute("""
      DROP TABLE IF EXISTS DimBroker;
        CREATE TABLE DimBroker  ( SK_BrokerID  INTEGER NOT NULL PRIMARY KEY,
            BrokerID  INTEGER NOT NULL,
            ManagerID  INTEGER,
            FirstName       CHAR(50) NOT NULL,
            LastName       CHAR(50) NOT NULL,
            MiddleInitial       CHAR(1),
            Branch       CHAR(50),
            Office       CHAR(50),
            Phone       CHAR(14),
            IsCurrent boolean NOT NULL,
            BatchID INTEGER NOT NULL,
            EffectiveDate date NOT NULL,
            EndDate date NOT NULL							
        );
    """)
    
    df = pd.read_csv(file_path, header=None)    
    df=df[df[5]==314]   
    df.drop(df.columns[5], axis=1, inplace=True)   
    sk = np.arange(1,len(df)+1)
    df.insert(0, 'SK_BrokerID', sk)
    cur.execute("SELECT min(DateValue) FROM DimDate;")
    df_d=pd.read_csv(date_file_path, header=None, delimiter='|')
    min_date=df_d[1].min()
    df['IsCurrent']=1
    df['BatchId']=1
    df['EffectiveDate']=min_date
    df['EndDate']='9999-12-31'       
    
    df.to_csv(tmp_file_path, index=False, header=False, sep="|")

    bulk_load(conn, 'DimBroker', tmp_file_path, '|')
    
    os.remove(tmp_file_path)