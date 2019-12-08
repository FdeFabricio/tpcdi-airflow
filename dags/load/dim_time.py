from utils.utils import bulk_load
import pandas as pd
import os

file_path = 'data/Batch1/Time.txt'
tmp_file_path = 'data/Batch1/DimTime_tmp.txt'

def load(conn):
    cur = conn.cursor()    
    cur.execute("""
      DROP TABLE IF EXISTS DimTime;
        CREATE TABLE DimTime ( SK_TimeID INTEGER Not NULL PRIMARY KEY,
                                TimeValue TIME Not NULL,
                                HourID numeric(2) Not NULL,
                                HourDesc CHAR(20) Not NULL,
                                MinuteID numeric(2) Not NULL,
                                MinuteDesc CHAR(20) Not NULL,
                                SecondID numeric(2) Not NULL,
                                SecondDesc CHAR(20) Not NULL,
                                MarketHoursFlag BOOLEAN,
                                OfficeHoursFlag BOOLEAN
        );
    """)
    df = pd.read_csv(file_path, header=None, delimiter='|')     
    df[df.columns[-1]] = df[df.columns[-1]].map({True:1, False:0})
    df[df.columns[-2]] = df[df.columns[-2]].map({True:1, False:0})
    df.to_csv(tmp_file_path, index=False, header=False, sep="|")

    bulk_load(conn, 'DimTime', tmp_file_path, '|')
    
    os.remove(tmp_file_path)
 