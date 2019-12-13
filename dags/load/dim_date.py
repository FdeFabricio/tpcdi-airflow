from utils.utils import df_bulk_load
import pandas as pd

file_path = 'data/Batch1/Date.txt'


def load(conn):
    cur = conn.cursor()
    
    cur.execute("""
        DROP TABLE IF EXISTS DimDate;
        CREATE TABLE DimDate (
            SK_DateID INTEGER NOT NULL PRIMARY KEY,
            DateValue DATE NOT NULL,
            DateDesc CHAR(20) NOT NULL,
            CalendarYearID NUMERIC(4) NOT NULL,
            CalendarYearDesc CHAR(20) NOT NULL,
            CalendarQtrID NUMERIC(5) NOT NULL,
            CalendarQtrDesc CHAR(20) NOT NULL,
            CalendarMonthID NUMERIC(6) NOT NULL,
            CalendarMonthDesc CHAR(20) NOT NULL,
            CalendarWeekID NUMERIC(6) NOT NULL,
            CalendarWeekDesc CHAR(20) NOT NULL,
            DayOfWeekNum NUMERIC(1) NOT NULL,
            DayOfWeekDesc CHAR(10) NOT NULL,
            FiscalYearID NUMERIC(4) NOT NULL,
            FiscalYearDesc CHAR(20) NOT NULL,
            FiscalQtrID NUMERIC(5) NOT NULL,
            FiscalQtrDesc CHAR(20) NOT NULL,
            HolidayFlag BOOLEAN
        );
    """)
    
    df = pd.read_csv(file_path, delimiter="|", header=None)
    df[17] = df[17].apply(lambda x: x and 1 or 0)
    
    df_bulk_load(conn, df, 'DimDate')
