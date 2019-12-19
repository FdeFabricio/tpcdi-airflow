import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

date_file_path = data_folder_path + "Date.txt"


def load(conn):
    cur = conn.cursor()
    df = pd.read_csv(date_file_path, delimiter="|",
                     names=["SK_DateID", "DateValue", "DateDesc", "CalendarYearID", "CalendarYearDesc", "CalendarQtrID",
                            "CalendarQtrDesc", "CalendarMonthID", "CalendarMonthDesc", "CalendarWeekID",
                            "CalendarWeekDesc", "DayOfWeekNum", "DayOfWeekDesc", "FiscalYearID", "FiscalYearDesc",
                            "FiscalQtrID", "FiscalQtrDesc", "HolidayFlag"])
    
    df["HolidayFlag"] = df["HolidayFlag"].apply(lambda x: x and 1 or 0)
    
    df.to_sql("DimDate", index=False, if_exists="append", con=get_engine())
    
    logging.info("Adding index to table")
    cur.execut("ALTER TABLE DimDate ADD INDEX(DateValue);")
    conn.commit()
