import pandas as pd

from utils.utils import data_folder_path, get_engine

time_file_path = data_folder_path + "/Time.txt"


def load():
    df = pd.read_csv(time_file_path, delimiter='|',
                     names=["SK_TimeID", "TimeValue", "HourID", "HourDesc", "MinuteID", "MinuteDesc", "SecondID",
                            "SecondDesc", "MarketHoursFlag", "OfficeHoursFlag"])
    
    df["MarketHoursFlag"] = df["MarketHoursFlag"].apply(lambda x: x and 1 or 0)
    df["OfficeHoursFlag"] = df["OfficeHoursFlag"].apply(lambda x: x and 1 or 0)
    
    df.to_sql("DimTime", index=False, if_exists="append", con=get_engine())
