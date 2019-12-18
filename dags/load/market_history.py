import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

daily_market_file_path = data_folder_path + "DailyMarket.txt"


def load(conn):
    cur = conn.cursor()
    
    # needs to execute setup.sql first
    df_ = pd.read_csv(daily_market_file_path, sep="|", parse_dates={"Date": ["DM_DATE"]}, keep_date_col=True,
                      names=["DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"])
    
    df_fact = pd.DataFrame(columns=["SK_SecurityID", "SK_CompanyID", "SK_DateID", "PERatio", "Yield",
                                    "SK_FiftyTwoWeekHighDate", "SK_FiftyTwoWeekLowDate", "ClosePrice",
                                    "DayHigh", "DayLow", "Volume", "BatchID"])
    
    df_fact["ClosePrice"] = df_["DM_CLOSE"]
    df_fact["DayHigh"] = df_["DM_HIGH"]
    df_fact["DayLow"] = df_["DM_LOW"]
    df_fact["Volume"] = df_["DM_VOL"]
    df_fact["BatchID"] = 1
    
    df_fact["Symbol"] = df_["DM_S_SYMB"]  # value used by the trigger
    df_fact["Date"] = df_["Date"]  # value used by the trigger
    df_fact["SK_DateID"] = 0  # value set by the trigger
    df_fact["SK_FiftyTwoWeekLowDate"] = 0  # value set by the trigger
    df_fact["SK_FiftyTwoWeekHighDate"] = 0  # value set by the trigger
    df_fact["SK_SecurityID"] = 0  # value set by the trigger
    df_fact["SK_CompanyID"] = 0  # value set by the trigger
    df_fact["PERatio"] = 0  # value set by the trigger
    df_fact["Yield"] = 0  # value set by the trigger
    
    df_52 = get_52_weeks_data(df_)
    df_fact = pd.merge(df_fact, df_52, on=["Date", "Symbol"])
    
    df_fact["quarter"] = df_fact["Date"].apply(lambda x: 1 + (x.month - 1) // 3)
    df_fact["year"] = df_fact["Date"].apply(lambda x: x.year)
    df_fact = df_fact.apply(get_previous_quarters, axis=1)
    df_fact.drop("quarter", inplace=True, axis=1)
    df_fact.drop("year", inplace=True, axis=1)
    
    logging.info("Inserting into MySQL")
    df_fact.to_sql("FactMarketHistory", index=False, if_exists="append", con=get_engine())
    
    logging.info("Dropping auxiliary columns")
    cur.execute("DROP TRIGGER tpcdi.ADD_FactMarketHistory;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev1_quarter;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev2_quarter;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev3_quarter;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev4_quarter;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev1_year;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev2_year;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev3_year;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN prev4_year;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN Date;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN Symbol;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN FiftyTwoWeekHigh;")
    cur.execute("ALTER TABLE FactMarketHistory DROP COLUMN FiftyTwoWeekLow;")
    
    conn.commit()


def get_52_weeks_data(df):
    oie = df.groupby("DM_S_SYMB").rolling('365D', on="Date").agg({"DM_HIGH": "max", "DM_LOW": "min"})
    ola = oie.reset_index()
    return ola.rename(columns={"DM_S_SYMB": "Symbol", "DM_HIGH": "FiftyTwoWeekHigh", "DM_LOW": "FiftyTwoWeekLow"})


def get_previous_quarters(row):
    year = row["year"]
    prev_year = year - 1
    quarter = row["quarter"]
    
    if quarter == 1:
        sequence = [4, prev_year, 3, prev_year, 2, prev_year, 1, prev_year]
    elif quarter == 2:
        sequence = [1, year, 4, prev_year, 3, prev_year, 2, prev_year]
    elif quarter == 3:
        sequence = [2, year, 1, year, 4, prev_year, 3, prev_year]
    elif quarter == 4:
        sequence = [3, year, 2, year, 1, year, 4, prev_year]
    else:
        sequence = 0 / 0
    
    row["prev1_quarter"] = sequence[0]
    row["prev1_year"] = sequence[1]
    row["prev2_quarter"] = sequence[2]
    row["prev2_year"] = sequence[3]
    row["prev3_quarter"] = sequence[4]
    row["prev3_year"] = sequence[5]
    row["prev4_quarter"] = sequence[6]
    row["prev4_year"] = sequence[7]
    
    return row
