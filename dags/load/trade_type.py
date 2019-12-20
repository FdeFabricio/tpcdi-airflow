import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "TradeType.txt"


def load():
    logging.info("Begin TradeType - Historical Load")
    df = pd.read_csv(file_path, sep="|", names=["TT_ID", "TT_NAME", "TT_IS_SELL", "TT_IS_MRKT"])
    df.to_sql("TradeType", index=False, if_exists="append", con=get_engine())
