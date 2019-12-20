import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "StatusType.txt"


def load():
    logging.info("Begin StatusType - Historical Load")
    df = pd.read_csv(file_path, sep="|", names=["ST_ID", "ST_NAME"])
    df.to_sql("StatusType", index=False, if_exists="append", con=get_engine())
