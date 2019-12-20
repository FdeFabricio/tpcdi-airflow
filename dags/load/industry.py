import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "Industry.txt"


def load():
    logging.info("Begin Industry - Historical Load")
    df = pd.read_csv(file_path, sep="|", names=["IN_ID", "IN_NAME", "IN_SC_ID"])
    df.to_sql("Industry", index=False, if_exists="append", con=get_engine())
