import logging

import pandas as pd

from utils.utils import data_folder_path, get_engine

file_path = data_folder_path + "TaxRate.txt"


def load():
    logging.info("Begin TaxRate - Historical Load")
    df = pd.read_csv(file_path, sep="|", names=["TX_ID", "TX_NAME", "TX_RATE"])
    df.to_sql("TaxRate", index=False, if_exists="append", con=get_engine())
