import logging
from glob import glob

import pandas as pd

from utils.utils import data_folder_path, get_engine


def load():
    logging.info("Begin Audit - Historical Load")
    for file in glob(data_folder_path + "*_audit.csv"):
        df = pd.read_csv(file)
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.to_sql("Audit", index=False, if_exists="append", con=get_engine())
